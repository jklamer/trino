/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.avro;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.MonitoredTrinoInputFile;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.internal.Accessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.hive.formats.avro.AvroTypeUtils.unwrapNullableUnion;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeReaderEnabled;
import static io.trino.plugin.hive.ReaderPageSource.noProjectionAdaptation;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static java.lang.Math.min;
import static java.util.function.Predicate.not;

public class AvroHivePageSourceFactory
        implements HivePageSourceFactory
{
    private static final DataSize BUFFER_SIZE = DataSize.of(8, DataSize.Unit.MEGABYTE);

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public AvroHivePageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this.typeManager = typeManager;
        this.hdfsEnvironment = hdfsEnvironment;
        this.stats = stats;
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!isAvroNativeReaderEnabled(session)) {
            return Optional.empty();
        }
        else if (!AVRO_SERDE_CLASS.equals(getDeserializerClassName(schema))) {
            return Optional.empty();
        }
        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        List<HiveColumnHandle> projectedReaderColumns = columns;
        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);

        if (readerProjections.isPresent()) {
            projectedReaderColumns = readerProjections.get().get().stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableList());
        }

        TrinoFileSystem trinoFileSystem = new HdfsFileSystemFactory(hdfsEnvironment).create(session.getIdentity());
        TrinoInputFile inputFile = new MonitoredTrinoInputFile(stats, trinoFileSystem.newInputFile(path.toString()));

        Schema tableSchema;
        try {
            tableSchema = AvroHiveFileUtils.determineSchemaOrThrowException(trinoFileSystem, configuration, schema);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Unable to load or parse schema", e);
        }

        try {
            length = min(inputFile.length() - start, length);
            if (!inputFile.exists()) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "File does not exist");
            }
            if (estimatedFileSize < BUFFER_SIZE.toBytes()) {
                try (TrinoInputStream input = inputFile.newStream()) {
                    byte[] data = input.readAllBytes();
                    inputFile = new MemoryInputFile(path.toString(), Slices.wrappedBuffer(data));
                }
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        // Split may be empty now that the correct file size is known
        if (length <= 0) {
            return Optional.of(noProjectionAdaptation(new EmptyPageSource()));
        }

        Schema maskedSchema = maskColumnsFromTableSchema(projectedReaderColumns, tableSchema);
        if (maskedSchema.getFields().isEmpty()) {
            // no non-masked columns to select from partition schema
            // hack to return null rows with same number as underlying data source
            // will error if UUID is same name as base column for underlying storage table but should never
            // return false data
            SchemaBuilder.FieldAssembler<Schema> nullSchema = SchemaBuilder.record("null_only").fields();
            for (int i = 0; i < Math.max(projectedReaderColumns.size(), 1); i++) {
                String notAColumnName;
                while (Objects.nonNull(tableSchema.getField((notAColumnName = "f"+UUID.randomUUID().toString().replace('-', '_'))))) {
                }
                nullSchema = nullSchema.name(notAColumnName).type(Schema.create(Schema.Type.NULL)).withDefault(null);
            }
            try {
                return Optional.of(new ReaderPageSource(new AvroHivePageSource(inputFile, nullSchema.endRecord(), new HiveAvroTypeManager(configuration), start, length), readerProjections));
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
        }

        try {
            return Optional.of(new ReaderPageSource(new AvroHivePageSource(inputFile, maskedSchema, new HiveAvroTypeManager(configuration), start, length), readerProjections));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
    }

    private Schema maskColumnsFromTableSchema(List<HiveColumnHandle> columns, Schema tableSchema)
    {
        verify(tableSchema.getType() == Schema.Type.RECORD);
        Set<String> maskedColumns = columns.stream().map(HiveColumnHandle::getBaseColumnName).collect(toImmutableSet());
        Map<String, List<HiveColumnHandle>> rowFieldMasking = ImmutableMap.copyOf(columns.stream().filter(not(HiveColumnHandle::isBaseColumn)).collect(Collectors.groupingBy(HiveColumnHandle::getBaseColumnName)));

        SchemaBuilder.FieldAssembler<Schema> maskedSchema = SchemaBuilder.builder()
            .record(tableSchema.getName())
            .namespace(tableSchema.getNamespace())
            .fields();

        for (Schema.Field field : tableSchema.getFields()) {
            if (maskedColumns.contains(field.name())) {
                maskedSchema = maskedSchema
                        .name(field.name())
                        .aliases(field.aliases().toArray(String[]::new))
                        .doc(field.doc())
                        .type(field.schema())
                        //.type(rowFieldMasking.containsKey(field.name()) ? maskRowField(rowFieldMasking.get(field.name()), field.schema(), 0) : field.schema())
                        .withDefault(Accessor.defaultValue(field));
            }
        }
        return maskedSchema.endRecord();
    }


    private Schema maskRowField(List<HiveColumnHandle> columns, Schema field, int level)
    {
        Schema unwrapped = field.isUnion() ? unwrapNullableUnion(field).orElseThrow(() -> new UnsupportedOperationException("Unable to handle non nullable")) : field;
        return field;
    }
}
