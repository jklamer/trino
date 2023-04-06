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

import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
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
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeReaderEnabled;
import static io.trino.plugin.hive.ReaderPageSource.noProjectionAdaptation;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static java.lang.Math.min;

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

        Schema avroSchema;
        try {
            avroSchema = AvroHiveFileUtils.determineSchemaOrThrowException(trinoFileSystem, configuration, schema);
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
                try (TrinoInput input = inputFile.newInput(); InputStream inputStream = input.inputStream()) {
                    byte[] data = inputStream.readAllBytes();
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

        try {
            return Optional.of(new ReaderPageSource(new AvroHivePageSource(inputFile, avroSchema, new HiveAvroTypeManager(), start, length), readerProjections));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
    }
}
