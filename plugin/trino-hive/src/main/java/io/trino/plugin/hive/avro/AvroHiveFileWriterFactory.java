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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeWriterEnabled;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_CONTAINER_OUTPUT_FORMAT_CLASS;
import static java.util.Objects.requireNonNull;

public class AvroHiveFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;

    @Inject
    public AvroHiveFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion");
    }

    @Override
    public Optional<FileWriter> createFileWriter(Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session,
            OptionalInt bucketNumber,
            AcidTransaction transaction,
            boolean useAcidSchema,
            WriterKind writerKind)
    {
        if (!isAvroNativeWriterEnabled(session)) {
            return Optional.empty();
        }
        if (!AVRO_CONTAINER_OUTPUT_FORMAT_CLASS.equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

//        boolean compress = HiveConf.getBoolVar(configuration, COMPRESSRESULT);
//        if (compress) {
//            String compressionString = getAvroFileCodec(configuration);
//        }

        try {
            TrinoFileSystem fileSystem = new HdfsFileSystemFactory(hdfsEnvironment).create(session.getIdentity());
            Schema fileSchema = AvroHiveFileUtils.determineSchemaOrThrowException(fileSystem, configuration, schema);
            AggregatedMemoryContext outputStreamMemoryContext = newSimpleAggregatedMemoryContext();
            OutputStream outputStream = fileSystem.newOutputFile(path.toString()).create(outputStreamMemoryContext);

            Closeable rollbackAction = () -> fileSystem.deleteFile(path.toString());

            return Optional.empty();
//            return Optional.of(
//                    new AvroHiveFileWriter(
//                            outputStream,
//                            outputStreamMemoryContext,
//                            rollbackAction,
//                            fileSchema,
//                    )
//            );
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating Avro Container file", e);
        }
    }
}
