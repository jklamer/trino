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
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.connector.ConnectorSession;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeWriterEnabled;

public class AvroFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystem fileSystem;

    @Inject
    public AvroFileWriterFactory(TrinoFileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
    }

    @Override
    public Optional<FileWriter> createFileWriter(Path path, List<String> inputColumnNames, StorageFormat storageFormat, Properties schema, JobConf conf, ConnectorSession session, OptionalInt bucketNumber, AcidTransaction transaction, boolean useAcidSchema, WriterKind writerKind)
    {
        if (!isAvroNativeWriterEnabled(session)) {
            return Optional.empty();
        }
        throw new NotImplementedException();
    }
}
