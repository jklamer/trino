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
package io.trino.hive.formats.avro;

import io.trino.filesystem.TrinoOutputFile;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.memory.context.AggregatedMemoryContext;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AvroFileWriter
        implements Closeable
{
    private final DataFileWriter<AvroPagePositionDataWriter.PagePosition> fileWriter;

    public AvroFileWriter(
            Schema schema,
            TrinoOutputFile rawOutput,
            AvroCompressionKind compressionKind,
            Map<String, String> fileMetadata)
            throws IOException, AvroTypeException
    {
        this.fileWriter = new DataFileWriter<>(new AvroPagePositionDataWriter())
                .setCodec(compressionKind.getCodecFactory())
                .create(schema, rawOutput.create(AggregatedMemoryContext.newSimpleAggregatedMemoryContext()));
        for (Map.Entry<String, String> entry : requireNonNull(fileMetadata, "metadata is null").entrySet()) {
            this.fileWriter.setMeta(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void close()
            throws IOException {}
}
