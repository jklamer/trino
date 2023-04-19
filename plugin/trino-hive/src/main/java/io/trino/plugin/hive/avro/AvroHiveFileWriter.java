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

import com.google.common.io.CountingOutputStream;
import io.trino.hive.formats.avro.AvroCompressionKind;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.RcFileFileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroHiveFileWriter
        implements FileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(AvroHiveFileWriter.class);

    private final CountingOutputStream outputStream;
    private final AggregatedMemoryContext outputStreamMemoryContext;
    private final Closeable rollbackAction;

    public AvroHiveFileWriter(
            OutputStream outputStream,
            AggregatedMemoryContext outputStreamMemoryContext,
            Closeable rollbackAction,
            Schema fileSchema,
            List<Type> fileColumnTypes,
            AvroCompressionKind compressionKind,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata)
    {
        this.outputStream = new CountingOutputStream(outputStream);
        this.outputStreamMemoryContext = outputStreamMemoryContext;
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");
    }

    @Override
    public long getWrittenBytes()
    {
        return outputStream.getCount();
    }

    @Override
    public long getMemoryUsage()
    {
        //TODO Get Retained bytes from File Writer
        return INSTANCE_SIZE + outputStreamMemoryContext.getBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
    }

    @Override
    public Closeable commit()
    {
        //todo close file writer and return rollback
        return null;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            //TODO close file writer
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }
}
