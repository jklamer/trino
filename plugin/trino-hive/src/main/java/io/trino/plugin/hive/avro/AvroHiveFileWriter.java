package io.trino.plugin.hive.avro;

import com.google.common.io.CountingOutputStream;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroHiveFileWriter
        implements FileWriter
{
    private final CountingOutputStream outputStream;
    private final AggregatedMemoryContext outputStreamMemoryContext;
    private final Closeable rollbackAction;

    public AvroHiveFileWriter(
            OutputStream outputStream,
            AggregatedMemoryContext outputStreamMemoryContext,
            Closeable rollbackAction,
            Schema fileSchema,
            List<Type> fileColumnTypes,
            Optional<CompressionKind> compressionKind,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata)
    {
        this.outputStream = new CountingOutputStream(outputStream);
        this.outputStreamMemoryContext = outputStreamMemoryContext;
        this.rollbackAction = requireNonNull(rollbackAction,"rollbackAction is null");
    }

    @Override
    public long getWrittenBytes()
    {
        return outputStream.getCount();
    }

    @Override
    public long getMemoryUsage()
    {
        //TODO implement
        return 0;
    }

    @Override
    public void appendRows(Page dataPage)
    {
    }

    @Override
    public Closeable commit()
    {
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
