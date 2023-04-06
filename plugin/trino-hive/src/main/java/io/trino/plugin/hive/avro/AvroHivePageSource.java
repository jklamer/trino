package io.trino.plugin.hive.avro;

import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.avro.AvroFileReader;
import io.trino.hive.formats.avro.AvroTypeManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.avro.Schema;

import java.io.IOException;

public class AvroHivePageSource
        implements ConnectorPageSource
{
    private static final long GUESSED_MEMORY_USAGE = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();

    private final AvroFileReader avroFileReader;

    public AvroHivePageSource(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager,
            long offset,
            long length)
            throws IOException
    {
        avroFileReader = new AvroFileReader(inputFile, schema, avroTypeManager, offset, length);
    }

    @Override
    public long getCompletedBytes()
    {
        return avroFileReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return avroFileReader.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !avroFileReader.getPageIterator().hasNext();
    }

    @Override
    public Page getNextPage()
    {
        return avroFileReader.getPageIterator().next();
    }

    @Override
    public long getMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    @Override
    public void close()
            throws IOException
    {
        avroFileReader.close();
    }
}
