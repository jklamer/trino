package io.trino.plugin.hive.avro;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;

public class AvroHivePageSource
        implements ConnectorPageSource
{


    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return false;
    }

    @Override
    public Page getNextPage()
    {
        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {}
}
