package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.SeekableInputStream;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.DataSeekableInputStream;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class AvroFileReader
        implements Closeable
{

    private final String location;
    private final long fileSize;
    private final long length;
    private final long end;
    private final DataSeekableInputStream input;
    private AvroFilePageIterator pageIterator;

    public AvroFileReader(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager,
            long offset,
            long length)
            throws IOException
    {
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(schema, "schema is null");
        this.location = inputFile.location();
        this.fileSize = inputFile.length();

        verify(offset >= 0, "offset is negative");
        verify(offset < inputFile.length(), "offset is greater than data size");
        verify(length >= 1, "length must be at least 1");
        this.length = length;
        this.end = offset + length;
        verify(end <= fileSize, "offset plus length is greater than data size");
        input = new DataSeekableInputStream(inputFile.newInput().inputStream());
        input.seek(offset);
        this.pageIterator = new AvroFilePageIterator(schema, avroTypeManager, new SeekableInput() {
            @Override
            public void seek(long p)
                    throws IOException
            {
                input.seek(p);
            }

            @Override
            public long tell()
                    throws IOException
            {
                return input.getPos();
            }

            @Override
            public long length()
                    throws IOException
            {
                return length;
            }

            @Override
            public int read(byte[] b, int off, int len)
                    throws IOException
            {
                return input.read(b, off, len);
            }

            @Override
            public void close()
                    throws IOException
            {
                input.close();
            }
        });
    }

    public AvroFilePageIterator getPageIterator()
    {
        return pageIterator;
    }

    public long getCompletedBytes()
    {
        return input.getReadBytes();
    }

    public long getReadTimeNanos()
    {
        return input.getReadTimeNanos();
    }

    @Override
    public void close()
            throws IOException
    {
        this.input.close();
    }
}
