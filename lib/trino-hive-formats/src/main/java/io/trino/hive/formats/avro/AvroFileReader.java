package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.DataSeekableInputStream;
import io.trino.spi.type.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

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
    private final List<String> columns;
    private final Map<String, Type> columnTypes;

    public AvroFileReader(
            TrinoInputFile inputFile,
            List<String> columns,
            Map<String, Type> columnTypes,
            long offset,
            long length)
            throws IOException
    {
        requireNonNull(inputFile, "inputFile is null");
        this.location = inputFile.location();
        this.fileSize = inputFile.length();
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnTypes = ImmutableMap.copyOf(requireNonNull(columnTypes, "columnTypes is null"));

        verify(offset >= 0, "offset is negative");
        verify(offset < inputFile.length(), "offset is greater than data size");
        verify(length >= 1, "length must be at least 1");
        this.length = length;
        this.end = offset + length;
        verify(end <= fileSize, "offset plus length is greater than data size");
        this.input = new DataSeekableInputStream(inputFile.newInput().inputStream(), length);
        this.input.seek(offset);
        DataFileReader<GenericRecord> recordReader = new DataFileReader<GenericRecord>((SeekableInput) input, new GenericDatumReader<>());
    }

    @Override
    public void close()
            throws IOException
    {

    }
}
