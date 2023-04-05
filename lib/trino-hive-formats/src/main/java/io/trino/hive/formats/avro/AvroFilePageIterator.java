package io.trino.hive.formats.avro;

import io.trino.hive.formats.DataSeekableInputStream;
import io.trino.spi.Page;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;

public class AvroFilePageIterator
        implements Iterator<Page>
{
    private AvroPageDataReader dataReader;
    private DataFileReader<Optional<Page>> fileReader;
    private Optional<Page> nextPage = Optional.empty();

    public AvroFilePageIterator(Schema readerSchema, SeekableInput inputStream) throws IOException
    {
        dataReader = new AvroPageDataReader(readerSchema);
        fileReader = new DataFileReader<>(inputStream, dataReader);
    }

    @Override
    public boolean hasNext()
    {
        populateNextPage();
        return nextPage.isPresent();
    }

    @Override
    public Page next()
    {
        populateNextPage();
        Page toReturn = nextPage.orElse(null);
        nextPage = Optional.empty();
        return toReturn;
    }

    private void populateNextPage()
    {
        while (fileReader.hasNext() && nextPage.isEmpty()) {
            nextPage = fileReader.next();
        }
        if (nextPage.isEmpty()) {
            try {
                nextPage = dataReader.flush();
            }
            catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
        }
    }
}
