package io.trino.hive.formats.avro;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.Page;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class AvroFilePageIterator
        implements Iterator<Page>
{
    private AvroPageDataReader dataReader;
    private DataFileReader<Optional<Page>> fileReader;
    private Optional<Page> nextPage = Optional.empty();


    @VisibleForTesting
    public AvroFilePageIterator(Schema readerSchema, SeekableInput inputStream) throws IOException
    {
        this(readerSchema, NoOpAvroTypeManager.INSTANCE, inputStream);
    }

    // Calling class responsible for closing input stream
    public AvroFilePageIterator(Schema readerSchema, AvroTypeManager typeManager, SeekableInput inputStream) throws IOException
    {
        dataReader = new AvroPageDataReader(readerSchema, typeManager);
        fileReader = new DataFileReader<>(inputStream, dataReader);
        typeManager.configure(fileReader.getMetaKeys().stream().collect(toImmutableMap(Function.identity(), fileReader::getMeta)));
    }

    @Override
    public boolean hasNext()
    {
        populateNextPage();
        return nextPage.isPresent();
    }

    @Override
    @Nullable
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
            try {
                nextPage = fileReader.next();
            }
            catch (AvroRuntimeException runtimeException) {
                throw new UncheckedIOException((IOException) runtimeException.getCause());
            }
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
