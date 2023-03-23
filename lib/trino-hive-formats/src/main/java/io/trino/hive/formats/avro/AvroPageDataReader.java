package io.trino.hive.formats.avro;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.AvroTypeUtils.typeFromAvro;
import static java.util.Objects.requireNonNull;

public class AvroPageDataReader extends GenericDatumReader<Optional<Page>>
{
    private final Schema readerSchema;
    private final Type readerSchemaType;

    public AvroPageDataReader(Schema readerSchema, InputStream inputStream)
    {
        super(readerSchema);
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        verify(readerSchema.getType().equals(Schema.Type.RECORD), "page Avro Schema must be record");
        this.readerSchemaType = typeFromAvro(this.readerSchema);
    }

    @Override
    protected Object readWithoutConversion(Object old, Schema expected, ResolvingDecoder in)
            throws IOException
    {
        switch (expected.getType()) {
            case RECORD:
                return readRecord(old, expected, in);
            case ENUM:
                return readEnum(expected, in);
            case ARRAY:
                return readArray(old, expected, in);
            case MAP:
                return readMap(old, expected, in);
            case UNION:
                return read(old, expected.getTypes().get(in.readIndex()), in);
            case FIXED:
                return readFixed(old, expected, in);
            case STRING:
                return readString(old, expected, in);
            case BYTES:
                return readBytes(old, expected, in);
            case INT:
                return readInt(old, expected, in);
            case LONG:
                return in.readLong();
            case FLOAT:
                return in.readFloat();
            case DOUBLE:
                return in.readDouble();
            case BOOLEAN:
                return in.readBoolean();
            case NULL:
                in.readNull();
                return null;
            default:
                throw new AvroRuntimeException("Unknown type: " + expected);
        }
    }
}
