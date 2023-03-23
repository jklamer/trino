package io.trino.hive.formats.avro;

import com.google.common.base.VerifyException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class AvroTypeUtils
{
    public static Type typeFromAvro(final Schema schema)
    {
        switch (schema.getType()) {
            case RECORD:
                return RowType.from(schema.getFields()
                        .stream()
                        .map(field ->
                        {
                            return new RowType.Field(Optional.of(field.name()), typeFromAvro(field.schema()));
                        }).collect(toImmutableList()));
            case ENUM:
                return VarcharType.VARCHAR;
            case ARRAY:
                return new ArrayType(typeFromAvro(schema.getElementType()));
            case MAP:
                return new MapType(VarcharType.VARCHAR, typeFromAvro(schema.getValueType()), new TypeOperators());
            case UNION:
                throw new UnsupportedOperationException("No union type support for now");
            case FIXED:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case NULL:
                throw new UnsupportedOperationException("No null column type support");
            default:
                throw new VerifyException("Schema type unknown: " + schema.getType().toString());
        }
    }
}
