package io.trino.hive.formats.avro;

import com.google.common.base.VerifyException;
import io.airlift.concurrent.ThreadLocalCache;
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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Predicate.not;

public class AvroTypeUtils
{
    public static Type typeFromAvro(final Schema schema)
    {
        return typeFromAvro(schema, new HashSet<>());
    }

    private static Type typeFromAvro(final Schema schema, Set<Schema> enclosingRecords)
    {
        return switch (schema.getType()) {
            case RECORD ->
            {
                if (!enclosingRecords.add(schema)) {
                    throw new UnsupportedOperationException("Unable to represent recursive avro schemas in Trino Type form");
                }
                yield RowType.from(schema.getFields()
                        .stream()
                        .map(field ->
                        {
                            return new RowType.Field(Optional.of(field.name()), typeFromAvro(field.schema(), new HashSet<>(enclosingRecords)));
                        }).collect(toImmutableList()));
            }
            case ENUM -> VarcharType.VARCHAR;
            case ARRAY -> new ArrayType(typeFromAvro(schema.getElementType(), enclosingRecords));
            case MAP -> new MapType(VarcharType.VARCHAR, typeFromAvro(schema.getValueType(), enclosingRecords), new TypeOperators());
            case UNION ->
                    typeFromAvro(unwrapNullableUnion(schema).orElseThrow(() -> new UnsupportedOperationException("Unable to make Trino Type from Avro Union: %s".formatted(schema))), enclosingRecords);
            case FIXED -> VarbinaryType.VARBINARY;
            case STRING, BYTES -> VarcharType.VARCHAR;
            case INT, LONG -> IntegerType.INTEGER;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case BOOLEAN -> BooleanType.BOOLEAN;
            case NULL -> throw new UnsupportedOperationException("No null column type support");
        };
    }

    private static Optional<Schema> unwrapNullableUnion(Schema schema)
    {
        verify(schema.isUnion());
        if (schema.isNullable() && schema.getTypes().size() == 2) {
            return schema.getTypes().stream().filter(not(Schema::isNullable)).findFirst();
        }
        return Optional.empty();
    }
}
