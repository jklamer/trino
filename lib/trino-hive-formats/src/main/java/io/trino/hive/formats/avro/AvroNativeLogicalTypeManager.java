package io.trino.hive.formats.avro;

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static org.apache.avro.LogicalTypes.fromSchemaIgnoreInvalid;

/**
 * An implementation that translates Avro Standard Logical types into Trino SPI types
 */
public class AvroNativeLogicalTypeManager
        extends AvroTypeManager
{

    private static final Logger log = Logger.get(AvroNativeLogicalTypeManager.class);
    // Copied from org.apache.avro.LogicalTypes
    private static final String DECIMAL = "decimal";
    private static final String UUID = "uuid";
    private static final String DATE = "date";
    private static final String TIME_MILLIS = "time-millis";
    private static final String TIME_MICROS = "time-micros";
    private static final String TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String TIMESTAMP_MICROS = "timestamp-micros";
    private static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    private static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

    @Override
    public void configure(Map<String, byte[]> fileMetaData)
    {
    }

    /**
     * Heavily borrow from org.apache.avro.LogicalTypes#fromSchemaImpl(org.apache.avro.Schema, boolean)
     *
     * @param schema
     * @return
     */
    @Override
    public Optional<Type> typeForSchema(Schema schema)
    {
        return validateAndProduceFromName(schema, logicalType ->
                switch (logicalType.getName()) {
                    case TIMESTAMP_MILLIS -> TimestampType.TIMESTAMP_MILLIS;
                    case TIMESTAMP_MICROS -> TimestampType.TIMESTAMP_MICROS;
                    case DECIMAL -> {
                        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                        yield DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
                    }
                    case DATE -> DateType.DATE;
                    case TIME_MILLIS -> TimeType.TIME_MILLIS;
                    case TIME_MICROS -> TimeType.TIME_MICROS;
                    case UUID -> UuidType.UUID;
                    default -> throw new IllegalStateException("Unreachable unfilted logical type");
                }
        );
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> buildingFunctionForSchema(Schema schema)
    {
        return validateAndProduceFromName(schema, logicalType ->
                switch (logicalType.getName()) {
                    case TIMESTAMP_MILLIS -> switch (schema.getType()) {
                        case LONG -> {
                            yield (builder, longObj) -> {
                                Long l = (Long) longObj;
                                TimestampType.TIMESTAMP_MILLIS.writeObject(builder, new LongTimestamp(l * 1000, 0));
                            };
                        }
                        default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                    };
                    case TIMESTAMP_MICROS -> switch (schema.getType()) {
                        case LONG -> {
                            yield (builder, longObj) -> {
                                Long l = (Long) longObj;
                                TimestampType.TIMESTAMP_MILLIS.writeObject(builder, new LongTimestamp(l, 0));
                            };
                        }
                        default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                    };
                    case DECIMAL -> {
                        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                        DecimalType decimalType = DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
                        Function<Object, byte[]> byteExtract = switch (schema.getType()) {
                            case BYTES -> {
                                yield (obj) -> ((ByteBuffer) obj).array();
                            }
                            case FIXED -> {
                                yield (obj) -> ((GenericFixed) obj).bytes();
                            }
                            default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                        };
                        yield switch (decimalType) {
                            case io.trino.spi.type.LongDecimalType longDecimalType -> {
                                yield (builder, obj) -> {
                                    longDecimalType.writeObject(builder, Int128.fromBigEndian(byteExtract.apply(obj)));
                                };
                            }
                            case io.trino.spi.type.ShortDecimalType shortDecimalType -> {
                                yield (builder, obj) -> {
                                    // inspired by io.trino.operator.scalar.VarbinaryFunctions.fromBigEndian64
                                    shortDecimalType.writeLong(builder, Long.reverseBytes(Slices.wrappedBuffer(byteExtract.apply(obj)).getLong(0)));
                                };
                            }
                        };
                    }
                    case DATE -> switch (schema.getType()) {
                        case INT -> {
                            yield (builder, obj) -> {
                                Integer i = (Integer) obj;
                                DateType.DATE.writeLong(builder, i.longValue());
                            };
                        }
                        default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                    };
                    case TIME_MILLIS -> switch (schema.getType()) {
                        case INT -> {
                            yield (builder, obj) -> {
                                Integer i = (Integer) obj;
                                TimeType.TIME_MILLIS.writeLong(builder, i.longValue());
                            };
                        }
                        default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                    };
                    case TIME_MICROS -> switch (schema.getType()) {
                        case INT -> {
                            yield (builder, obj) -> {
                                Integer i = (Integer) obj;
                                TimeType.TIME_MICROS.writeLong(builder, i.longValue());
                            };
                        }
                        default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                    };
                    case UUID -> switch (schema.getType()) {
                        case STRING -> {
                            yield (builder, obj) -> {
                                UuidType.UUID.writeSlice(builder, javaUuidToTrinoUuid(java.util.UUID.fromString(obj.toString())));
                            };
                        }
                        default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                    };
                    default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                }
        );
    }

    private <T> Optional<T> validateAndProduceFromName(Schema schema, Function<LogicalType, T> produce)
    {
        final String typeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        if (typeName == null) {
            return Optional.empty();
        }
        LogicalType logicalType = null;
        switch (typeName) {
            case TIMESTAMP_MILLIS, TIMESTAMP_MICROS, DECIMAL, DATE, TIME_MILLIS, TIME_MICROS, UUID:
                logicalType = fromSchemaIgnoreInvalid(schema);
            case LOCAL_TIMESTAMP_MICROS + LOCAL_TIMESTAMP_MILLIS:
                log.warn("Logical type " + typeName + " not currently supported by by Trino");
            default:
                log.warn("Unrecognized logical type " + typeName);
        }
        // make sure the type is valid before returning it
        if (logicalType != null) {
            try {
                logicalType.validate(schema);
            }
            catch (RuntimeException e) {
                log.warn("Invalid logical type found", e);
            }
            return Optional.of(produce.apply(logicalType));
        }
        else {
            return Optional.empty();
        }
    }
}
