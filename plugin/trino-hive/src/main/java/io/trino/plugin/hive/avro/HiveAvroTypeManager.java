/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.avro;

import io.airlift.slice.Slices;
import io.trino.hive.formats.avro.AvroNativeLogicalTypeManager;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Chars;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.Varchars;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.joda.time.DateTimeZone;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.trino.plugin.hive.avro.HiveAvroConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.plugin.hive.avro.HiveAvroConstants.VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP;
import static io.trino.plugin.hive.avro.HiveAvroConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class HiveAvroTypeManager
        extends AvroNativeLogicalTypeManager
{
    private final AtomicReference<ZoneId> convertToTimezone = new AtomicReference<>(UTC);
    private boolean skipConversion;

    public HiveAvroTypeManager(Configuration configuration)
    {
        this.skipConversion = HiveConf.getBoolVar(
                requireNonNull(configuration, "configuration is null"), HiveConf.ConfVars.HIVE_AVRO_TIMESTAMP_SKIP_CONVERSION);
    }

    @Override
    public void configure(Map<String, byte[]> fileMetaData)
    {
        if (fileMetaData.containsKey(HiveAvroConstants.WRITER_TIME_ZONE)) {
            convertToTimezone.set(ZoneId.of(new String(fileMetaData.get(HiveAvroConstants.WRITER_TIME_ZONE), StandardCharsets.UTF_8)));
        }
        else if (!skipConversion) {
            convertToTimezone.set(TimeZone.getDefault().toZoneId());
        }
    }

    @Override
    public Optional<Type> overrideTypeForSchema(Schema schema)
    {
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        return switch (result) {
            // mapped in from HiveType translator
            case AvroNativeLogicalTypeManager.NoLogicalType ignored -> Optional.empty();
            case AvroNativeLogicalTypeManager.NotNativeAvroLogicalType notNativeAvroLogicalType -> {
                if (!notNativeAvroLogicalType.getLogicalTypeName().equals(VARCHAR_TYPE_LOGICAL_NAME) && !notNativeAvroLogicalType.getLogicalTypeName().equals(CHAR_TYPE_LOGICAL_NAME)) {
                    yield Optional.empty();
                }
                yield Optional.of(getHiveLogicalVarCharOrCharType(schema, notNativeAvroLogicalType));
            }
            case AvroNativeLogicalTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType -> switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
            case AvroNativeLogicalTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType -> switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case TIMESTAMP_MILLIS, DATE -> super.overrideTypeForSchema(schema);
                case DECIMAL -> {
                    if (schema.getType().equals(Schema.Type.FIXED)) {
                        // for backwards compatibility
                        throw new AvroTypeException("Hive does not support fixed decimal types");
                    }
                    yield super.overrideTypeForSchema(schema);
                }
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        };
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> overrideBuildingFunctionForSchema(Schema schema)
    {
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        return switch (result) {
            case AvroNativeLogicalTypeManager.NoLogicalType ignored -> Optional.empty();
            case AvroNativeLogicalTypeManager.NotNativeAvroLogicalType notNativeAvroLogicalType -> {
                if (!notNativeAvroLogicalType.getLogicalTypeName().equals(VARCHAR_TYPE_LOGICAL_NAME) && !notNativeAvroLogicalType.getLogicalTypeName().equals(CHAR_TYPE_LOGICAL_NAME)) {
                    yield Optional.empty();
                }
                Type type = getHiveLogicalVarCharOrCharType(schema, notNativeAvroLogicalType);
                if (notNativeAvroLogicalType.getLogicalTypeName().equals(VARCHAR_TYPE_LOGICAL_NAME)) {
                    yield Optional.of(((blockBuilder, obj) -> {
                        type.writeSlice(blockBuilder, Varchars.truncateToLength(Slices.utf8Slice(obj.toString()), type));
                    }));
                }
                else {
                    yield Optional.of(((blockBuilder, obj) -> {
                        type.writeSlice(blockBuilder, Chars.truncateToLengthAndTrimSpaces(Slices.utf8Slice(obj.toString()), type));
                    }));
                }
            }
            case AvroNativeLogicalTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType -> switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
            case AvroNativeLogicalTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType -> switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case TIMESTAMP_MILLIS -> {
                    yield Optional.of(((blockBuilder, obj) -> {
                        Long millisSinceEpochUTC = (Long) obj;
                        TimestampType.TIMESTAMP_MILLIS.writeLong(blockBuilder, DateTimeZone.forTimeZone(TimeZone.getTimeZone(convertToTimezone.get())).convertUTCToLocal(millisSinceEpochUTC) * Timestamps.MICROSECONDS_PER_MILLISECOND);
                    }));
                }
                case DATE -> super.overrideBuildingFunctionForSchema(schema);
                case DECIMAL -> {
                    if (schema.getType().equals(Schema.Type.FIXED)) {
                        // for backwards compatibility
                        throw new AvroTypeException("Hive does not support fixed decimal types");
                    }
                    yield super.overrideBuildingFunctionForSchema(schema);
                }
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        };
    }

    private static Type getHiveLogicalVarCharOrCharType(Schema schema, NotNativeAvroLogicalType notNativeAvroLogicalType)
    {
        if (!schema.getType().equals(Schema.Type.STRING)) {
            throw new AvroTypeException("Unsupported Avro type for Hive Logical Type in schema " + schema.toString());
        }
        Object maxLengthObject = schema.getObjectProp(VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP);
        if (maxLengthObject == null) {
            throw new AvroTypeException("Missing property maxLength in schema for Hive Type " + notNativeAvroLogicalType.getLogicalTypeName());
        }
        try {
            int maxLength = 0;
            if (maxLengthObject instanceof String maxLengthString) {
                maxLength = Integer.parseInt(maxLengthString);
            }
            else if (maxLengthObject instanceof Number maxLengthNumber) {
                maxLength = maxLengthNumber.intValue();
            }
            else {
                throw new AvroTypeException("Unrecognized property type for " + VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP + " in schema " + schema);
            }
            if (notNativeAvroLogicalType.getLogicalTypeName().equals(VARCHAR_TYPE_LOGICAL_NAME)) {
                return createVarcharType(maxLength);
            }
            else {
                return createCharType(maxLength);
            }
        }
        catch (NumberFormatException numberFormatException) {
            throw new AvroTypeException("Property maxLength not convertible to Integer in Hive Logical type schema " + schema.toString());
        }
    }
}
