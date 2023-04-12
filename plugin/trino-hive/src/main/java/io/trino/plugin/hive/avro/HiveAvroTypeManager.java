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

import io.trino.hive.formats.avro.AvroNativeLogicalTypeManager;
import io.trino.hive.formats.avro.AvroTypeManager;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiConsumer;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class HiveAvroTypeManager
        extends AvroNativeLogicalTypeManager
{
    private ZoneId writerTimezone = UTC;
    private boolean skipConversion = false;

    public HiveAvroTypeManager(Configuration configuration)
    {
        this.skipConversion = HiveConf.getBoolVar(
                requireNonNull(configuration, "configuration is null"), HiveConf.ConfVars.HIVE_AVRO_TIMESTAMP_SKIP_CONVERSION);
    }

    @Override
    public void configure(Map<String, byte[]> fileMetaData)
    {
        if (fileMetaData.containsKey(HiveAvroConstants.WRITER_TIME_ZONE)) {
            writerTimezone = ZoneId.of(new String(fileMetaData.get(HiveAvroConstants.WRITER_TIME_ZONE), StandardCharsets.UTF_8));
        } else if (!skipConversion) {
            writerTimezone = TimeZone.getDefault().toZoneId();
        }
    }

    @Override
    public Optional<Type> overrideTypeForSchema(Schema schema)
    {
        String logicalTypeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        if (logicalTypeName == null) {
            return Optional.empty();
        }
        switch (logicalTypeName) {
            case TIMESTAMP_MILLIS -> {
                if(!schema.getType().equals(Schema.Type.LONG)) {
                    throw new AvroTypeException("Invalid ")
                }
            }
            default -> {}
        }
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> overrideBuildingFunctionForSchema(Schema schema)
    {
        return defaultManager.buildingFunctionForSchema(schema);
    }
}
