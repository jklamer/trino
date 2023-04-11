package io.trino.hive.formats.avro;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

public abstract class AvroTypeManager
{
    public abstract void configure(Map<String, byte[]> fileMetaData);

    public abstract Optional<Type> typeForSchema(Schema schema);

    /**
     * Object provided by FasterReader's deserialization with no conversions.
     * Object class determined by Avro's standard generic data process
     * BlockBuilder provided by Type returned above for the schema
     */
    public abstract Optional<BiConsumer<BlockBuilder, Object>> buildingFunctionForSchema(Schema schema);
}
