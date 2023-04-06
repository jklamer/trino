package io.trino.hive.formats.avro;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

public class NoOpAvroTypeManager extends AvroTypeManager
{
    public static final NoOpAvroTypeManager INSTANCE = new NoOpAvroTypeManager();

    private NoOpAvroTypeManager(){}

    @Override
    public void configure(Map<String, byte[]> fileMetaData) {}

    @Override
    public Optional<Type> typeForSchema(Schema schema)
    {
        return Optional.empty();
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> buildingFunctionForSchema(Schema schema)
    {
        return Optional.empty();
    }
}
