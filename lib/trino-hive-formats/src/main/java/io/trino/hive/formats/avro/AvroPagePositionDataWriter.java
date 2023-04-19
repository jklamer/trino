package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hadoop.$internal.org.apache.commons.lang3.NotImplementedException;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import net.bytebuddy.implementation.Implementation;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public class AvroPagePositionDataWriter
        implements DatumWriter<Integer>
{
    private Page page;
    private Schema schema;

    @Override
    public void setSchema(Schema schema)
    {
        this.schema = requireNonNull(schema, "schema is null");
    }

    public void setPage(Page page)
    {
        this.page = requireNonNull(page, "page is null");
    }

    @Override
    public void write(Integer position, Encoder encoder)
            throws IOException
    {
        if (position >= page.getPositionCount()) {
            throw new IndexOutOfBoundsException("Position %s not within page with position count %s".formatted(position, page.getPositionCount()));
        }
    }

    private abstract static class BlockPositionEncoder
    {
        enum SimpleUnionNullIndex {
            ZERO(0),
            ONE(1);
            public final int idx;
            SimpleUnionNullIndex(int idx)
            {
                this.idx = idx;
            }
        }

        protected Block block;
        private final Optional<SimpleUnionNullIndex> isNullWithIndex;

        protected BlockPositionEncoder()
        {
            this(Optional.empty());
        }

        protected BlockPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex)
        {
            this.isNullWithIndex = requireNonNull(isNullWithIndex, "isNullWithIndex is null");
        }

        abstract void encodeFromBlock(int position, Encoder encoder)
                throws IOException;

        void encode(int position, Encoder encoder)
                throws IOException
        {
            requireNonNull(block, "block must be set before calling encode");
            if (block.isNull(position) && isNullWithIndex.isEmpty()) {
                throw new IOException("Can not write null value for non-nullable schema");
            }
            if (isNullWithIndex.isPresent()) {
                encoder.writeIndex(block.isNull(position) ? isNullWithIndex.get().idx : 1 ^ isNullWithIndex.get().idx);
            }
            if (block.isNull(position)) {
                encoder.writeNull();
            }
            else {
                encodeFromBlock(position, encoder);
            }
        }

        void setBlock(Block block)
        {
            this.block = block;
        }
    }

    private static BlockPositionEncoder createBlockPositionEncoder(Schema schema, Type type) {
        throw new NotImplementedException("not done");
    }

    private static class BooleanBlockPositionEncoder
            extends BlockPositionEncoder
    {
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeBoolean(BOOLEAN.getBoolean(block, position));
        }
    }

    private static class IntBlockPositionEncoder
            extends BlockPositionEncoder
    {
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeInt(INTEGER.getInt(block, position));
        }
    }

    private static class LongBlockPositionEncoder
            extends BlockPositionEncoder
    {
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeLong(BIGINT.getLong(block, position));
        }
    }

    private static class FloatBlockPositionEncoder
            extends BlockPositionEncoder
    {
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeFloat(RealType.REAL.getFloat(block, position));
        }
    }

    private static class DoubleBlockPositionEncoder
            extends BlockPositionEncoder
    {
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeDouble(DOUBLE.getDouble(block, position));
        }
    }

    private static class StringOrBytesPositionEncoder
            extends BlockPositionEncoder
    {
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            int length = block.getSliceLength(position);
            encoder.writeLong(length);
            encoder.writeFixed(block.getSlice(position, 0, length).getBytes());
        }
    }

    private static class FixedBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final int fixedSize;

        public FixedBlockPositionEncoder(int fixedSize)
        {
            this.fixedSize = fixedSize;
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            int length = block.getSliceLength(position);
            if (length != fixedSize) {
                throw new IOException("Unable to write Avro fixed with size %s from slice of length %s".formatted(fixedSize, length));
            }
            encoder.writeFixed(block.getSlice(position, 0, length).getBytes());
        }
    }

    private static class EnumBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final Map<Slice, Integer> symbolToIndex;
        public EnumBlockPositionEncoder(List<String> symbols)
        {
            ImmutableMap.Builder<Slice, Integer> symbolToIndex = ImmutableMap.builder();
            for (int i = 0; i < symbols.size(); i++) {
                symbolToIndex.put(Slices.utf8Slice(symbols.get(i)), i);
            }
            this.symbolToIndex = symbolToIndex.buildOrThrow();
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            int length = block.getSliceLength(position);
            Integer symbolIndex = symbolToIndex.get(block.getSlice(position, 0, length));
            if (symbolIndex == null) {
                throw new IOException("Unable to write Avro Enum symbol %s. Not found in set %s".formatted(block.getSlice(position, 0, length).toStringUtf8(), symbolToIndex.keySet().stream().map(Slice::toStringUtf8).collect(toImmutableList())));
            }
            encoder.writeEnum(symbolIndex);
        }
    }

    private static class ArrayBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private BlockPositionEncoder elementBlockPositionEncoder;
        private ArrayType type;

        public ArrayBlockPositionEncoder(Type type)
        {
            this.type = requireNonNullInstanceOf(ArrayType.class, type);

        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            Block elementBlock = type.getObject(block, position);
            elementBlockPositionEncoder.setBlock(elementBlock);
            int size = elementBlock.getPositionCount();
            encoder.writeArrayStart();
            encoder.setItemCount(size);
            for (int itemPos = 0; itemPos < size; itemPos++) {
                encoder.startItem();
                elementBlockPositionEncoder.encode(itemPos, encoder);
            }
            encoder.writeArrayEnd();
        }
    }

    private static class MapBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private BlockPositionEncoder keyBlockPositionEncoder;
        private BlockPositionEncoder valueBlockPositionEncoder;
        private final MapType type;

        public MapBlockPositionEncoder(Type type)
        {
            this.type = requireNonNullInstanceOf(MapType.class, type);
            // Todo throw type exception
            verify(VARCHAR == this.type.getKeyType());
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            ColumnarMap singleMap = ColumnarMap.toColumnarMap(type.getObject(block, position));
            Block keyBlock = singleMap.getKeysBlock();
            Block valueBlock = singleMap.getValuesBlock();
            verify(keyBlock.getPositionCount() == valueBlock.getPositionCount());
            keyBlockPositionEncoder.setBlock(keyBlock);
            valueBlockPositionEncoder.setBlock(valueBlock);
            int size = keyBlock.getPositionCount();
            encoder.writeMapStart();
            encoder.setItemCount(size);
            for (int entryPos = 0; entryPos < size; entryPos++) {
                encoder.startItem();
                keyBlockPositionEncoder.encode(entryPos, encoder);
                valueBlockPositionEncoder.encode(entryPos, encoder);
            }
            encoder.writeMapEnd();
        }
    }


    private static class RecordBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private BlockPositionEncoder keyBlockPositionEncoder;
        private BlockPositionEncoder valueBlockPositionEncoder;
        private RowType type;
        private BlockPositionEncoder[] channelEncoders;
        private int[] fieldToChannel;

        public RecordBlockPositionEncoder(Type type, Schema schema)
                throws AvroTypeException
        {
            this.type = requireNonNullInstanceOf(RowType.class, type);
            List<RowType.Field> typeFields = this.type.getFields();
            verify(schema.getType() == Schema.Type.RECORD);
            verify(schema.getFields().size() == typeFields.size());
            fieldToChannel = new int[schema.getFields().size()];
            for (int i = 0; i < typeFields.size(); i++) {
                String fieldName = typeFields.get(0).getName().orElseThrow(() -> new AvroTypeException("Row Type fields must have name"));
                Schema.Field avroField = schema.getField(fieldName);
                fieldToChannel[avroField.pos()] = i;

            }
        }
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {

        }
    }

    private static <C> C requireNonNullInstanceOf(Class<C> castTo, Object obj)
    {
        requireNonNull(obj);
        if (castTo.isInstance(obj)) {
            return castTo.cast(obj);
        }
        throw new ClassCastException("Cannot cast object with class %s to %s".formatted(obj.getClass(), castTo));
    }
}
