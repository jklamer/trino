package io.trino.hive.formats.avro;

import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.util.internal.Accessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.AvroTypeUtils.typeFromAvro;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class AvroPageDataReader
        implements DatumReader<Optional<Page>>
{
    private final Schema readerSchema;
    private Schema writerSchema;
    private final PageBuilder pageBuilder;
    private RowBlockBuildingDecoder rowBlockBuildingDecoder;

    public AvroPageDataReader(Schema readerSchema)
            throws IOException
    {
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        this.writerSchema = this.readerSchema;
        Type readerSchemaType = typeFromAvro(this.readerSchema);
        verify(readerSchemaType instanceof RowType, "Can only build pages when top level type is Row");
        this.pageBuilder = new PageBuilder(readerSchemaType.getTypeParameters());
        initialize();
    }

    private void initialize()
            throws IOException
    {
        verify(readerSchema.getType().equals(Schema.Type.RECORD), "Avro schema for page reader must be record");
        verify(writerSchema.getType().equals(Schema.Type.RECORD), "File Avro schema for page reader must be record");
        this.rowBlockBuildingDecoder = new RowBlockBuildingDecoder(writerSchema, readerSchema);
    }

    @Override
    public void setSchema(Schema schema)
    {
        if (schema != null && schema != writerSchema) {
            writerSchema = schema;
            try {
                initialize();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public Optional<Page> read(Optional<Page> ignoredReuse, Decoder decoder)
            throws IOException
    {
        Optional<Page> page = Optional.empty();
        rowBlockBuildingDecoder.decodeIntoPageBuilder(decoder, pageBuilder);
        if (pageBuilder.isFull()) {
            page = Optional.of(pageBuilder.build());
            pageBuilder.reset();
        }
        return page;
    }

    public Optional<Page> flush()
            throws IOException
    {
        if (!pageBuilder.isEmpty()) {
            Optional<Page> lastPage =  Optional.of(pageBuilder.build());
            pageBuilder.reset();
            return lastPage;
        }
        return Optional.empty();
    }

    private static abstract class BlockBuildingDecoder
    {
        protected abstract void decodeIntoBlock(Decoder decoder, BlockBuilder builder) throws IOException;
    }


    private sealed interface RowBuildingAction permits SkipSchemaBuildingAction, BuildIntoBlockAction, ConstantBlockAction
    {
        int getOutputChannel();
    }

    private static final class SkipSchemaBuildingAction
            implements RowBuildingAction
    {
        private final Schema schema;

        SkipSchemaBuildingAction(Schema schema)
        {
            this.schema = requireNonNull(schema, "schema is null");
        }

        private void skip(Decoder decoder)
                throws IOException
        {
            GenericDatumReader.skip(schema, decoder);
        }

        @Override
        public int getOutputChannel()
        {
            return -1;
        }
    }

    private static final class BuildIntoBlockAction
            implements RowBuildingAction
    {
        private final BlockBuildingDecoder delegate;
        private final int outputChannel;

        public BuildIntoBlockAction(BlockBuildingDecoder delegate, int outputChannel)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            checkArgument(outputChannel >= 0, "outputChannel must be positive");
            this.outputChannel = outputChannel;
        }

        public void decode(Decoder decoder, IntFunction<BlockBuilder> channelSelector)
                throws IOException
        {
            delegate.decodeIntoBlock(decoder, channelSelector.apply(outputChannel));
        }

        @Override
        public int getOutputChannel()
        {
            return outputChannel;
        }
    }

    private static final class ConstantBlockAction
            implements RowBuildingAction
    {
        private final IoConsumer<BlockBuilder> addConstantFunction;
        private final int outputChannel;

        public ConstantBlockAction(IoConsumer<BlockBuilder> addConstantFunction, int outputChannel)
        {
            this.addConstantFunction = requireNonNull(addConstantFunction, "addConstantFunction is null");
            checkArgument(outputChannel >= 0, "outputChannel must be positive");
            this.outputChannel = outputChannel;
        }

        public void addConstant(IntFunction<BlockBuilder> channelSelector) throws IOException
        {
            addConstantFunction.accept(channelSelector.apply(outputChannel));
        }

        @Override
        public int getOutputChannel()
        {
            return outputChannel;
        }
    }

    private static class RowBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        RowBuildingAction[] buildSteps;

        private RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema) throws IOException
        {
            this(Resolver.resolve(writeSchema, readSchema));
        }

        private RowBlockBuildingDecoder(Resolver.Action action)
                throws IOException
        {
            if (action instanceof Resolver.ErrorAction errorAction) {
                throw new RuntimeException("Error in resolution of types for row building: " + errorAction.error);
            }
            else if (action instanceof Resolver.RecordAdjust recordAdjust) {
                buildSteps = new RowBuildingAction[recordAdjust.fieldActions.length + recordAdjust.readerOrder.length
                        - recordAdjust.firstDefault];
                int i = 0;
                int readerFieldCount = 0;
                for (; i < recordAdjust.fieldActions.length; i++) {
                    Resolver.Action fieldAction = recordAdjust.fieldActions[i];
                    if (fieldAction instanceof Resolver.Skip skip) {
                        buildSteps[i] = new SkipSchemaBuildingAction(skip.writer);
                    }
                    else {
                        Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                        buildSteps[i] = new BuildIntoBlockAction(createBlockBuildingDecoderForAction(fieldAction), readField.pos());
                    }
                }

                // add defaulting if required
                for (; i < buildSteps.length; i++) {
                    // create constant block
                    Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                    buildSteps[i] = new ConstantBlockAction(getDefaultBlockBuilder(readField), readField.pos());
                }

                verify(Arrays.stream(buildSteps).mapToInt(RowBuildingAction::getOutputChannel).filter(a -> a >= 0).distinct().sum() == (recordAdjust.reader.getFields().size() * (recordAdjust.reader.getFields().size() - 1) / 2),
                        "Every channel in output block builder must be accounted for");
                verify(Arrays.stream(buildSteps).mapToInt(RowBuildingAction::getOutputChannel).filter(a -> a >= 0).distinct().count() == (long) recordAdjust.reader.getFields().size(), "Every channel in output block builder must be accounted for");
            }
            else {
                throw new RuntimeException("Write and Read Schemas must be records when building a row block building decoder");
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            SingleRowBlockWriter currentBuilder = (SingleRowBlockWriter) builder.beginBlockEntry();
            decodeIntoField(decoder, currentBuilder::getFieldBlockBuilder);
            builder.closeEntry();
        }

        protected void decodeIntoPageBuilder(Decoder decoder, PageBuilder builder) throws IOException
        {
            builder.declarePosition();
            decodeIntoField(decoder, builder::getBlockBuilder);
        }

        protected void decodeIntoField(Decoder decoder, IntFunction<BlockBuilder> fieldBuilders)
                throws IOException
        {
            for (int i = 0; i < buildSteps.length; i++) {
                switch (buildSteps[i]) {
                    case SkipSchemaBuildingAction skipSchemaBuildingAction -> {
                        skipSchemaBuildingAction.skip(decoder);
                    }
                    case BuildIntoBlockAction buildIntoBlockAction -> {
                        buildIntoBlockAction.decode(decoder, fieldBuilders);
                    }
                    case ConstantBlockAction constantBlockAction -> {
                        constantBlockAction.addConstant(fieldBuilders);
                    }
                }
            }
        }
    }

    private static class MapBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder keyBlockBuildingDecoder = new StringBlockBuildingDecoder();
        private final BlockBuildingDecoder valueBlockBuildingDecoder;

        public MapBlockBuildingDecoder(Resolver.Container containerAction)
                throws IOException
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.MAP, "Reader schema must be a map");
            this.valueBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction);
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BlockBuilder entryBuilder = builder.beginBlockEntry();
            long entriesInBlock = decoder.readMapStart();
            // TODO need to filter out all but last value for key?
            if (entriesInBlock > 0) {
                do {
                    for (int i = 0; i < entriesInBlock; i++) {
                        keyBlockBuildingDecoder.decodeIntoBlock(decoder, entryBuilder);
                        valueBlockBuildingDecoder.decodeIntoBlock(decoder, entryBuilder);
                    }
                }
                while ((entriesInBlock = decoder.mapNext()) > 0);
            }
            builder.closeEntry();
        }
    }

    private static class ArrayBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder elementBlockBuildingDecoder;

        public ArrayBlockBuildingDecoder(Resolver.Container containerAction) throws IOException
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.ARRAY, "Reader schema must be a array");
            this.elementBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction);
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BlockBuilder elementBuilder = builder.beginBlockEntry();
            long elementsInBlock = decoder.readArrayStart();
            if (elementsInBlock > 0) {
                do {
                    for (int i = 0; i < elementsInBlock; i++) {
                        elementBlockBuildingDecoder.decodeIntoBlock(decoder, elementBuilder);
                    }
                }
                while ((elementsInBlock = decoder.arrayNext()) > 0);
            }
            builder.closeEntry();
        }
    }

    private static class IntBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, decoder.readInt());
        }
    }

    private static class LongBlockBuildingDecoder extends BlockBuildingDecoder
    {
        private static final LongIoFunction<Decoder> DEFAULT_EXTRACT_LONG = Decoder::readLong;
        private final LongIoFunction<Decoder> extractLong;

        public LongBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_LONG);
        }

        public LongBlockBuildingDecoder(LongIoFunction<Decoder> extractLong)
        {
            this.extractLong = requireNonNull(extractLong, "extractLong is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, extractLong.apply(decoder));
        }
    }

    private static class FloatBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final FloatIoFunction<Decoder> DEFAULT_EXTRACT_FLOAT = Decoder::readFloat;
        private final FloatIoFunction<Decoder> extractFloat;

        public FloatBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_FLOAT);
        }

        public FloatBlockBuildingDecoder(FloatIoFunction<Decoder> extractFloat)
        {
            this.extractFloat = requireNonNull(extractFloat, "extractFloat is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            REAL.writeLong(builder, floatToRawIntBits(extractFloat.apply(decoder)));
        }
    }

    private static class DoubleBlockBuildingDecoder extends BlockBuildingDecoder
    {
        private static final DoubleIoFunction<Decoder> DEFAULT_EXTRACT_DOUBLE = Decoder::readDouble;
        private final DoubleIoFunction<Decoder> extractDouble;

        public DoubleBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_DOUBLE);
        }

        public DoubleBlockBuildingDecoder(DoubleIoFunction<Decoder> extractDouble)
        {
            this.extractDouble = requireNonNull(extractDouble, "extractDouble is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            DOUBLE.writeDouble(builder, extractDouble.apply(decoder));
        }
    }

    private static class BooleanBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BOOLEAN.writeBoolean(builder, decoder.readBoolean());
        }
    }

    private static class NullBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readNull();
            builder.appendNull();
        }
    }

    private static class FixedBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final int expectedSize;

        public FixedBlockBuildingDecoder(int expectedSize)
        {
            verify(expectedSize >= 0, "expected size must be greater than or equal to 0");
            this.expectedSize = expectedSize;
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            byte[] slice = new byte[expectedSize];
            decoder.readFixed(slice);
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(slice));
        }
    }

    private static class BytesBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final IOFunction<Decoder, Slice> DEFAULT_EXTRACT_BYTES = decoder -> Slices.wrappedBuffer(decoder.readBytes(ByteBuffer.allocate(32)));
        private final IOFunction<Decoder, Slice> extractBytes;

        public BytesBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_BYTES);
        }

        public BytesBlockBuildingDecoder(IOFunction<Decoder, Slice> extractBytes)
        {
            this.extractBytes = requireNonNull(extractBytes, "extractBytes is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARBINARY.writeSlice(builder, extractBytes.apply(decoder));
        }
    }

    private static class StringBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final IOFunction<Decoder, String> DEFAULT_EXTRACT_STRING = Decoder::readString;
        private final IOFunction<Decoder, String> extractString;

        public StringBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_STRING);
        }

        public StringBlockBuildingDecoder(IOFunction<Decoder, String> extractString)
        {
            this.extractString = requireNonNull(extractString, "extractString is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeString(builder, extractString.apply(decoder));
        }
    }

    private static class EnumBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private String[] symbols;

        public EnumBlockBuildingDecoder(Resolver.EnumAdjust action)
        {
            List<String> symbolsList = requireNonNull(action, "action is null").reader.getEnumSymbols();
            symbols = symbolsList.toArray(String[]::new);
            if (!action.noAdjustmentsNeeded) {
                String[] adjustedSymbols = new String[(action.writer.getEnumSymbols().size())];
                for (int i = 0; i < action.adjustments.length; i++) {
                    if (action.adjustments[i] < 0) {
                        throw new AvroTypeException("No reader Enum value for writer Enum value " + action.writer.getEnumSymbols().get(i));
                    }
                    adjustedSymbols[i] = symbols[action.adjustments[i]];
                }
                symbols = adjustedSymbols;
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeString(builder, symbols[decoder.readEnum()]);
        }
    }

    private static class WriterUnionBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder[] blockBuildingDecoders;

        public WriterUnionBlockBuildingDecoder(Resolver.WriterUnion writerUnion)
                throws IOException
        {
            blockBuildingDecoders = new BlockBuildingDecoder[writerUnion.actions.length];
            for (int i = 0; i < writerUnion.actions.length; i++) {
                blockBuildingDecoders[i] = createBlockBuildingDecoderForAction(writerUnion.actions[i]);
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            int writeIndex = decoder.readIndex();
            blockBuildingDecoders[writeIndex].decodeIntoBlock(decoder, builder);
        }
    }


    private static BlockBuildingDecoder createBlockBuildingDecoderForAction(Resolver.Action action) throws IOException
    {
        return switch (action.type) {
            case CONTAINER -> switch (action.reader.getType()) {
                case MAP -> new MapBlockBuildingDecoder((Resolver.Container) action);
                case ARRAY -> new ArrayBlockBuildingDecoder((Resolver.Container) action);
                default -> throw new AvroTypeException("Not possible to have container action type with non container reader schema " + action.reader.getType());
            };
            case DO_NOTHING -> switch (action.reader.getType()) {
                case INT -> new IntBlockBuildingDecoder();
                case LONG -> new LongBlockBuildingDecoder();
                case FLOAT -> new FloatBlockBuildingDecoder();
                case DOUBLE -> new DoubleBlockBuildingDecoder();
                case BOOLEAN -> new BooleanBlockBuildingDecoder();
                case NULL -> new NullBlockBuildingDecoder();
                case FIXED -> new FixedBlockBuildingDecoder(action.reader.getFixedSize());
                case STRING -> new StringBlockBuildingDecoder();
                case BYTES -> new BytesBlockBuildingDecoder();
                // these reader types covered by special action types
                case RECORD, ENUM, ARRAY, MAP, UNION -> {
                    throw new IllegalStateException("Do Nothing action type not compatible with reader schema type " + action.reader.getType());
                }
            };
            case RECORD -> new RowBlockBuildingDecoder(action);
            case ENUM -> new EnumBlockBuildingDecoder((Resolver.EnumAdjust) action);
            case PROMOTE -> switch (action.reader.getType()) {
                // only certain types valid to promote into as determined by org.apache.avro.Resolver.Promote.isValid
                case BYTES -> new BytesBlockBuildingDecoder(getBytesPromotionFunction(action.writer));
                case STRING -> new StringBlockBuildingDecoder(getStringPromotionFunction(action.writer));
                case FLOAT -> new FloatBlockBuildingDecoder(getFloatPromotionFunction(action.writer));
                case LONG -> new LongBlockBuildingDecoder(getLongPromotionFunction(action.writer));
                case DOUBLE -> new DoubleBlockBuildingDecoder(getDoublePromotionFunction(action.writer));
                case BOOLEAN, NULL, RECORD, ENUM, ARRAY, MAP, UNION, FIXED, INT ->
                        throw new AvroTypeException("Promotion action not allowed for reader schema type " + action.reader.getType());
            };
            case WRITER_UNION -> new WriterUnionBlockBuildingDecoder((Resolver.WriterUnion) action);
            case READER_UNION -> createBlockBuildingDecoderForAction(((Resolver.ReaderUnion) action).actualAction);
            case ERROR -> throw new AvroTypeException("Resolution action returned with error " + action.toString());
            case SKIP -> throw new VerifyException("Skips filtered by row step");
        };
    }

    private static IOFunction<Decoder, Slice> getBytesPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case STRING -> decoder -> Slices.wrappedBuffer(decoder.readString().getBytes(StandardCharsets.UTF_8));
            default -> throw new AvroTypeException("Cannot promote type %s to bytes".formatted(writerSchema.getType()));
        };
    }

    private static IOFunction<Decoder, String> getStringPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case BYTES -> decoder -> new String(decoder.readBytes(null).array());
            default -> throw new AvroTypeException("Cannot promote type %s to string".formatted(writerSchema.getType()));
        };
    }

    private static LongIoFunction<Decoder> getLongPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case DOUBLE -> decoder -> (long) decoder.readDouble();
            default -> throw new AvroTypeException("Cannot promote type %s to long".formatted(writerSchema.getType()));
        };
    }

    private static FloatIoFunction<Decoder> getFloatPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            default -> throw new AvroTypeException("Cannot promote type %s to float".formatted(writerSchema.getType()));
        };
    }

    private static DoubleIoFunction<Decoder> getDoublePromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            case FLOAT -> Decoder::readFloat;
            default -> throw new AvroTypeException("Cannot promote type %s to double".formatted(writerSchema.getType()));
        };
    }

    @FunctionalInterface
    private interface IOFunction<A,B>
    {
        B apply(A a) throws IOException;
    }

    @FunctionalInterface
    private interface LongIoFunction<A>
    {
        long apply(A a) throws IOException;
    }

    @FunctionalInterface
    private interface FloatIoFunction<A>
    {
        float apply(A a) throws IOException;
    }

    @FunctionalInterface
    private interface DoubleIoFunction<A>
    {
        double apply(A a) throws IOException;
    }

    @FunctionalInterface
    private interface IoConsumer<A>
    {
        void accept(A a) throws IOException;
    }

    private static IoConsumer<BlockBuilder> getDefaultBlockBuilder(Schema.Field field)
            throws IOException
    {
        BlockBuildingDecoder buildingDecoder = createBlockBuildingDecoderForAction(Resolver.resolve(field.schema(), field.schema()));
        byte[] defaultBytes = getDefaultByes(field);
        BinaryDecoder reuse = DecoderFactory.get().binaryDecoder(defaultBytes, null);
        return blockBuilder -> buildingDecoder.decodeIntoBlock(DecoderFactory.get().binaryDecoder(defaultBytes, reuse), blockBuilder);
    }

    private static byte[] getDefaultByes(Schema.Field field)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(out, null);
        ResolvingGrammarGenerator.encode(e, field.schema(), Accessor.defaultValue(field));
        e.flush();
        return out.toByteArray();
    }
}
