package io.trino.hive.formats.avro;

import com.sun.jdi.LongType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hadoop.$internal.org.apache.commons.lang3.NotImplementedException;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.hadoop.hdfs.protocol.Block;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

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
    private Schema readerSchema;
    private Schema writerSchema;
    private final Type readerSchemaType;
    private final BinaryDecoder binaryDecoder;
    private ResolvingDecoder decoder;
    private PageBuilder pageBuilder;
    private RowBlockBuildingDecoder rowBlockBuildingDecoder;

    public AvroPageDataReader(Schema readerSchema, InputStream inputStream)
            throws IOException
    {
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        this.writerSchema = this.readerSchema;
        this.readerSchemaType = typeFromAvro(this.readerSchema);
        verify(readerSchemaType instanceof RowType, "Can only build pages when top level type is Row");
        this.pageBuilder = new PageBuilder(this.readerSchemaType.getTypeParameters());
        this.binaryDecoder = DecoderFactory.get().binaryDecoder(requireNonNull(inputStream, "inputStream is null"), null);
        initialize();
    }

    private void initialize()
            throws IOException
    {
        verify(readerSchema.getType().equals(Schema.Type.RECORD), "Avro schema for page reader must be record");
        verify(writerSchema.getType().equals(Schema.Type.RECORD), "File Avro schema for page reader must be record");
        this.decoder = DecoderFactory.get().resolvingDecoder(this.readerSchema, this.writerSchema, binaryDecoder);
        this.rowBlockBuildingDecoder = new RowBlockBuildingDecoder(writerSchema, readerSchema);
    }

    @Override
    public void setSchema(Schema schema)
    {
        if (schema != null) {
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
    public Optional<Page> read(Optional<Page> ignoredReuse, Decoder ignoredDecoder)
            throws IOException
    {


        this.decoder.drain();
        Optional<Page> page = Optional.empty();
        if(pageBuilder.isFull()) {
            page = Optional.of(pageBuilder.build());
            pageBuilder.reset();
        }
        return page;
    }


    private static abstract class BlockBuildingDecoder
    {
        protected abstract void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder) throws IOException;
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

        private void skip(ResolvingDecoder decoder)
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

        public void decode(ResolvingDecoder decoder, IntFunction<BlockBuilder> channelSelector)
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
        private final Consumer<BlockBuilder> addConstantFunction;
        private final int outputChannel;

        public ConstantBlockAction(Consumer<BlockBuilder> addConstantFunction, int outputChannel)
        {
            this.addConstantFunction = requireNonNull(addConstantFunction, "addConstantFunction is null");
            checkArgument(outputChannel >= 0, "outputChannel must be positive");
            this.outputChannel = outputChannel;
        }

        public void addConstant(IntFunction<BlockBuilder> channelSelector)
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

        private RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema)
        {
            this(Resolver.resolve(writeSchema, readSchema));
        }

        private RowBlockBuildingDecoder(Resolver.Action action)
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
                        buildSteps[i] = new BuildIntoBlockAction(null, readField.pos());
                    }
                }

                // add defaulting if required
                for (; i < buildSteps.length; i++) {
                    // create constant block
                    Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                    buildSteps[i] = new ConstantBlockAction(null, readField.pos()); // TODO implement constants
                }

                verify(Arrays.stream(buildSteps).mapToInt(RowBuildingAction::getOutputChannel).filter(i -> i >= 0).distinct().sum() == (recordAdjust.reader.getFields().size() * (recordAdjust.reader.getFields().size() + 1) / 2),
                        "Every channel in output block builder must be accounted for");
                verify(Arrays.stream(buildSteps).mapToInt(RowBuildingAction::getOutputChannel).filter(i -> i >= 0).distinct().count() == (long) recordAdjust.reader.getFields().size(), "Every channel in output block builder must be accounted for");
            }
            else {
                throw new RuntimeException("Write and Read Schemas must be records when building a row block building decoder");
            }
        }

        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            SingleRowBlockWriter currentBuilder = (SingleRowBlockWriter) builder.beginBlockEntry();
            decodeIntoField(decoder, currentBuilder::getFieldBlockBuilder);
            builder.closeEntry();
        }

        protected void decodeIntoPageBuilder(ResolvingDecoder decoder, PageBuilder builder) throws IOException
        {
            builder.declarePosition();
            decodeIntoField(decoder, builder::getBlockBuilder);
        }

        protected void decodeIntoField(ResolvingDecoder decoder, IntFunction<BlockBuilder> fieldBuilders)
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
        private final BlockBuildingDecoder valueBlockBuildingDevoder;

        public MapBlockBuildingDecoder(Resolver.Container containerAction)
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.MAP, "Reader schema must be a map");
            this.valueBlockBuildingDevoder = createBlockBuildingDecoderForAction(containerAction.elementAction);
        }

        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            throw new NotImplementedException("Need to figure out this key uniqueness stuff");
        }
    }

    private static class ArrayBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder elementBlockBuildingDecoder;

        public ArrayBlockBuildingDecoder(Resolver.Container containerAction)
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.ARRAY, "Reader schema must be a array");
            this.elementBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction);
        }

        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
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
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, decoder.readInt());
        }
    }

    private static class LongBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, decoder.readLong());
        }
    }

    private static class FloatBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            REAL.writeLong(builder, floatToRawIntBits(decoder.readFloat()));
        }
    }

    private static class DoubleBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            DOUBLE.writeDouble(builder, decoder.readDouble());
        }
    }

    private static class BooleanBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            BOOLEAN.writeBoolean(builder, decoder.readBoolean());
        }
    }

    private static class NullBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readNull();
            builder.appendNull();
        }
    }

    private static class FixedBlockBuildingDecoder extends BlockBuildingDecoder
    {
        private final int expectedSize;
        public FixedBlockBuildingDecoder(Schema fixedSchema)
        {
            verify(requireNonNull(fixedSchema, "fixedSchema is null").getType().equals(Schema.Type.FIXED), "must use with fixed schema");
            expectedSize = fixedSchema.getFixedSize();
        }

        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            byte[] slice = new byte[expectedSize];
            decoder.readFixed(slice);
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(slice));
        }
    }

    private static class BytesBlockBuildingDecoder extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(decoder.readBytes(ByteBuffer.allocate(32))));
        }
    }

    private static class StringBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeString(builder, decoder.readString());
        }
    }

    private static BlockBuildingDecoder createBlockBuildingDecoderForAction(Resolver.Action action)
    {
        return switch (action.type) {
            case CONTAINER -> switch (action.reader.getType()) {
                case MAP -> {
                    throw new NotImplementedException("hey");
                }
                case ARRAY -> new ArrayBlockBuildingDecoder((Resolver.Container) action);
                default -> {
                    throw new IllegalStateException("Error getting reader for action type " + action.getClass());
                }
            };
            case DO_NOTHING -> switch (action.reader.getType()) {
                case INT -> new IntBlockBuildingDecoder();
                case LONG -> new LongBlockBuildingDecoder();
                case FLOAT -> new FloatBlockBuildingDecoder();
                case DOUBLE -> new DoubleBlockBuildingDecoder();
                case BOOLEAN -> new BooleanBlockBuildingDecoder();
                case NULL -> new NullBlockBuildingDecoder();
                case FIXED -> new FixedBlockBuildingDecoder(action.reader);
                case STRING -> new StringBlockBuildingDecoder();
                case BYTES -> new BytesBlockBuildingDecoder();
                case RECORD, ENUM, ARRAY, MAP, UNION -> {
                    throw new IllegalStateException("Do Nothing action type not compatible with reader schema type " + action.reader.getType());
                }
            };
            case RECORD -> new RowBlockBuildingDecoder(action);
            case ENUM ->
                throw new NotImplementedException("hey");
                //return createEnumReader((Resolver.EnumAdjust) action);
            case PROMOTE:
                throw new NotImplementedException("hey");
                // return createPromotingReader((Resolver.Promote) action);
            case WRITER_UNION:
                throw new NotImplementedException("hey");
                //return createUnionReader((Resolver.WriterUnion) action);
            case READER_UNION:
                throw new NotImplementedException("hey");
//                return getReaderFor(((Resolver.ReaderUnion) action).actualAction, null);
            case ERROR:
                throw new UncheckedIOException("Not sure what this exception should be but should throw");
            default:
                throw new IllegalStateException("Error getting reader for action type " + action.getClass());
        }
    }

    private static BlockBuildingDecoder createPageBuildingDecoder(Schema schema, PageBuilder pageBuilder)
    {
        switch (schema.getType()) {
            case RECORD -> {
            }
            case ENUM -> {

            }
            case ARRAY -> {
            }
            case MAP -> {
            }
            case UNION -> {
            }
            case FIXED -> {
            }
            case STRING -> {
            }
            case BYTES -> {
            }
            case INT -> {

            }
            case LONG -> {
            }
            case FLOAT -> {
            }
            case DOUBLE -> {
            }
            case BOOLEAN -> {
            }
            case NULL -> {
            }
        }
    }
}
