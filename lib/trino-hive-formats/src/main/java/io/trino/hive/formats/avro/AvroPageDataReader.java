package io.trino.hive.formats.avro;

import io.trino.hadoop.$internal.org.apache.commons.lang3.NotImplementedException;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.Type;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntFunction;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.AvroTypeUtils.typeFromAvro;
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

    public AvroPageDataReader(Schema readerSchema, InputStream inputStream)
            throws IOException
    {
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        this.writerSchema = this.readerSchema;
        this.readerSchemaType = typeFromAvro(this.readerSchema);
        this.binaryDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        initialize();
    }

    private void initialize()
            throws IOException
    {
        verify(readerSchema.getType().equals(Schema.Type.RECORD), "Avro schema for page reader must be record");
        verify(writerSchema.getType().equals(Schema.Type.RECORD), "File Avro schema for page reader must be record");
        this.decoder = DecoderFactory.get().resolvingDecoder(this.readerSchema, this.writerSchema, binaryDecoder);
    }

    @Override
    public void setSchema(Schema schema)
    {
        if(schema != null) {
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
        return Optional.empty();
    }


    private static abstract class BlockBuildingDecoder
    {
        protected abstract void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder) throws IOException;
    }

    private static class SkipSchemaBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final Schema schema;

        SkipSchemaBlockBuildingDecoder(Schema schema)
        {
            this.schema = requireNonNull(schema, "schema is null");
        }

        protected void decodeIntoBlock(ResolvingDecoder decoder, BlockBuilder builder)
                throws IOException
        {
            GenericDatumReader.skip(schema, decoder);
        }
    }

    private static class RowBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        BlockBuildingDecoder[] buildSteps;
        Integer[] channelForStep;

        private RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema)
        {
            Resolver.Action action = Resolver.resolve(writeSchema, readSchema);
            if (action instanceof Resolver.ErrorAction errorAction) {
                throw new RuntimeException("Error in resolution of types for row building: " + errorAction.error
                        + "\nWrite Schema: " + writeSchema
                        + "\nRead Schema: %s" + readSchema
                );
            }
            else if (action instanceof Resolver.RecordAdjust recordAdjust) {
                buildSteps = new BlockBuildingDecoder[recordAdjust.fieldActions.length + recordAdjust.readerOrder.length
                        - recordAdjust.firstDefault];
                channelForStep = new Integer[buildSteps.length];
                int i = 0;
                int readerFieldCount = 0;
                for(; i < recordAdjust.fieldActions.length; i++) {
                    Resolver.Action fieldAction = recordAdjust.fieldActions[i];
                    if (fieldAction instanceof Resolver.Skip skip) {
                        buildSteps[i] = new SkipSchemaBlockBuildingDecoder(skip.writer);
                    }
                    Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                    channelForStep[i] = readField.pos();

                }

                // add defaulting if required
                for (; i < buildSteps.length; i++) {
                    // create constant block
                    Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                    channelForStep[i] = readField.pos();
                    // buildSteps[i] =
                }

            }
            else {
                throw new RuntimeException("Write and Read Schemas must be records when building a row block building decoder");
            }

            verify(Arrays.stream(channelForStep).filter(Objects::nonNull).mapToInt(Integer::intValue).sum() == (readSchema.getFields().size() * (readSchema.getFields().size() + 1) / 2),
                    "Every channel in output block builder must be accounted for");
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

        protected void decodeIntoField(ResolvingDecoder decoder, IntFunction<BlockBuilder> fieldBuilders) throws IOException
        {
            for (int i = 0; i < buildSteps.length; i++) {
                buildSteps[i].decodeIntoBlock(decoder, Optional.ofNullable(channelForStep[i]).map(fieldBuilders::apply).orElse(null)); //TODO use different types of build step
            }
        }
    }

    private class IntBlockBuildingDecoder
    {
        protected void decodeIntoBlock(ResolvingDecoder decoder)
        {

        }
    }

    private static BlockBuildingDecoder createBlockBuildingDecoderForAction(Resolver.Action action)
    {
        switch (action.type) {
            case CONTAINER:
                switch (action.reader.getType()) {
                    case MAP:
                        throw new NotImplementedException("hey");
                    case ARRAY:
                        throw new NotImplementedException("hey");
                    default:
                        throw new IllegalStateException("Error getting reader for action type " + action.getClass());
                }
            case DO_NOTHING:
                throw new NotImplementedException("hey");
            case RECORD:
                throw new NotImplementedException("hey");
//                return createRecordReader((Resolver.RecordAdjust) action);
            case ENUM:
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
