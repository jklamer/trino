package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.MapBlock;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAvroPageDataReaderWithoutConversions
{
    private static final Schema SIMPLE_RECORD_SCHEMA = SchemaBuilder.record("simpleRecord")
            .fields()
            .name("a")
            .type().intType().noDefault()
            .name("b")
            .type().doubleType().noDefault()
            .name("c")
            .type().stringType().noDefault()
            .endRecord();

    @Test
    public void testAllTypesSimple()
            throws IOException, InterruptedException
    {
        Schema schema = SchemaBuilder.builder()
                .record("all")
                .fields()
                .name("aBoolean")
                .type().booleanType().noDefault()
                .name("aInt")
                .type().intType().noDefault()
                .name("aLong")
                .type().intType().noDefault()
                .name("aFloat")
                .type().floatType().noDefault()
                .name("aDouble")
                .type().doubleType().noDefault()
                .name("aString")
                .type().stringType().noDefault()
                .name("aBytes")
                .type().bytesType().noDefault()
                .name("aFixed")
                .type().fixed("myFixedType").size(16).noDefault()
                .name("anArray")
                .type().array().items().intType().noDefault()
                .name("aMap")
                .type().map().values().intType().noDefault()
                .name("anEnum")
                .type().enumeration("myEnumType").symbols("A", "B", "C").noDefault()
                .name("aRecord")
                .type(SIMPLE_RECORD_SCHEMA)
                .noDefault()
                .endRecord();

        int count = 10000;
        try (SeekableFileInput input = new SeekableFileInput(createWrittenFileWithSchema(count, schema))) {
            Iterator<Page> pageIterator = new AvroFilePageIterator(schema, input);
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithSkips()
            throws IOException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        fieldAssembler.name("skippedField1").type().optional().array().items().intType();
        for (Schema.Field field : SIMPLE_RECORD_SCHEMA.getFields()) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }
        fieldAssembler.name("skippedField2").type().booleanType();
        Schema writeSchema = fieldAssembler.endRecord();

        int count = 10000;
        try (SeekableFileInput input = new SeekableFileInput(createWrittenFileWithSchema(count, writeSchema))) {
            Iterator<Page> pageIterator = new AvroFilePageIterator(SIMPLE_RECORD_SCHEMA, input);
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithDefaults()
            throws IOException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        fieldAssembler.name("defaultedField1").type().map().values().stringType().mapDefault(ImmutableMap.of("key1", "value1"));
        for (Schema.Field field : SIMPLE_RECORD_SCHEMA.getFields()) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }
        fieldAssembler.name("defaultedField2").type().booleanType().booleanDefault(true);
        Schema readerSchema = fieldAssembler.endRecord();

        int count = 10000;
        try (SeekableFileInput input = new SeekableFileInput(createWrittenFileWithSchema(count, SIMPLE_RECORD_SCHEMA))) {
            Iterator<Page> pageIterator = new AvroFilePageIterator(readerSchema, input);
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                MapBlock mb = (MapBlock) p.getBlock(0);
                MapBlock expected = (MapBlock) mapType(VARCHAR, VARCHAR).createBlockFromKeyValue(Optional.empty(),
                        new int[] {0, 1},
                        createStringsBlock("key1"),
                        createStringsBlock("value1"));
                mb = (MapBlock) mb.getRegion(0, 1);
                assertBlockEquals(mapType(VARCHAR, VARCHAR), mb, expected);

                ByteArrayBlock block = (ByteArrayBlock) p.getBlock(readerSchema.getFields().size() - 1);
                assertThat(block.getByte(0, 0)).isGreaterThan((byte) 0);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithReorders()
            throws IOException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        for (Schema.Field field : Lists.reverse(SIMPLE_RECORD_SCHEMA.getFields())) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }
        Schema writerSchema = fieldAssembler.endRecord();

        int count = 10000;
        try (SeekableFileInput input = new SeekableFileInput(createWrittenFileWithSchema(count, writerSchema))) {
            Iterator<Page> pageIterator = new AvroFilePageIterator(SIMPLE_RECORD_SCHEMA, input);
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                assertThat(p.getBlock(0)).isInstanceOf(IntArrayBlock.class);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    private static File createWrittenFileWithSchema(int count, Schema schema)
            throws IOException
    {
        Iterator<Object> randomData = new RandomData(schema, count).iterator();
        File tempFile = File.createTempFile("testingAvroReading", null);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, tempFile);
            while (randomData.hasNext()) {
                fileWriter.append((GenericRecord) randomData.next());
            }
        }
        return tempFile;
    }
}
