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
package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAvroPageDataReaderWithoutTypeManager
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final MapType MAP_VARCHAR_VARCHAR = new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS);
    private static final TrinoFileSystem TRINO_LOCAL_FILESYSTEM = new LocalFileSystem(Path.of("/"));

    private static final Schema SIMPLE_RECORD_SCHEMA = SchemaBuilder.record("simpleRecord")
            .fields()
            .name("a")
            .type().intType().noDefault()
            .name("b")
            .type().doubleType().noDefault()
            .name("c")
            .type().stringType().noDefault()
            .endRecord();

    private static final Schema SIMPLE_ENUM_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C");

    private static final Schema SIMPLE_ENUM_SUPER_SCHEMA = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C", "D");

    private static final Schema SIMPLE_ENUM_REORDERED = SchemaBuilder.enumeration("myEnumType").symbols("C", "D", "B", "A");

    private static final Schema ALL_TYPE_RECORD_SCHEMA = SchemaBuilder.builder()
            .record("all")
            .fields()
            .name("aBoolean")
            .type().booleanType().noDefault()
            .name("aInt")
            .type().intType().noDefault()
            .name("aLong")
            .type().longType().noDefault()
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
            .type(SIMPLE_ENUM_SCHEMA).noDefault()
            .name("aRecord")
            .type(SIMPLE_RECORD_SCHEMA)
            .noDefault()
            .endRecord();

    @Test
    public void testAllTypesSimple()
            throws IOException, AvroTypeException
    {
        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, ALL_TYPE_RECORD_SCHEMA);
        try (AvroFileReader pageIterator = new AvroFileReader(input, ALL_TYPE_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
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
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        fieldAssembler.name("notInAllTypeRecordSchema").type().optional().array().items().intType();
        Schema readSchema = fieldAssembler.endRecord();

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, ALL_TYPE_RECORD_SCHEMA);
        try (AvroFileReader pageIterator = new AvroFileReader(input, readSchema, NoOpAvroTypeManager.INSTANCE)) {
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
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        fieldAssembler.name("defaultedField1").type().map().values().stringType().mapDefault(ImmutableMap.of("key1", "value1"));
        for (Schema.Field field : SIMPLE_RECORD_SCHEMA.getFields()) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }
        fieldAssembler.name("defaultedField2").type().booleanType().booleanDefault(true);
        Schema readerSchema = fieldAssembler.endRecord();

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, SIMPLE_RECORD_SCHEMA);
        try (AvroFileReader pageIterator = new AvroFileReader(input, readerSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                MapBlock mb = (MapBlock) p.getBlock(0);
                MapBlock expected = (MapBlock) MAP_VARCHAR_VARCHAR.createBlockFromKeyValue(Optional.empty(),
                        new int[] {0, 1},
                        createStringsBlock("key1"),
                        createStringsBlock("value1"));
                mb = (MapBlock) mb.getRegion(0, 1);
                assertBlockEquals(MAP_VARCHAR_VARCHAR, mb, expected);

                ByteArrayBlock block = (ByteArrayBlock) p.getBlock(readerSchema.getFields().size() - 1);
                assertThat(block.getByte(0, 0)).isGreaterThan((byte) 0);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithReorders()
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("simpleRecord").fields();
        for (Schema.Field field : Lists.reverse(SIMPLE_RECORD_SCHEMA.getFields())) {
            if (field.schema().getType().equals(Schema.Type.ENUM)) {
                fieldAssembler = fieldAssembler.name(field.name()).type(SIMPLE_ENUM_REORDERED).noDefault();
            }
            else {
                fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
            }
        }
        Schema writerSchema = fieldAssembler.endRecord();

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, writerSchema);
        try (AvroFileReader pageIterator = new AvroFileReader(input, SIMPLE_RECORD_SCHEMA, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                assertThat(p.getBlock(0)).isInstanceOf(IntArrayBlock.class);
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testSchemaWithNull()
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler1 = SchemaBuilder.builder().record("myRecord").fields();
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler2 = SchemaBuilder.builder().record("myRecord").fields();
        for (Schema.Field field : ALL_TYPE_RECORD_SCHEMA.getFields()) {
            fieldAssembler1 = fieldAssembler1.name(field.name()).type(Schema.createUnion(Schema.create(Schema.Type.NULL), field.schema())).noDefault();
            fieldAssembler2 = fieldAssembler2.name(field.name()).type(Schema.createUnion(field.schema(), Schema.create(Schema.Type.NULL))).noDefault();
        }
        Schema nullableSchema = fieldAssembler1.endRecord();
        Schema nullableSchemaDifferentOrder = fieldAssembler2.endRecord();

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, nullableSchema);
        try (AvroFileReader pageIterator = new AvroFileReader(input, nullableSchemaDifferentOrder, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testPromotions()
            throws IOException, AvroTypeException
    {
        SchemaBuilder.FieldAssembler<Schema> writeSchemaBuilder = SchemaBuilder.builder().record("writeRecord").fields();
        SchemaBuilder.FieldAssembler<Schema> readSchemaBuilder = SchemaBuilder.builder().record("readRecord").fields();

        AtomicInteger fieldNum = new AtomicInteger(0);
        for (Schema.Type readType : Schema.Type.values()) {
            List<Schema.Type> promotesFrom = switch (readType) {
                case STRING -> ImmutableList.of(Schema.Type.BYTES);
                case BYTES -> ImmutableList.of(Schema.Type.STRING);
                case LONG -> ImmutableList.of(Schema.Type.INT);
                case FLOAT -> ImmutableList.of(Schema.Type.INT, Schema.Type.LONG);
                case DOUBLE -> ImmutableList.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT);
                case RECORD, ENUM, ARRAY, MAP, UNION, FIXED, INT, BOOLEAN, NULL -> ImmutableList.of();
            };
            for (Schema.Type writeType : promotesFrom) {
                String fieldName = "field" + fieldNum.getAndIncrement();
                writeSchemaBuilder = writeSchemaBuilder.name(fieldName).type(Schema.create(writeType)).noDefault();
                readSchemaBuilder = readSchemaBuilder.name(fieldName).type(Schema.create(readType)).noDefault();
            }
        }

        int count = ThreadLocalRandom.current().nextInt(10000, 100000);
        TrinoInputFile input = createWrittenFileWithSchema(count, writeSchemaBuilder.endRecord());
        try (AvroFileReader pageIterator = new AvroFileReader(input, readSchemaBuilder.endRecord(), NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(count);
        }
    }

    @Test
    public void testEnum()
            throws IOException, AvroTypeException
    {
        Schema base = SchemaBuilder.record("test").fields()
                .name("myEnum")
                .type(SIMPLE_ENUM_SCHEMA).noDefault()
                .endRecord();
        Schema superSchema = SchemaBuilder.record("test").fields()
                .name("myEnum")
                .type(SIMPLE_ENUM_SUPER_SCHEMA).noDefault()
                .endRecord();
        Schema reorderdSchema = SchemaBuilder.record("test").fields()
                .name("myEnum")
                .type(SIMPLE_ENUM_REORDERED).noDefault()
                .endRecord();

        GenericRecord expected = (GenericRecord) new RandomData(base, 1).iterator().next();

        //test superset
        TrinoInputFile input = createWrittenFileWithData(base, ImmutableList.of(expected));
        try (AvroFileReader pageIterator = new AvroFileReader(input, superSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                String actualSymbol = new String(((Slice) VarcharType.VARCHAR.getObject(p.getBlock(0), 0)).getBytes(), StandardCharsets.UTF_8);
                assertThat(actualSymbol).isEqualTo(expected.get("myEnum").toString());
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }

        //test reordered
        input = createWrittenFileWithData(base, ImmutableList.of(expected));
        try (AvroFileReader pageIterator = new AvroFileReader(input, reorderdSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                String actualSymbol = new String(((Slice) VarcharType.VARCHAR.getObject(p.getBlock(0), 0)).getBytes(), StandardCharsets.UTF_8);
                assertThat(actualSymbol).isEqualTo(expected.get("myEnum").toString());
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }
    }

    @Test
    public void testCoercionOfUnionToStruct()
            throws IOException, AvroTypeException
    {
        Schema complexUnion = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL));

        Schema readSchema = SchemaBuilder.builder()
                .record("testComplexUnions")
                .fields()
                .name("readStraightUp")
                .type(complexUnion)
                .noDefault()
                .name("readFromReverse")
                .type(complexUnion)
                .noDefault()
                .name("readFromDefault")
                .type(complexUnion)
                .withDefault(42)
                .endRecord();

        Schema writeSchema = SchemaBuilder.builder()
                .record("testComplexUnions")
                .fields()
                .name("readStraightUp")
                .type(complexUnion)
                .noDefault()
                .name("readFromReverse")
                .type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)))
                .noDefault()
                .endRecord();

        GenericRecord stringsOnly = new GenericData.Record(writeSchema);
        stringsOnly.put("readStraightUp", "I am in column 0 field 1");
        stringsOnly.put("readFromReverse", "I am in column 1 field 1");

        GenericRecord ints = new GenericData.Record(writeSchema);
        ints.put("readStraightUp", 5);
        ints.put("readFromReverse", 21);

        GenericRecord nulls = new GenericData.Record(writeSchema);
        nulls.put("readStraightUp", null);
        nulls.put("readFromReverse", null);

        TrinoInputFile input = createWrittenFileWithData(writeSchema, ImmutableList.of(stringsOnly, ints, nulls));
        try (AvroFileReader pageIterator = new AvroFileReader(input, readSchema, NoOpAvroTypeManager.INSTANCE)) {
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                assertThat(p.getPositionCount()).withFailMessage("Page Batch should be at least 3").isEqualTo(3);
                //check first column
                //check first column first row coerced struct
                Block readStraightUpStringsOnly = p.getBlock(0).getSingleValueBlock(0);
                assertThat(readStraightUpStringsOnly.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readStraightUpStringsOnly.getChildren().get(1).isNull(0)).isTrue(); // int field null
                assertThat(VARCHAR.getObjectValue(null, readStraightUpStringsOnly.getChildren().get(2), 0)).isEqualTo("I am in column 0 field 1"); //string field expected value
                // check first column second row coerced struct
                Block readStraightUpInts = p.getBlock(0).getSingleValueBlock(1);
                assertThat(readStraightUpInts.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readStraightUpInts.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(IntegerType.INTEGER.getObjectValue(null, readStraightUpInts.getChildren().get(1), 0)).isEqualTo(5);

                //check first column third row is null
                assertThat(p.getBlock(0).isNull(2)).isTrue();
                //check second column
                //check second column first row coerced struct
                Block readFromReverseStringsOnly = p.getBlock(1).getSingleValueBlock(0);
                assertThat(readFromReverseStringsOnly.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromReverseStringsOnly.getChildren().get(1).isNull(0)).isTrue(); // int field null
                assertThat(VARCHAR.getObjectValue(null, readFromReverseStringsOnly.getChildren().get(2), 0)).isEqualTo("I am in column 1 field 1");
                //check second column second row coerced struct
                Block readFromReverseUpInts = p.getBlock(1).getSingleValueBlock(1);
                assertThat(readFromReverseUpInts.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromReverseUpInts.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(IntegerType.INTEGER.getObjectValue(null, readFromReverseUpInts.getChildren().get(1), 0)).isEqualTo(21);
                //check second column third row is null
                assertThat(p.getBlock(1).isNull(2)).isTrue();

                //check third column (default of 42 always)
                //check third column first row coerced struct
                Block readFromDefaultStringsOnly = p.getBlock(2).getSingleValueBlock(0);
                assertThat(readFromDefaultStringsOnly.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromDefaultStringsOnly.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(IntegerType.INTEGER.getObjectValue(null, readFromDefaultStringsOnly.getChildren().get(1), 0)).isEqualTo(42);
                //check third column second row coerced struct
                Block readFromDefaultInts = p.getBlock(2).getSingleValueBlock(1);
                assertThat(readFromDefaultInts.getChildren().size()).isEqualTo(3); // tag, int and string block fields
                assertThat(readFromDefaultInts.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(IntegerType.INTEGER.getObjectValue(null, readFromDefaultInts.getChildren().get(1), 0)).isEqualTo(42);
                //check third column third row coerced struct
                Block readFromDefaultNulls = p.getBlock(2).getSingleValueBlock(2);
                assertThat(readFromDefaultNulls.getChildren().size()).isEqualTo(3); // int and string block fields
                assertThat(readFromDefaultNulls.getChildren().get(2).isNull(0)).isTrue(); // string field null
                assertThat(IntegerType.INTEGER.getObjectValue(null, readFromDefaultNulls.getChildren().get(1), 0)).isEqualTo(42);

                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(3);
        }
    }

    protected static TrinoInputFile createWrittenFileWithData(Schema schema, List<GenericRecord> records)
            throws IOException
    {
        File tempFile = File.createTempFile("testingAvroReading", null);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, tempFile);
            for (GenericRecord genericRecord : records) {
                fileWriter.append(genericRecord);
            }
        }
        tempFile.deleteOnExit();
        return TRINO_LOCAL_FILESYSTEM.newInputFile("local://" + tempFile.getAbsolutePath());
    }

    protected static TrinoInputFile createWrittenFileWithSchema(int count, Schema schema)
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
        tempFile.deleteOnExit();
        return TRINO_LOCAL_FILESYSTEM.newInputFile("local://" + tempFile.getAbsolutePath());
    }
}
