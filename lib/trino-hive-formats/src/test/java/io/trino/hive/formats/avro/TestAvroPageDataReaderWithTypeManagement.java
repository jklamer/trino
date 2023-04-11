package io.trino.hive.formats.avro;

import com.google.common.primitives.Longs;
import io.trino.spi.Page;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import static io.trino.hive.formats.avro.AvroNativeLogicalTypeManager.DATE_SCHEMA;
import static io.trino.hive.formats.avro.AvroNativeLogicalTypeManager.TIMESTAMP_MICRO_SCHEMA;
import static io.trino.hive.formats.avro.AvroNativeLogicalTypeManager.TIMESTAMP_MILLI_SCHEMA;
import static io.trino.hive.formats.avro.AvroNativeLogicalTypeManager.TIME_MICROS_SCHEMA;
import static io.trino.hive.formats.avro.AvroNativeLogicalTypeManager.TIME_MILLIS_SCHEMA;
import static io.trino.hive.formats.avro.AvroNativeLogicalTypeManager.UUID_SCHEMA;
import static io.trino.hive.formats.avro.TestAvroPageDataReaderWithoutTypeManager.createWrittenFileWithSchema;
import static io.trino.hive.formats.avro.TestLongFromBigEndian.padBigEndianCorrectly;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAvroPageDataReaderWithTypeManagement
{
    private static final Schema DECIMAL_SMALL_BYTES_SCHEMA;
    private static final int SMALL_FIXED_SIZE = (int) ((MAX_SHORT_PRECISION) * Math.log(10) / Math.log(2) / 8) + 1; // TODO math
    private static final int LARGE_FIXED_SIZE = (int) ((MAX_SHORT_PRECISION + 2) * Math.log(10) / Math.log(2) / 8) + 1;
    private static final Schema DECIMAL_SMALL_FIXED_SCHEMA;
    private static final Schema DECIMAL_LARGE_BYTES_SCHEMA;
    private static final Schema DECIMAL_LARGE_FIXED_SCHEMA;
    private static final Date testTime = new Date(780681600000L);
    private static final Type SMALL_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION - 1, 2);
    private static final Type LARGE_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1, 2);

    static {
        LogicalTypes.Decimal small = LogicalTypes.decimal(MAX_SHORT_PRECISION - 1, 2);
        LogicalTypes.Decimal large = LogicalTypes.decimal(MAX_SHORT_PRECISION + 1, 2);
        DECIMAL_SMALL_BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
        small.addToSchema(DECIMAL_SMALL_BYTES_SCHEMA);
        DECIMAL_SMALL_FIXED_SCHEMA = Schema.createFixed("smallDecimal", "myFixed", "namespace", SMALL_FIXED_SIZE);
        small.addToSchema(DECIMAL_SMALL_FIXED_SCHEMA);
        DECIMAL_LARGE_BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
        large.addToSchema(DECIMAL_LARGE_BYTES_SCHEMA);
        DECIMAL_LARGE_FIXED_SCHEMA = Schema.createFixed("largeDecimal", "myFixed", "namespace", (int) ((MAX_SHORT_PRECISION + 2) * Math.log(10) / Math.log(2) / 8) + 1);
        large.addToSchema(DECIMAL_LARGE_FIXED_SCHEMA);
    }

    @Test
    public void testTypesSimple()
            throws IOException
    {
        Schema schema = SchemaBuilder.builder()
                .record("allSupported")
                .fields()
                .name("timestampMillis")
                .type(TIMESTAMP_MILLI_SCHEMA).noDefault()
                .name("timestampMicros")
                .type(TIMESTAMP_MICRO_SCHEMA).noDefault()
                .name("smallBytesDecimal")
                .type(DECIMAL_SMALL_BYTES_SCHEMA).noDefault()
                .name("smallFixedDecimal")
                .type(DECIMAL_SMALL_FIXED_SCHEMA).noDefault()
                .name("largeBytesDecimal")
                .type(DECIMAL_LARGE_BYTES_SCHEMA).noDefault()
                .name("largeFixedDecimal")
                .type(DECIMAL_LARGE_FIXED_SCHEMA).noDefault()
                .name("date")
                .type(DATE_SCHEMA).noDefault()
                .name("timeMillis")
                .type(TIME_MILLIS_SCHEMA).noDefault()
                .name("timeMicros")
                .type(TIME_MICROS_SCHEMA).noDefault()
                .name("id")
                .type(UUID_SCHEMA).noDefault()
                .endRecord();

        GenericData.Fixed genericSmallFixedDecimal = new GenericData.Fixed(DECIMAL_SMALL_FIXED_SCHEMA);
        genericSmallFixedDecimal.bytes(padBigEndianCorrectly(78068160000000L, SMALL_FIXED_SIZE));
        GenericData.Fixed genericLargeFixedDecimal = new GenericData.Fixed(DECIMAL_LARGE_FIXED_SCHEMA);
        genericLargeFixedDecimal.bytes(padBigEndianCorrectly(78068160000000L, LARGE_FIXED_SIZE));
        UUID id = UUID.randomUUID();

        GenericData.Record myRecord = new GenericData.Record(schema);
        myRecord.put("timestampMillis", testTime.getTime());
        myRecord.put("timestampMicros", testTime.getTime() * 1000);
        myRecord.put("smallBytesDecimal", ByteBuffer.wrap(Longs.toByteArray(78068160000000L)));
        myRecord.put("smallFixedDecimal", genericSmallFixedDecimal);
        myRecord.put("largeBytesDecimal", ByteBuffer.wrap(Int128.fromBigEndian(Longs.toByteArray(78068160000000L)).toBigEndianBytes()));
        myRecord.put("largeFixedDecimal", genericLargeFixedDecimal);
        myRecord.put("date", 9035);
        myRecord.put("timeMillis", 39_600_000);
        myRecord.put("timeMicros", 39_600_000_000L);
        myRecord.put("id", id.toString());
        File tempFile = File.createTempFile("testingAvroReading", null);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, tempFile);
            fileWriter.append(myRecord);
        }
        try (SeekableFileInput input = new SeekableFileInput(tempFile)) {
            Iterator<Page> pageIterator = new AvroFilePageIterator(schema, new AvroNativeLogicalTypeManager(), input);
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                // Timestamps equal
                SqlTimestamp milliTimestamp = (SqlTimestamp) TimestampType.TIMESTAMP_MILLIS.getObjectValue(null, p.getBlock(0), 0);
                SqlTimestamp microTimestamp = (SqlTimestamp) TimestampType.TIMESTAMP_MICROS.getObjectValue(null, p.getBlock(1), 0);
                assertThat(milliTimestamp).isEqualTo(microTimestamp.roundTo(3));
                assertThat(microTimestamp.getEpochMicros()).isEqualTo(testTime.getTime() * 1000);

                // Decimals Equal
                SqlDecimal smallBytesDecimal = (SqlDecimal) SMALL_DECIMAL_TYPE.getObjectValue(null, p.getBlock(2), 0);
                SqlDecimal smallFixedDecimal = (SqlDecimal) SMALL_DECIMAL_TYPE.getObjectValue(null, p.getBlock(3), 0);
                SqlDecimal largeBytesDecimal = (SqlDecimal) LARGE_DECIMAL_TYPE.getObjectValue(null, p.getBlock(4), 0);
                SqlDecimal largeFixedDecimal = (SqlDecimal) LARGE_DECIMAL_TYPE.getObjectValue(null, p.getBlock(5), 0);

                assertThat(smallBytesDecimal).isEqualTo(smallFixedDecimal);
                assertThat(largeBytesDecimal).isEqualTo(largeFixedDecimal);
                assertThat(smallBytesDecimal.toBigDecimal()).isEqualTo(largeBytesDecimal.toBigDecimal());

                // Get date
                SqlDate date = (SqlDate) DateType.DATE.getObjectValue(null, p.getBlock(6), 0);
                assertThat(date.getDays()).isEqualTo(9035);

                // Time equals
                SqlTime timeMillis = (SqlTime) TimeType.TIME_MILLIS.getObjectValue(null, p.getBlock(7), 0);
                SqlTime timeMicros = (SqlTime) TimeType.TIME_MICROS.getObjectValue(null, p.getBlock(8), 0);
                assertThat(timeMillis).isEqualTo(timeMicros.roundTo(3));
                assertThat(timeMillis.getPicos()).isEqualTo(timeMicros.getPicos()).isEqualTo(39_600_000_000L * 1_000_000L);

                //UUID
                assertThat(id.toString()).isEqualTo(UuidType.UUID.getObjectValue(null, p.getBlock(9), 0));

                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }
    }

    @Test
    public void testWithDefaults()
            throws IOException
    {
        String id = UUID.randomUUID().toString();
        Schema schema = SchemaBuilder.builder()
                .record("testDefaults")
                .fields()
                .name("timestampMillis")
                .type(TIMESTAMP_MILLI_SCHEMA).withDefault(testTime.getTime())
                .name("smallBytesDecimal")
                .type(DECIMAL_SMALL_BYTES_SCHEMA).withDefault(ByteBuffer.wrap(Longs.toByteArray(testTime.getTime())))
                .name("timeMicros")
                .type(TIME_MICROS_SCHEMA).withDefault(39_600_000_000L)
                .name("id")
                .type(UUID_SCHEMA).withDefault(id)
                .endRecord();
        Schema writeSchema = SchemaBuilder.builder()
                .record("testDefaults")
                .fields()
                .name("notRead").type().optional().booleanType()
                .endRecord();

        File tempFile = createWrittenFileWithSchema(10, writeSchema);
        try (SeekableFileInput input = new SeekableFileInput(tempFile)) {
            Iterator<Page> pageIterator = new AvroFilePageIterator(schema, new AvroNativeLogicalTypeManager(), input);
            int totalRecords = 0;
            while (pageIterator.hasNext()) {
                Page p = pageIterator.next();
                for (int i = 0; i < p.getPositionCount(); i++) {
                    // millis timestamp const
                    SqlTimestamp milliTimestamp = (SqlTimestamp) TimestampType.TIMESTAMP_MILLIS.getObjectValue(null, p.getBlock(0), i);
                    assertThat(milliTimestamp.getEpochMicros()).isEqualTo(testTime.getTime() * 1000);

                    // decimal bytes const
                    SqlDecimal smallBytesDecimal = (SqlDecimal) SMALL_DECIMAL_TYPE.getObjectValue(null, p.getBlock(1), i);
                    assertThat(smallBytesDecimal.getUnscaledValue()).isEqualTo(new BigInteger(Longs.toByteArray(testTime.getTime())));

                    // time micros const
                    SqlTime timeMicros = (SqlTime) TimeType.TIME_MICROS.getObjectValue(null, p.getBlock(2), i);
                    assertThat(timeMicros.getPicos()).isEqualTo(39_600_000_000L * 1_000_000L);

                    //UUID const assert
                    assertThat(id.toString()).isEqualTo(UuidType.UUID.getObjectValue(null, p.getBlock(3), i));
                }
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(10);
        }
    }
}
