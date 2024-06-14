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

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.trino.spi.type.Int128;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class DebugDecimalTypes
{
    private DebugDecimalTypes() {}

    public static void main(String[] args)
            throws IOException
    {
        new DebugDecimalTypes().testDecimalTypesInFile();
    }

    @Test
    public void testDecimalTypesInFile()
            throws IOException
    {
        String dataFile = requireNonNull(System.getenv("AVRO_FILE"), "Define avro data file path in env variable AVRO_FILE");
        String schemaFile = requireNonNull(System.getenv("SCHEMA_FILE"), "Define avro schema file path in env variable SCHEMA_FILE");
        Schema schema = new Schema.Parser().parse(new File(schemaFile));

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(dataFile), new GenericDatumReader<>(schema));
        try {
            Schema fileSchema = dataFileReader.getSchema();

            List<Schema.Field> readDecimalFields = getDecimalFields(schema);
            List<Schema.Field> actualDecimalFields = getDecimalFields(fileSchema);

            if (!schema.equals(fileSchema)) {
                System.out.println("Schema used to read file is not same as schema read from file");
                System.out.println("Provided Schema");
                System.out.println(schema.toString(true));
                System.out.println("File Schema");
                System.out.println(fileSchema.toString(true));

                if (!readDecimalFields.equals(actualDecimalFields)) {
                    Set<String> readDecimalsOnly = Sets.difference(readDecimalFields.stream().map(Schema.Field::name).collect(Collectors.toSet()), actualDecimalFields.stream().map(Schema.Field::name).collect(Collectors.toSet()));
                    if (!readDecimalsOnly.isEmpty()) {
                        System.out.println("Fields not marked as decimals when written are being read as decimals");
                        System.out.println(Arrays.toString(readDecimalsOnly.toArray()));
                    }
                    Set<String> writeDecimals = Sets.difference(actualDecimalFields.stream().map(Schema.Field::name).collect(Collectors.toSet()), readDecimalFields.stream().map(Schema.Field::name).collect(Collectors.toSet()));
                    if (!writeDecimals.isEmpty()) {
                        System.out.println("Fields marked as decimals when written are being read not as decimals");
                        System.out.println(Arrays.toString(writeDecimals.toArray()));
                    }
                }
            }

            verify(!readDecimalFields.isEmpty());
            GenericRecord record;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next();
                for (Schema.Field field : readDecimalFields) {
                    Object decimal = record.get(field.name());
                    if (decimal != null) {
                        Function<Object, byte[]> byteExtract = byteExtract(field.schema());
                        byte[] decimalBigEndianBytes = byteExtract.apply(decimal);
                        try {
                            Int128.fromBigEndian(decimalBigEndianBytes);
                        }
                        catch (Throwable t) {
                            System.out.println("Error parsing decimal bytes as decimal for field " + field.name());
                            System.out.println("Decimal schema " + field.schema());
                            System.out.println(Throwables.getStackTraceAsString(t));
                            System.out.println("Decimal Bytes of size " + decimalBigEndianBytes.length);
                            System.out.println(Arrays.toString(decimalBigEndianBytes));
                        }
                    }
                }
            }
        }
        finally {
            dataFileReader.close();
        }
    }

    public static List<Schema.Field> getDecimalFields(Schema schema)
    {
        verify(schema.getType() == Schema.Type.RECORD);
        return schema.getFields().stream()
                .filter(field -> {
                    Schema fieldSchema = isSimpleNullableUnion(field.schema()) ?
                            unwrapNullableUnion(field.schema()) : field.schema();
                    if (fieldSchema.getType() != Schema.Type.BYTES && fieldSchema.getType() != Schema.Type.FIXED) {
                        return false;
                    }
                    String logicalType = fieldSchema.getProp(LogicalType.LOGICAL_TYPE_PROP);
                    return logicalType != null && logicalType.equals("decimal");
                }).collect(Collectors.toList());
    }

    public static boolean isSimpleNullableUnion(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        return schema.getTypes().stream().filter(not(Schema::isNullable)).count() == 1L;
    }

    static Schema unwrapNullableUnion(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        verify(schema.isNullable() && schema.getTypes().size() == 2);
        return schema.getTypes().stream().filter(not(Schema::isNullable)).collect(onlyElement());
    }

    public static Function<Object, byte[]> byteExtract(Schema schema)
    {
        Schema unwrapped = isSimpleNullableUnion(schema) ?
                unwrapNullableUnion(schema) : schema;
        return switch (unwrapped.getType()) {
            case BYTES -> // This is only safe because we don't reuse byte buffer objects which means each gets sized exactly for the bytes contained
                    (obj) -> ((ByteBuffer) obj).array();
            case FIXED -> (obj) -> ((GenericFixed) obj).bytes();
            default -> throw new IllegalStateException("Unreachable unfiltered logical type " + unwrapped.getType());
        };
    }
}
