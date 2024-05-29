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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class DebugDecimalGenData
{
    private DebugDecimalGenData() {}

    public static void main(String[] args)
            throws IOException
    {
        String schemaFile = System.getenv("SCHEMA_FILE");
        String dataFile = System.getenv("AVRO_FILE");
        Schema schema = new Schema.Parser().parse(new File(schemaFile));

        Iterator<Object> randomData = new RandomData(schema, 100).iterator();

        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>())) {
            fileWriter.create(schema, new File(dataFile));
            while (randomData.hasNext()) {
                GenericRecord genericRecord = (GenericRecord) randomData.next();
                fileWriter.append(genericRecord);
            }
        }
    }
}
