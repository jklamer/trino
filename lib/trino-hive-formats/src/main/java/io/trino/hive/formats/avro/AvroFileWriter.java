package io.trino.hive.formats.avro;

import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AvroFileWriter implements Closeable
{
    private final DataFileWriter<GenericRecord> fileWriter;

    public AvroFileWriter(
            Schema schema,
            OutputStream rawOutput,
            Optional<CompressionKind> compressionKind,
            Map<String, String> metadata) throws IOException
    {
        this.fileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(requireNonNull(schema, "schema is null")))
                .create(schema, rawOutput);
        for (Map.Entry<String, String> entry : requireNonNull(metadata, "metadata is null").entrySet()) {
            this.fileWriter.setMeta(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try(fileWriter){}
    }
}
