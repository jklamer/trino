package io.trino.hive.formats.avro;

import org.apache.avro.file.CodecFactory;

import java.util.NoSuchElementException;

/**
 * Inner join between Trino plugin supported codec types and Avro spec supported codec types
 * Spec list: <a href="https://avro.apache.org/docs/1.11.1/specification/#required-codecs">required</a> and
 * <a href="https://avro.apache.org/docs/1.11.1/specification/#optional-codecs">optionals</a>
 */
public enum AvroCompressionKind
{
    NULL("null"),
    DEFLATE("deflate"),
    SNAPPY("snappy"),
    ZSTANDARD("zstandard");

    public static AvroCompressionKind fromCodecString(String codecString)
    {
        for (AvroCompressionKind kind : AvroCompressionKind.values()) {
            if (kind.codecString.equals(codecString)) {
                return kind;
            }
        }
        throw new NoSuchElementException("No Avro Compression Kind with a codec string " + codecString);
    }

    private final String codecString;

    AvroCompressionKind(String codecString)
    {
        this.codecString = codecString;
    }

    @Override
    public String toString()
    {
        return codecString;
    }

    public CodecFactory getCodecFactory()
    {
        return CodecFactory.fromString(codecString);
    }
}
