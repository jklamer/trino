package io.trino.plugin.hive.avro;

public final class HiveAvroConstants
{
    // Job configuration keys
    public static final String OUTPUT_CODEC_JOB_CONF = "avro.output.codec";
    public static final String AVRO_SERDE_SCHEMA = "avro.serde.schema";

    //hive table properties
    public static final String SCHEMA_LITERAL = "avro.schema.literal";
    public static final String SCHEMA_URL = "avro.schema.url";
    public static final String SCHEMA_NONE = "none";
    public static final String SCHEMA_NAMESPACE = "avro.schema.namespace";
    public static final String SCHEMA_NAME = "avro.schema.name";
    public static final String SCHEMA_DOC = "avro.schema.doc";
    public static final String TABLE_NAME = "name";

    // Container FileConstants
    public static final String SCHEMA = "avro.schema";
    public static final String CODEC = "avro.codec";
    public static final String NULL_CODEC = "null";
    public static final String DEFLATE_CODEC = "deflate";
    public static final String SNAPPY_CODEC = "snappy";
    public static final String BZIP2_CODEC = "bzip2";
    public static final String XZ_CODEC = "xz";
    public static final String ZSTANDARD_CODEC = "zstandard";

    // Hive Logical types
    public static final String CHAR_TYPE_NAME = "char";
    public static final String VARCHAR_TYPE_NAME = "varchar";
    public static final String DATE_TYPE_NAME = "date";
    public static final String TIMESTAMP_TYPE_NAME = "timestamp-millis";
}
