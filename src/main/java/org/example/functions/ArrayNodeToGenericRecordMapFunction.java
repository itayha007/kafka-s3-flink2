package org.example.functions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.example.models.Message;

/**
 * Simple mapper that wraps the JSON payload as a single field Avro GenericRecord.
 */
public class ArrayNodeToGenericRecordMapFunction implements MapFunction<Message<ArrayNode>, GenericRecord> {

    private static final Schema SCHEMA = SchemaBuilder.record("PayloadRecord")
            .namespace("org.example")
            .fields()
            .name("payload").type().stringType().noDefault()
            .endRecord();

    @Override
    public GenericRecord map(Message<ArrayNode> value) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("payload", value.getPayload().toString());
        return record;
    }

    public static Schema getSchema() {
        return SCHEMA;
    }
}
