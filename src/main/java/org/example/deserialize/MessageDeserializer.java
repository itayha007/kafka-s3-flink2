package org.example.deserialize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.Headers;
import org.example.models.KafkaReference;
import org.example.models.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class MessageDeserializer implements KafkaRecordDeserializationSchema<Message<ArrayNode>> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Message<ArrayNode>> out) throws IOException {
        JsonNode node = this.objectMapper.readTree(record.value());
        // Coerce to ArrayNode
        final ArrayNode payload;
        if (node == null || node.isNull()) {
            payload = objectMapper.createArrayNode(); // empty array
        } else if (node.isArray()) {
            payload = (ArrayNode) node;
        } else {
            payload = objectMapper.createArrayNode().add(node); // wrap single object/value
        }

        String sourceType = null;
        if (record.headers() != null && record.headers().lastHeader("source.type") != null) {
            sourceType = new String(record.headers().lastHeader("source.type").value(), StandardCharsets.UTF_8);
        }

        if (sourceType == null || sourceType.equals("kafka")) {
            sourceType = "kafka";
            out.collect(new Message<>(new Headers(sourceType), payload, null));
        } else {
            try {
                out.collect(new Message<>(new Headers(sourceType), payload, KafkaReference.builder()
                        .bucket(node.get("bucket").asText())
                        .keys(Collections.singletonList(node.get("item_id").get(0).asText()))
                        .build()));
            } catch (Exception e) {
                new Message<>(null, null, null);
            }
        }

    }

    @Override
    public TypeInformation<Message<ArrayNode>> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {
        });
    }
}