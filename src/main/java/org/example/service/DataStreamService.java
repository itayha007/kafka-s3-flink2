package org.example.service;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.models.Message;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DataStreamService {
    private final KafkaSource<Message<ArrayNode>> source;
    private final StreamExecutionEnvironment environment;
    private final WatermarkStrategy<Message<ArrayNode>> watermarkStrategy;

    public DataStream<Message<ArrayNode>> kafkaDataStream() {
        return this.environment.fromSource(this.source, this.watermarkStrategy, "kafka-source");
    }
}
