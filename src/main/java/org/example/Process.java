package org.example;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
public class Process implements CommandLineRunner {

    private final StreamExecutionEnvironment env;
    private final KafkaSource<String> source;
    private final WatermarkStrategy<String> watermarkStrategy;

    @Override
    public void run(String... args) throws Exception {
        DataStreamSource<String> dataStreamSource =
                this.env.fromSource(this.source, this.watermarkStrategy, "kafka-source");

        dataStreamSource
                .map(msg -> {
                    System.out.println("nice message: " + msg);
                    return msg;
                });
        this.env.execute();
    }
}
