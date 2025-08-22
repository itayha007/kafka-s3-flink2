package org.example;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.example.configuration.HdfsConfiguration;
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
        String hdfsUri = HdfsConfiguration.hdfsConfiguration()
                .get("fs.defaultFS", "hdfs://namenode:9000");

        DataStreamSource<String> dataStreamSource =
                this.env.fromSource(this.source, this.watermarkStrategy, "kafka-source");

        StreamingFileSink<String> sink = StreamingFileSink.
                <String>forRowFormat(new Path(hdfsUri+"flink/output"), new SimpleStringEncoder<>("UTF-8"))
                .build();

        dataStreamSource
                .map(msg -> {
                    System.out.println("nice message: " + msg);
                    return msg;
                });

        dataStreamSource.addSink(sink);

        this.env.execute();
    }
}
