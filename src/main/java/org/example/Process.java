package org.example;

import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.configuration.S3Config;
import org.example.functions.ArrayNodeToGenericRecordMapFunction;
import org.example.service.DataStreamService;
import org.example.service.S3Enricher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.TimeUnit;


@Component
@RequiredArgsConstructor
public class Process implements CommandLineRunner {
    private final StreamExecutionEnvironment environment;
    private final DataStreamService dataStreamService;
    private final S3Config s3Config;

    @Override
    public void run(String... args) throws Exception {
        DataStream<GenericRecord> records = AsyncDataStream.unorderedWait(
                        this.dataStreamService.kafkaDataStream()
                                .filter(Objects::nonNull),
                        new S3Enricher(
                                this.s3Config.getEndpoint(),
                                this.s3Config.getRegion(),
                                this.s3Config.getAccessKey(),
                                this.s3Config.getSecretKey()
                        ),
                        30,
                        TimeUnit.SECONDS,
                        5000
                )
                .map(new ArrayNodeToGenericRecordMapFunction());

        records.sinkTo(FileSink.forBulkFormat(
                new Path("hdfs://hdfs-namenode:8020/flink/output"),
                AvroWriters.forGenericRecord(ArrayNodeToGenericRecordMapFunction.getSchema())
        ).build());

        this.environment.execute();
    }
}
