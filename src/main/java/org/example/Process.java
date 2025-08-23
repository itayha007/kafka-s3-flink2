package org.example;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.configuration.S3Config;
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
        AsyncDataStream.unorderedWait(
                        this.dataStreamService.kafkaDataStream()
                                .filter(Objects::nonNull),
                        new S3Enricher(
                                this.s3Config.getEndpoint(),
                                this.s3Config.getRegion(),
                                this.s3Config.getAccessKey(),
                                this.s3Config.getSecretKey()
                        ),
                        this.s3Config.getAsyncTimeoutSeconds(),
                        TimeUnit.SECONDS,
                        5000
                )
                .map(msg -> {
                    System.out.println("nice message: " + msg.getPayload().toString());
                    System.out.println("nice headers: " + msg.getHeaders().toString());
                    return msg;
                })
                .returns(TypeInformation.of(new TypeHint<>() {
                }));
        this.environment.execute();
    }
}
