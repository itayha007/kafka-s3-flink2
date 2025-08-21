package org.example.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class FlinkConfiguration {
    private final KafkaConfiguration kafkaConfiguration;

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        org.apache.flink.configuration.Configuration cfg = new org.apache.flink.configuration.Configuration();
        // force local executor; helpful when embedding in Spring
        cfg.set(DeploymentOptions.TARGET, "local");
        cfg.setString(RestOptions.BIND_PORT, "8081");
        return StreamExecutionEnvironment.getExecutionEnvironment(cfg);
    }

    @Bean
    public KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(this.kafkaConfiguration.getBootstrapServers())
                .setTopics(this.kafkaConfiguration.getTopic())
                .setGroupId(this.kafkaConfiguration.getGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    @Bean
    public WatermarkStrategy<String> noWatermarks() {
        return WatermarkStrategy.noWatermarks();
    }

}
