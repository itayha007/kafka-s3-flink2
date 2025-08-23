package org.example.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.example.models.Message;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class S3Enricher extends RichAsyncFunction<Message<ArrayNode>, Message<ArrayNode>> {
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private ObjectMapper objectMapper;
    private S3AsyncClient s3AsyncClient;

    @Override
    public void open(Configuration parameters) {
        SdkAsyncHttpClient http = NettyNioAsyncHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(30))
                .readTimeout(Duration.ofMinutes(2))
                .writeTimeout(Duration.ofMinutes(2))
                .maxConcurrency(64)
                .build();
        this.s3AsyncClient = S3AsyncClient.builder()
                .httpClient(http)
                .region(Region.of(this.region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(this.accessKey, this.secretKey)))
                .endpointOverride(URI.create(this.endpoint))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();

        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void asyncInvoke(Message<ArrayNode> message, ResultFuture<Message<ArrayNode>> resultFuture) {
        if (!"s3".equals(message.getHeaders().getSourceType())) {
            resultFuture.complete(Collections.singleton(message));
            return;
        }

        this.asyncQueryFromS3(
                        message.getKafkaReference().getBucket(),
                        message.getKafkaReference().getKeys().get(0))
                .thenApply(payload -> {
                    ArrayNode array = this.toArrayNode(payload);
                    return new Message<>(message.getHeaders(), array, null);
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        resultFuture.complete(Collections.singletonList(message));
                    } else {
                        resultFuture.complete(Collections.singletonList(result));
                    }
                });
    }

    private ArrayNode toArrayNode(byte[] payload) {
        try {
            if (payload == null || payload.length == 0) {
                return this.objectMapper.createArrayNode();
            }
            JsonNode root = this.objectMapper.readTree(payload);
            if (root == null || root.isNull()) {
                return this.objectMapper.createArrayNode();
            }
            if (root.isArray()) {
                return (ArrayNode) root;
            }
            return this.objectMapper.createArrayNode().add(root);
        } catch (IOException e) {
            return this.objectMapper.createArrayNode();
        }
    }


    public CompletableFuture<byte[]> asyncQueryFromS3(String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return this.s3AsyncClient.getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
                .thenApply(BytesWrapper::asByteArray);
    }
}
