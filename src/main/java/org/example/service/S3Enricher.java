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
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.nio.file.Files;
import java.nio.file.Path;

@RequiredArgsConstructor
public class S3Enricher extends RichAsyncFunction<Message<ArrayNode>, Message<ArrayNode>> {
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private ObjectMapper objectMapper;
    private S3Client s3Client;
    private SdkHttpClient httpClient;
    private java.util.concurrent.ExecutorService downloadExecutor;

    @Override
    public void open(Configuration parameters) {
        this.httpClient = UrlConnectionHttpClient.builder().build();
        this.s3Client = S3Client.builder()
                .httpClient(this.httpClient)
                .region(Region.of(this.region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(this.accessKey, this.secretKey)))
                .endpointOverride(URI.create(this.endpoint))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
        this.downloadExecutor = java.util.concurrent.Executors.newCachedThreadPool();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void close() {
        if (this.s3Client != null) {
            this.s3Client.close();
        }
        if (this.httpClient != null) {
            this.httpClient.close();
        }
        if (this.downloadExecutor != null) {
            this.downloadExecutor.shutdown();
        }
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
                .thenApply(path -> {
                    try (InputStream input = Files.newInputStream(path)) {
                        ArrayNode array = this.toArrayNode(input);
                        return new Message<>(message.getHeaders(), array, null);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    }
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        resultFuture.complete(Collections.singletonList(message));
                    } else {
                        resultFuture.complete(Collections.singletonList(result));
                    }
                });
    }

    private ArrayNode toArrayNode(InputStream payload) {
        try {
            if (payload == null) {
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


    public CompletableFuture<Path> asyncQueryFromS3(String bucket, String key) {
        return CompletableFuture.supplyAsync(() -> {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            try {
                Path tempFile = Files.createTempFile("s3-enricher-", ".tmp");
                this.s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(tempFile));
                return tempFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, this.downloadExecutor);
    }
}
