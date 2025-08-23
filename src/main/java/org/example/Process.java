package org.example;

import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.OutputFileConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.AvroKryoSerializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.example.configuration.S3Config;
import org.example.functions.ArrayNodeToGenericRecordMapFunction;
import org.example.service.DataStreamService;
import org.example.service.S3Enricher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
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
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null || hadoopHome.isEmpty()) {
            hadoopHome = new File(System.getProperty("java.io.tmpdir"), "hadoop").getAbsolutePath();
        }
        System.setProperty("hadoop.home.dir", hadoopHome);

        File homeDir = new File(hadoopHome);
        File binDir = new File(homeDir, "bin");
        if (!binDir.exists()) {
            binDir.mkdirs();
            new File(binDir, "winutils.exe").createNewFile();
        }

        String hdfsHost = System.getenv().getOrDefault("HDFS_NAMENODE", "localhost");

        // run HDFS operations as root so the sink can create output directories
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("dfs.client.use.datanode.hostname", "true");

        // ensure GenericRecord uses Avro serialization instead of Kryo's default
        this.environment.getConfig()
                .registerTypeWithKryoSerializer(GenericData.Record.class, AvroKryoSerializer.class);

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
                .map(new ArrayNodeToGenericRecordMapFunction())
                .returns(new GenericRecordAvroTypeInfo(ArrayNodeToGenericRecordMapFunction.getSchema()));

        Path outputPath = new Path(String.format("hdfs://%s:8020/flink/output", hdfsHost));

        ParquetWriterFactory<GenericRecord> parquetFactory = ParquetAvroWriters
                .forGenericRecord(
                        ArrayNodeToGenericRecordMapFunction.getSchema(),
                        CompressionCodecName.SNAPPY);

        OutputFileConfig fileConfig = OutputFileConfig.builder()
                .withPartPrefix("data")
                .withPartSuffix(".snappy.parquet")
                .build();

        records.sinkTo(
                FileSink.forBulkFormat(outputPath, parquetFactory)
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withOutputFileConfig(fileConfig)
                        .build()
        );

        this.environment.execute();
    }
}
