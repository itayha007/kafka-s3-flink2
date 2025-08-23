package org.example.service;

import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.functions.ArrayNodeToGenericRecordMapFunction;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class HdfsSinkApplier {

    public void apply(DataStream<GenericRecord> input) {
        org.apache.flink.core.fs.Path outputPath = new Path(String.format("hdfs://%s:8020/flink/output", "localhost"));
        input.sinkTo(FileSink.forBulkFormat(
                outputPath,
                AvroWriters.forGenericRecord(ArrayNodeToGenericRecordMapFunction.getSchema())
        ).build());
    }

}
