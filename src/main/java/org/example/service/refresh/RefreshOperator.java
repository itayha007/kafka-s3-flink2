package org.example.service.refresh;

import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorEventDispatcherImpl;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.example.configuration.S3Config;
import org.example.event.RefreshEvent;

import java.lang.reflect.Field;

@Log4j2
public class RefreshOperator extends AbstractStreamOperator<GenericRecord>  implements OneInputStreamOperator<GenericRecord, GenericRecord>, OperatorEventHandler {
    @Override
    public void open() throws Exception {
        super.open();

        StreamTask task = (StreamTask) getContainingTask();

        Field chainField = task.getClass().getDeclaredField("operatorChain");
        chainField.setAccessible(true);
        Object chain = chainField.get(task);

        Field dispatcherField = chain.getClass().getDeclaredField("operatorEventDispatcher");
        dispatcherField.setAccessible(true);
        Object dispatcher = dispatcherField.get(chain);

        if (dispatcher instanceof OperatorEventDispatcherImpl) {
            ((OperatorEventDispatcherImpl)dispatcher).registerEventHandler(getOperatorID(), this);
            log.info("OperatorEventDispatcherImpl registered" + getOperatorID());
        }
        else {
            throw new IllegalStateException("could not access operatorEventDispatcher");
        }

    }

    @Override
    public void handleOperatorEvent(OperatorEvent operatorEvent) {
        if(operatorEvent instanceof RefreshEvent) {
            //refresh
        }
    }

    @Override
    public void processElement(StreamRecord<GenericRecord> streamRecord) throws Exception {
        output.collect(streamRecord);
    }
}
