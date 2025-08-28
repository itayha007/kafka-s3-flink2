package org.example.service.refresh;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;

import java.io.Serializable;

public class RefreshOperatorFactory extends AbstractStreamOperatorFactory<GenericRecord> implements OneInputStreamOperatorFactory<GenericRecord, GenericRecord>, CoordinatedOperatorFactory<GenericRecord>, Serializable {
    private static final long serialVersionUID = 1L;

    public RefreshOperatorFactory() {
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public <T extends StreamOperator<GenericRecord>> T createStreamOperator(StreamOperatorParameters<GenericRecord> streamOperatorParameters) {
        RefreshOperator refreshOperator = new RefreshOperator();

        refreshOperator.setup(streamOperatorParameters.getContainingTask(), streamOperatorParameters.getStreamConfig(), streamOperatorParameters.getOutput());

        return (T) refreshOperator;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
        return new BroadcastOperatorCoordinator.Provider(operatorID);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return RefreshOperator.class;
    }
}
