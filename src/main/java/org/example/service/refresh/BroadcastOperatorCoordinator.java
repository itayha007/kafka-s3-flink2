package org.example.service.refresh;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.example.event.RefreshEvent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
@RequiredArgsConstructor
public class BroadcastOperatorCoordinator implements OperatorCoordinator {
    @RequiredArgsConstructor
    public static class Provider implements OperatorCoordinator.Provider {
        private static final long serialVersionUID = 1L;
        private final OperatorID operatorID;

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            log.info("operator id " + operatorID + " created");
            return new BroadcastOperatorCoordinator(context);
        }

        @Override
        public OperatorID getOperatorId() {
            return this.operatorID;
        }
    }

    @Nonnull
    private final Context ctx;

    private final ConcurrentHashMap<Integer, SubtaskGateway> gateways = new ConcurrentHashMap<>();

    @Override
    public void start() throws Exception {
        RefreshService.bridge = this;
    }

    @Override
    public void close() throws Exception {

    }

    public void broadcastRefresh(){
        RefreshEvent refreshEvent = new RefreshEvent();
        for(Map.Entry<Integer, SubtaskGateway> e : gateways.entrySet()){
            try {
                e.getValue().sendEvent(refreshEvent);
            }
            catch (Exception ex) {
                log.error("failed to send event", ex);
            }
        }
    }

    @Override
    public void handleEventFromOperator(int i, OperatorEvent operatorEvent) throws Exception {}

    @Override
    public void checkpointCoordinator(long l, CompletableFuture<byte[]> completableFuture) throws Exception {
        completableFuture.complete(new byte[0]);
    }

    @Override
    public void notifyCheckpointComplete(long l) {

    }

    @Override
    public void resetToCheckpoint(long l, @Nullable byte[] bytes) throws Exception {

    }

    @Override
    public void subtaskFailed(int i, @Nullable Throwable throwable) {

    }

    @Override
    public void subtaskReset(int i, long l) {

    }

    @Override
    public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
        this.gateways.put(i, subtaskGateway);
    }

//    @Override
//    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway){
//
//    }
}
