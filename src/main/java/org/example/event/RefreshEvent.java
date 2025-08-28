package org.example.event;

import lombok.RequiredArgsConstructor;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

@RequiredArgsConstructor
public class RefreshEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;
}
