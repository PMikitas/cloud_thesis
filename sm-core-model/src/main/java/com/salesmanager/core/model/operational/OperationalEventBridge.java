package com.salesmanager.core.model.operational;

import java.util.concurrent.atomic.AtomicReference;

public final class OperationalEventBridge {

    private static final AtomicReference<OperationalEventSink> SINK = new AtomicReference<>();

    private OperationalEventBridge() {
    }

    public static void register(OperationalEventSink sink) {
        SINK.set(sink);
    }

    public static void unregister(OperationalEventSink sink) {
        SINK.compareAndSet(sink, null);
    }

    public static void capture(Object entity, OperationalEventOperation operation) {
        OperationalEventSink sink = SINK.get();
        if (sink != null) {
            sink.capture(entity, operation);
        }
    }
}
