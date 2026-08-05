package com.salesmanager.core.model.operational;

public interface OperationalEventSink {

    void capture(Object entity, OperationalEventOperation operation);
}
