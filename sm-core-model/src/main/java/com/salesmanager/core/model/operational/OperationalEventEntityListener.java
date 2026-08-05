package com.salesmanager.core.model.operational;

import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;

public class OperationalEventEntityListener {

    @PostPersist
    public void onPostPersist(Object entity) {
        OperationalEventBridge.capture(entity, OperationalEventOperation.INSERT);
    }

    @PostUpdate
    public void onPostUpdate(Object entity) {
        OperationalEventBridge.capture(entity, OperationalEventOperation.UPDATE);
    }

    @PostRemove
    public void onPostRemove(Object entity) {
        OperationalEventBridge.capture(entity, OperationalEventOperation.DELETE);
    }
}
