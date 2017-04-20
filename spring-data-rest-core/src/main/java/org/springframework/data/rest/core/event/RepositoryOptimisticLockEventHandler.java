package org.springframework.data.rest.core.event;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.lang.reflect.Field;

/**
 * User: patryk
 * Date: 2016-11-04
 * Time: 10:34 AM
 */
@Component
public class RepositoryOptimisticLockEventHandler implements ApplicationListener<RepositoryEvent>, ApplicationEventPublisherAware {

    @PersistenceContext
    private EntityManager em;
    private ApplicationEventPublisher publisher;

    @Override
    public void onApplicationEvent(RepositoryEvent event) {
        Class<? extends RepositoryEvent> eventType = event.getClass();

        if (eventType.equals(OptimisticLockEvent.class)) {
            try {
                //Only entities that use internal versioning will be used in CRUD
                if (Class.forName("com.translucentcomputing.tekstack.core.commons.domain.search.audit.AbstractAuditingInternalEntity").isAssignableFrom(event.getSource().getClass())) {
                    processEventES(event);
                } else {
                    processEventJPA(event);
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("TEKStack ES Audit classes not found");
            }
        }
    }

    private void processEventES(RepositoryEvent event) {
        publisher.publishEvent(new AfterSaveEvent(event.getSource()));
    }

    private void processEventJPA(RepositoryEvent event) {
        if (em == null || !TransactionSynchronizationManager.isActualTransactionActive()) {
            publisher.publishEvent(new AfterSaveEvent(event.getSource()));
        } else {
            Object object = event.getSource();
            Class clazz = object.getClass();
            Long oldVersion = null;
            Long newVersion;
            if (findVersionField(clazz) != null) {
                oldVersion = findVersion(object, clazz);
            }

            em.flush();

            if (findVersionField(clazz) != null) {
                newVersion = findVersion(object, clazz);

                //check if the version changed
                if ((oldVersion == null && newVersion != null) ||
                        (oldVersion != null && !oldVersion.equals(newVersion))) {
                    publisher.publishEvent(new AfterSaveEvent(event.getSource()));
                } else {
                    //publish event after the save signaling that the main entity was not updated
                    publisher.publishEvent(new AfterSaveNoChangeEvent(event.getSource()));
                }

            } else {
                publisher.publishEvent(new AfterSaveEvent(event.getSource()));
            }
        }
    }

    private Long findVersion(Object object, Class clazz) {
        Field versionField = findVersionField(clazz);
        try {
            return (Long) FieldUtils.readField(versionField, object, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Version field not accessible");
        }
    }

    private Field findVersionField(Class clazz) {
        Field[] fields = FieldUtils.getFieldsWithAnnotation(clazz, org.springframework.data.annotation.Version.class);
        if (fields == null || fields.length == 0) {
            fields = FieldUtils.getFieldsWithAnnotation(clazz, javax.persistence.Version.class);
        }
        return fields == null || fields.length == 0 ? null : fields[0];
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.publisher = applicationEventPublisher;
    }
}
