package org.springframework.data.rest.core.event;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.data.annotation.Version;
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
            processEvent(event);
        }
    }

    private void processEvent(RepositoryEvent event) {
        if (em == null || !TransactionSynchronizationManager.isActualTransactionActive()) {
            publisher.publishEvent(new AfterSaveEvent(event.getSource()));
        } else {
            Object object = event.getSource();
            Class clazz = object.getClass();
            Long oldVersion = null;
            Long newVersion;
            if (clazz.isAnnotationPresent(Version.class)) {
                oldVersion = findVersion(object, clazz);
            }

            em.flush();

            if (clazz.isAnnotationPresent(Version.class)) {
                newVersion = findVersion(object,clazz);

                //check if the version changed
                if((oldVersion==null && newVersion != null) ||
                        (oldVersion!=null && !oldVersion.equals(newVersion))){
                    publisher.publishEvent(new AfterSaveEvent(event.getSource()));
                }
                else {
                    //publish event after the save signaling that the main entity was not updated
                    publisher.publishEvent(new AfterSaveNoChangeEvent(event.getSource()));
                }

            } else {
                publisher.publishEvent(new AfterSaveEvent(event.getSource()));
            }
        }
    }

    private Long findVersion(Object object, Class clazz) {
        Field[] field = FieldUtils.getFieldsWithAnnotation(clazz, Version.class);
        Field versionField = field[0];
        try {
            return (Long) FieldUtils.readField(versionField, object, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Version field not accessible");
        }
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.publisher = applicationEventPublisher;
    }
}
