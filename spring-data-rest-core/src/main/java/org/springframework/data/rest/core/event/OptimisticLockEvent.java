package org.springframework.data.rest.core.event;

/**
 * User: patryk
 * Date: 2016-11-04
 * Time: 9:14 AM
 */
public class OptimisticLockEvent extends RepositoryEvent {

    private static final long serialVersionUID = 4056971502090201918L;

    public OptimisticLockEvent(Object source) {
        super(source);
    }
}
