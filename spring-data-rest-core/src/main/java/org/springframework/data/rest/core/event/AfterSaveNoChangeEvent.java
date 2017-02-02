package org.springframework.data.rest.core.event;

/**
 * Emitted after a save to the repository.
 * 
 * @author Jon Brisbin
 */
public class AfterSaveNoChangeEvent extends RepositoryEvent {

	private static final long serialVersionUID = 8568843338617401903L;

	public AfterSaveNoChangeEvent(Object source) {
		super(source);
	}
}
