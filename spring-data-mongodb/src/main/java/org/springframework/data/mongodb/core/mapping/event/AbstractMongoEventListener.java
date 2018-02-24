/*
 * Copyright 2011-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.GenericTypeResolver;

/**
 * Base class to implement domain class specific {@link ApplicationListener}s.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Martin Baumgartner
 * @author Christoph Strobl
 */
public abstract class AbstractMongoEventListener<E> implements ApplicationListener<MongoMappingEvent<?>> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractMongoEventListener.class);
	private final Class<?> domainClass;

	/**
	 * Creates a new {@link AbstractMongoEventListener}.
	 */
	public AbstractMongoEventListener() {
		Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(this.getClass(), AbstractMongoEventListener.class);
		this.domainClass = typeArgument == null ? Object.class : typeArgument;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onApplicationEvent(MongoMappingEvent<?> event) {

		if (event instanceof AfterLoadEvent) {
			AfterLoadEvent<?> afterLoadEvent = (AfterLoadEvent<?>) event;

			if (domainClass.isAssignableFrom(afterLoadEvent.getType())) {
				onAfterLoad((AfterLoadEvent<E>) event);
			}

			return;
		}

		if (event instanceof AbstractDeleteEvent) {

			Class<?> eventDomainType = ((AbstractDeleteEvent) event).getType();

			if (eventDomainType != null && domainClass.isAssignableFrom(eventDomainType)) {
				if (event instanceof BeforeDeleteEvent) {
					onBeforeDelete((BeforeDeleteEvent<E>) event);
				}
				if (event instanceof AfterDeleteEvent) {
					onAfterDelete((AfterDeleteEvent<E>) event);
				}
			}

			return;

		}

		Object source = event.getSource();

		// Check for matching domain type and invoke callbacks
		if (source != null && !domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeConvertEvent) {
			onBeforeConvert((BeforeConvertEvent<E>) event);
		} else if (event instanceof BeforeSaveEvent) {
			onBeforeSave((BeforeSaveEvent<E>) event);
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave((AfterSaveEvent<E>) event);
		} else if (event instanceof AfterConvertEvent) {
			onAfterConvert((AfterConvertEvent<E>) event);
		}
	}

	/**
	 * Captures {@link BeforeConvertEvent}.
	 *
	 * @param event never {@literal null}.
	 * @since 1.8
	 */
	public void onBeforeConvert(BeforeConvertEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeConvert({})", event.getSource());
		}
	}

	/**
	 * Captures {@link BeforeSaveEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 1.8
	 */
	public void onBeforeSave(BeforeSaveEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeSave({}, {})", event.getSource(), event.getDocument());
		}
	}

	/**
	 * Captures {@link AfterSaveEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 1.8
	 */
	public void onAfterSave(AfterSaveEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterSave({}, {})", event.getSource(), event.getDocument());
		}
	}

	/**
	 * Captures {@link AfterLoadEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 1.8
	 */
	public void onAfterLoad(AfterLoadEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterLoad({})", event.getDocument());
		}
	}

	/**
	 * Captures {@link AfterConvertEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 1.8
	 */
	public void onAfterConvert(AfterConvertEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterConvert({}, {})", event.getDocument(), event.getSource());
		}
	}

	/**
	 * Captures {@link AfterDeleteEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 1.8
	 */
	public void onAfterDelete(AfterDeleteEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterDelete({})", event.getDocument());
		}
	}

	/**
	 * Capture {@link BeforeDeleteEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 1.8
	 */
	public void onBeforeDelete(BeforeDeleteEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeDelete({})", event.getDocument());
		}
	}
}
