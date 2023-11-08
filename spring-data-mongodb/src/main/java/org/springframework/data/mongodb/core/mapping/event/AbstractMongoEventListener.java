/*
 * Copyright 2011-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping.event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.ApplicationListener;
import org.springframework.core.GenericTypeResolver;
import org.springframework.data.mongodb.core.query.SerializationUtils;

/**
 * Base class to implement domain class specific {@link ApplicationListener}s.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Martin Baumgartner
 * @author Christoph Strobl
 */
public abstract class AbstractMongoEventListener<E> implements ApplicationListener<MongoMappingEvent<?>> {

	private static final Log LOG = LogFactory.getLog(AbstractMongoEventListener.class);
	private final Class<?> domainClass;

	/**
	 * Creates a new {@link AbstractMongoEventListener}.
	 */
	public AbstractMongoEventListener() {
		Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(this.getClass(), AbstractMongoEventListener.class);
		this.domainClass = typeArgument == null ? Object.class : typeArgument;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onApplicationEvent(MongoMappingEvent<?> event) {

		if (event instanceof AfterLoadEvent<?> afterLoadEvent) {

			if (domainClass.isAssignableFrom(afterLoadEvent.getType())) {
				onAfterLoad((AfterLoadEvent<E>) event);
			}

			return;
		}

		if (event instanceof AbstractDeleteEvent deleteEvent) {

			Class<?> eventDomainType = deleteEvent.getType();

			if (eventDomainType != null && domainClass.isAssignableFrom(eventDomainType)) {
				if (event instanceof BeforeDeleteEvent beforeDeleteEvent) {
					onBeforeDelete(beforeDeleteEvent);
				}
				if (event instanceof AfterDeleteEvent afterDeleteEvent) {
					onAfterDelete(afterDeleteEvent);
				}
			}

			return;

		}

		Object source = event.getSource();

		// Check for matching domain type and invoke callbacks
		if (source != null && !domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeConvertEvent beforeConvertEvent) {
			onBeforeConvert(beforeConvertEvent);
		} else if (event instanceof BeforeSaveEvent beforeSaveEvent) {
			onBeforeSave(beforeSaveEvent);
		} else if (event instanceof AfterSaveEvent afterSaveEvent) {
			onAfterSave(afterSaveEvent);
		} else if (event instanceof AfterConvertEvent afterConvertEvent) {
			onAfterConvert(afterConvertEvent);
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
			LOG.debug(String.format("onBeforeConvert(%s)", SerializationUtils.serializeToJsonSafely(event.getSource())));
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
			LOG.debug(String.format("onBeforeSave(%s, %s)", SerializationUtils.serializeToJsonSafely(event.getSource()), SerializationUtils.serializeToJsonSafely(event.getDocument())));
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
			LOG.debug(String.format("onAfterSave(%s, %s)", SerializationUtils.serializeToJsonSafely(event.getSource()), SerializationUtils.serializeToJsonSafely(event.getDocument())));
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
			LOG.debug(String.format("onAfterLoad(%s)", SerializationUtils.serializeToJsonSafely(event.getDocument())));
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
			LOG.debug(String.format("onAfterConvert(%s, %s)", SerializationUtils.serializeToJsonSafely(event.getDocument()), SerializationUtils.serializeToJsonSafely(event.getSource())));
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
			LOG.debug(String.format("onAfterDelete(%s)", SerializationUtils.serializeToJsonSafely(event.getDocument())));
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
			LOG.debug(String.format("onBeforeDelete(%s)", SerializationUtils.serializeToJsonSafely(event.getDocument())));
		}
	}
}
