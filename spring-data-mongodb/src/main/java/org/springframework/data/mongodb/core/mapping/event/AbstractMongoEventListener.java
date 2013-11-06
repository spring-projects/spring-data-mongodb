/*
 * Copyright 2011-2013 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.mongodb.DBObject;

/**
 * Base class to implement domain class specific {@link ApplicationListener}s.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Martin Baumgartner
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
	@SuppressWarnings("rawtypes")
	public void onApplicationEvent(MongoMappingEvent<?> event) {

		if (event instanceof AfterLoadEvent) {
			AfterLoadEvent<?> afterLoadEvent = (AfterLoadEvent<?>) event;

			if (domainClass.isAssignableFrom(afterLoadEvent.getType())) {
				onAfterLoad(event.getDBObject());
			}

			return;
		}

		if (event instanceof AbstractDeleteEvent) {

			Class<?> eventDomainType = ((AbstractDeleteEvent) event).getType();

			if (eventDomainType != null && domainClass.isAssignableFrom(eventDomainType)) {
				if (event instanceof BeforeDeleteEvent) {
					onBeforeDelete(event.getDBObject());
				}
				if (event instanceof AfterDeleteEvent) {
					onAfterDelete(event.getDBObject());
				}
			}

			return;
		}

		@SuppressWarnings("unchecked")
		E source = (E) event.getSource();

		// Check for matching domain type and invoke callbacks
		if (source != null && !domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeConvertEvent) {
			onBeforeConvert(source);
		} else if (event instanceof BeforeSaveEvent) {
			onBeforeSave(source, event.getDBObject());
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave(source, event.getDBObject());
		} else if (event instanceof AfterConvertEvent) {
			onAfterConvert(event.getDBObject(), source);
		}
	}

	public void onBeforeConvert(E source) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeConvert({})", source);
		}
	}

	public void onBeforeSave(E source, DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeSave({}, {})", source, dbo);
		}
	}

	public void onAfterSave(E source, DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterSave({}, {})", source, dbo);
		}
	}

	public void onAfterLoad(DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterLoad({})", dbo);
		}
	}

	public void onAfterConvert(DBObject dbo, E source) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterConvert({}, {})", dbo, source);
		}
	}

	public void onAfterDelete(DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterDelete({})", dbo);
		}
	}

	public void onBeforeDelete(DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeDelete({})", dbo);
		}
	}
}
