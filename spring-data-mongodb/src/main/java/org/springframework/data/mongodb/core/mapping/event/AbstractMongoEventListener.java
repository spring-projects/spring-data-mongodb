/*
 * Copyright 2011 by the original author(s).
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

import com.mongodb.DBObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.GenericTypeResolver;

/**
 * Base class to implement domain class specific {@link ApplicationListener}s.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 */
public abstract class AbstractMongoEventListener<E> implements ApplicationListener<MongoMappingEvent<?>> {

	protected final Log LOG = LogFactory.getLog(getClass());
	private final Class<?> domainClass;
	
	/**
	 * Creates a new {@link AbstractMongoEventListener}.
	 */
	public AbstractMongoEventListener() {
		this.domainClass = GenericTypeResolver.resolveTypeArgument(this.getClass(), AbstractMongoEventListener.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@SuppressWarnings("unchecked")
	public void onApplicationEvent(MongoMappingEvent<?> event) {
		
		Object source = event.getSource();
		
		// Invoke domain type independent events
		if (event instanceof AfterLoadEvent) {
			onAfterLoad(((AfterLoadEvent) event).getSource());
		}
		
		// Check for matching domain type and invoke callbacks
		if (source != null && !domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeConvertEvent) {
			onBeforeConvert((E) source);
		} else if (event instanceof BeforeSaveEvent) {
			onBeforeSave((E) source, event.getDBObject());
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave((E) source, event.getDBObject());
		} else if (event instanceof AfterConvertEvent) {
			onAfterConvert(event.getDBObject(), (E) source);
		}
	}

	public void onBeforeConvert(E source) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeConvert(" + source + ")");
		}
	}

	public void onBeforeSave(E source, DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onBeforeSave(" + source + ", " + dbo + ")");
		}
	}

	public void onAfterSave(E source, DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterSave(" + source + ", " + dbo + ")");
		}
	}

	public void onAfterLoad(DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterLoad(" + dbo + ")");
		}
	}

	public void onAfterConvert(DBObject dbo, E source) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterConvert(" + dbo + "," + source + ")");
		}
	}
}
