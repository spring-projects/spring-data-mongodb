/*
 * Copyright (c) 2011 by the original author(s).
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

package org.springframework.data.mongodb.mapping.event;

import com.mongodb.DBObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public abstract class AbstractMappingEventListener<T extends ApplicationEvent, E> implements ApplicationListener<T> {

	protected final Log log = LogFactory.getLog(getClass());

	@SuppressWarnings({ "unchecked" })
	public void onApplicationEvent(T appEvent) {
		if (appEvent instanceof MongoMappingEvent) {
			try {
				MongoMappingEvent<E> event = (MongoMappingEvent<E>) appEvent;
				if (event instanceof BeforeConvertEvent) {
					onBeforeConvert(event.getSource());
				} else if (event instanceof BeforeSaveEvent) {
					onBeforeSave(event.getSource(), event.getDBObject());
				} else if (event instanceof AfterSaveEvent) {
					onAfterSave(event.getSource(), event.getDBObject());
				} else if (event instanceof AfterLoadEvent) {
					onAfterLoad((DBObject) event.getSource());
				} else if (event instanceof AfterConvertEvent) {
					onAfterConvert(event.getDBObject(), event.getSource());
				}
			} catch (ClassCastException e) {
				// Not a mapping event for this entity, apparently.
				// Just ignore it for now.
			}
		}
	}

	public void onBeforeConvert(E source) {
		if (log.isDebugEnabled()) {
			log.debug("onBeforeConvert(" + source + ")");
		}
	}

	public void onBeforeSave(E source, DBObject dbo) {
		if (log.isDebugEnabled()) {
			log.debug("onBeforeSave(" + source + ", " + dbo + ")");
		}
	}

	public void onAfterSave(E source, DBObject dbo) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterSave(" + source + ", " + dbo + ")");
		}
	}

	public void onAfterLoad(DBObject dbo) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterLoad(" + dbo + ")");
		}
	}

	public void onAfterConvert(DBObject dbo, E source) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterConvert(" + dbo + "," + source + ")");
		}
	}

}
