/*
 * Copyright 2011-2021 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.ApplicationListener;

/**
 * {@link ApplicationListener} for Mongo mapping events logging the events.
 *
 * @author Jon Brisbin
 * @author Martin Baumgartner
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class LoggingEventListener extends AbstractMongoEventListener<Object> {

	private static final Log LOGGER = LogFactory.getLog(LoggingEventListener.class);

	@Override
	public void onBeforeConvert(BeforeConvertEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onBeforeConvert: %s", event.getSource()));
		}
	}

	@Override
	public void onBeforeSave(BeforeSaveEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onBeforeSave: %s, %s", event.getSource(), serializeToJsonSafely(event.getDocument())));
		}
	}

	@Override
	public void onAfterSave(AfterSaveEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onAfterSave: %s, %s", event.getSource(), serializeToJsonSafely(event.getDocument())));
		}
	}

	@Override
	public void onAfterLoad(AfterLoadEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onAfterLoad: %s", serializeToJsonSafely(event.getDocument())));
		}
	}

	@Override
	public void onAfterConvert(AfterConvertEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onAfterConvert: %s, %s", serializeToJsonSafely(event.getDocument()), event.getSource()));
		}
	}

	@Override
	public void onAfterDelete(AfterDeleteEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onAfterDelete: %s", serializeToJsonSafely(event.getDocument())));
		}
	}

	@Override
	public void onBeforeDelete(BeforeDeleteEvent<Object> event) {
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("onBeforeDelete: %s", serializeToJsonSafely(event.getDocument())));
		}
	}
}
