/*
 * Copyright 2011-2016 the original author or authors.
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

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;

/**
 * {@link ApplicationListener} for Mongo mapping events logging the events.
 * 
 * @author Jon Brisbin
 * @author Martin Baumgartner
 * @author Oliver Gierke
 */
public class LoggingEventListener extends AbstractMongoEventListener<Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(LoggingEventListener.class);

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeConvert(org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent)
	 */
	@Override
	public void onBeforeConvert(BeforeConvertEvent<Object> event) {
		LOGGER.info("onBeforeConvert: {}", event.getSource());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeSave(org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent)
	 */
	@Override
	public void onBeforeSave(BeforeSaveEvent<Object> event) {
		LOGGER.info("onBeforeSave: {}, {}", event.getSource(), event.getDocument());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterSave(org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent)
	 */
	@Override
	public void onAfterSave(AfterSaveEvent<Object> event) {
		LOGGER.info("onAfterSave: {}, {}", event.getSource(), event.getDocument());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterLoad(org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent)
	 */
	@Override
	public void onAfterLoad(AfterLoadEvent<Object> event) {
		LOGGER.info("onAfterLoad: {}", event.getDocument());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterConvert(org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent)
	 */
	@Override
	public void onAfterConvert(AfterConvertEvent<Object> event) {
		LOGGER.info("onAfterConvert: {}, {}", event.getDocument(), event.getSource());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterDelete(org.springframework.data.mongodb.core.mapping.event.AfterDeleteEvent)
	 */
	@Override
	public void onAfterDelete(AfterDeleteEvent<Object> event) {
		LOGGER.info("onAfterDelete: {}", event.getDocument());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeDelete(org.springframework.data.mongodb.core.mapping.event.BeforeDeleteEvent)
	 */
	@Override
	public void onBeforeDelete(BeforeDeleteEvent<Object> event) {
		LOGGER.info("onBeforeDelete: {}", event.getDocument());
	}
}
