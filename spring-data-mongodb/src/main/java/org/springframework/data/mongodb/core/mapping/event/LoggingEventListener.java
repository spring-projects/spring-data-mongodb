/*
 * Copyright 2011-2012 the original author or authors.
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

import com.mongodb.DBObject;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class LoggingEventListener extends AbstractMongoEventListener<Object> {

	private static final Logger log = LoggerFactory.getLogger(LoggingEventListener.class);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeConvert(java.lang.Object)
	 */
	@Override
	public void onBeforeConvert(Object source) {
		log.info("onBeforeConvert: " + source);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeSave(java.lang.Object, com.mongodb.DBObject)
	 */
	@Override
	public void onBeforeSave(Object source, DBObject dbo) {
		try {
			log.info("onBeforeSave: " + source + ", " + dbo);
		} catch (Throwable ignored) {
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterSave(java.lang.Object, com.mongodb.DBObject)
	 */
	@Override
	public void onAfterSave(Object source, DBObject dbo) {
		log.info("onAfterSave: " + source + ", " + dbo);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterLoad(com.mongodb.DBObject)
	 */
	@Override
	public void onAfterLoad(DBObject dbo) {
		log.info("onAfterLoad: " + dbo);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onAfterConvert(com.mongodb.DBObject, java.lang.Object)
	 */
	@Override
	public void onAfterConvert(DBObject dbo, Object source) {
		log.info("onAfterConvert: " + dbo + ", " + source);
	}

	@Override
	public void onAfterDelete(DBObject dbo) {
		log.info("onAfterDelete: {}", dbo );
	}

	@Override
	public void onBeforeDelete(DBObject dbo) {
		log.info("onBeforeDelete: {}", dbo );

	}
}
