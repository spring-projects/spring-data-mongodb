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

package org.springframework.data.document.mongodb.mapping.event;

import com.mongodb.DBObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class LoggingEventListener<MongoMappingEvent> extends AbstractMappingEventListener {

	private Log log = LogFactory.getLog(getClass());

	@Override
	public void onBeforeConvert(Object source) {
		log.info("onBeforeConvert: " + source);
	}

	@Override
	public void onBeforeSave(Object source, DBObject dbo) {
		try {
			log.info("onBeforeSave: " + source + ", " + dbo);
		} catch (Throwable ignored) {
		}
	}

	@Override
	public void onAfterSave(Object source, DBObject dbo) {
		log.info("onAfterSave: " + source + ", " + dbo);
	}

	@Override
	public void onAfterLoad(DBObject dbo) {
		log.info("onAfterLoad: " + dbo);
	}

	@Override
	public void onAfterConvert(DBObject dbo, Object source) {
		log.info("onAfterConvert: " + dbo + ", " + source);
	}

}
