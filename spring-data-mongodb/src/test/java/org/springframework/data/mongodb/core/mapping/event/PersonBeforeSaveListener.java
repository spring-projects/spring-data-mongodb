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

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.ApplicationEvent;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;

public class PersonBeforeSaveListener extends AbstractMongoEventListener<PersonPojoStringId> {

	public final List<ApplicationEvent> seenEvents = new ArrayList<ApplicationEvent>();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeSave(java.lang.Object, com.mongodb.Document)
	 */
	@Override
	public void onBeforeSave(BeforeSaveEvent<PersonPojoStringId> event) {
		seenEvents.add(event);
	}
}
