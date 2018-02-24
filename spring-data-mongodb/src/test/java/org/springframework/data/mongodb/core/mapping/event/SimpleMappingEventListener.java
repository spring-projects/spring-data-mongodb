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

/**
 * @author Mark Pollak
 * @author Oliver Gierke
 * @author Christoph Leiter
 * @author Christoph Strobl
 */
public class SimpleMappingEventListener extends AbstractMongoEventListener<Object> {

	public final ArrayList<BeforeConvertEvent<Object>> onBeforeConvertEvents = new ArrayList<BeforeConvertEvent<Object>>();
	public final ArrayList<BeforeSaveEvent<Object>> onBeforeSaveEvents = new ArrayList<BeforeSaveEvent<Object>>();
	public final ArrayList<AfterSaveEvent<Object>> onAfterSaveEvents = new ArrayList<AfterSaveEvent<Object>>();
	public final ArrayList<AfterLoadEvent<Object>> onAfterLoadEvents = new ArrayList<AfterLoadEvent<Object>>();
	public final ArrayList<AfterConvertEvent<Object>> onAfterConvertEvents = new ArrayList<AfterConvertEvent<Object>>();
	public final ArrayList<BeforeDeleteEvent<Object>> onBeforeDeleteEvents = new ArrayList<BeforeDeleteEvent<Object>>();
	public final ArrayList<AfterDeleteEvent<Object>> onAfterDeleteEvents = new ArrayList<AfterDeleteEvent<Object>>();

	@Override
	public void onBeforeConvert(BeforeConvertEvent<Object> event) {
		onBeforeConvertEvents.add(event);
	}

	@Override
	public void onBeforeSave(BeforeSaveEvent<Object> event) {
		onBeforeSaveEvents.add(event);
	}

	@Override
	public void onAfterSave(AfterSaveEvent<Object> event) {
		onAfterSaveEvents.add(event);
	}

	@Override
	public void onAfterLoad(AfterLoadEvent<Object> event) {
		onAfterLoadEvents.add(event);
	}

	@Override
	public void onAfterConvert(AfterConvertEvent<Object> event) {
		onAfterConvertEvents.add(event);
	}

	@Override
	public void onAfterDelete(AfterDeleteEvent<Object> event) {
		onAfterDeleteEvents.add(event);
	}

	@Override
	public void onBeforeDelete(BeforeDeleteEvent<Object> event) {
		onBeforeDeleteEvents.add(event);
	}
}
