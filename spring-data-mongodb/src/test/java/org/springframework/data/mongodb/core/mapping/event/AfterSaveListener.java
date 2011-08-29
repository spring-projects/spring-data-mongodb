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
package org.springframework.data.mongodb.core.mapping.event;

import java.util.ArrayList;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

public class AfterSaveListener implements ApplicationListener<AfterSaveEvent<Object>> {

	public final ArrayList<ApplicationEvent> seenEvents = new ArrayList<ApplicationEvent>();

	public void onApplicationEvent(AfterSaveEvent<Object> event) {
		this.seenEvents.add(event);
	}

}
