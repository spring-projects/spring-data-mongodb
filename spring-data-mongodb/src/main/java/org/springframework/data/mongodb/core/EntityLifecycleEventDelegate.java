/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.Nullable;

/**
 * Delegate class to encapsulate lifecycle event configuration and publishing.
 *
 * @author Mark Paluch
 * @since 4.0
 * @see ApplicationEventPublisher
 */
class EntityLifecycleEventDelegate {

	private @Nullable ApplicationEventPublisher publisher;
	private boolean eventsEnabled = true;

	public void setPublisher(@Nullable ApplicationEventPublisher publisher) {
		this.publisher = publisher;
	}

	public boolean isEventsEnabled() {
		return eventsEnabled;
	}

	public void setEventsEnabled(boolean eventsEnabled) {
		this.eventsEnabled = eventsEnabled;
	}

	/**
	 * Publish an application event if event publishing is enabled.
	 *
	 * @param event the application event.
	 */
	public void publishEvent(Object event) {

		if (canPublishEvent()) {
			publisher.publishEvent(event);
		}
	}

	private boolean canPublishEvent() {
		return publisher != null && eventsEnabled;
	}
}
