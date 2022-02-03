/*
 * Copyright 2013-2021 the original author or authors.
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

package org.springframework.data.mongodb.observability;

import com.mongodb.RequestContext;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.micrometer.api.instrument.observation.Observation;

/**
 * A {@link Observation.Context} that contains Mongo events.
 *
 * @author Marcin Grzejszczak
 * @since 4.0.0
 */
public class MongoHandlerContext extends Observation.Context {

	private final CommandStartedEvent commandStartedEvent;

	private final RequestContext requestContext;

	private CommandSucceededEvent commandSucceededEvent;

	private CommandFailedEvent commandFailedEvent;

	public MongoHandlerContext(CommandStartedEvent commandStartedEvent, RequestContext requestContext) {
		this.commandStartedEvent = commandStartedEvent;
		this.requestContext = requestContext;
	}

	public CommandStartedEvent getCommandStartedEvent() {
		return this.commandStartedEvent;
	}

	public CommandSucceededEvent getCommandSucceededEvent() {
		return this.commandSucceededEvent;
	}

	public void setCommandSucceededEvent(CommandSucceededEvent commandSucceededEvent) {
		this.commandSucceededEvent = commandSucceededEvent;
	}

	public CommandFailedEvent getCommandFailedEvent() {
		return this.commandFailedEvent;
	}

	public void setCommandFailedEvent(CommandFailedEvent commandFailedEvent) {
		this.commandFailedEvent = commandFailedEvent;
	}

	public RequestContext getRequestContext() {
		return requestContext;
	}
}
