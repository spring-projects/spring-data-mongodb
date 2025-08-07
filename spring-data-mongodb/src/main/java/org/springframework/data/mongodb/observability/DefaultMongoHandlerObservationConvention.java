/*
 * Copyright 2022-2025 the original author or authors.
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

import io.micrometer.common.KeyValues;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.event.CommandStartedEvent;

/**
 * Default {@link MongoHandlerObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 * @author Michal Domagala
 * @since 4.0
 */
class DefaultMongoHandlerObservationConvention implements MongoHandlerObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(MongoHandlerContext context) {

		if (context.getCommandStartedEvent() == null) {
			throw new IllegalStateException("not command started event present");
		}

		return MongoObservation.LowCardinality.observe(context).toKeyValues();
	}

	@Override
	public String getContextualName(MongoHandlerContext context) {

		String collectionName = context.getCollectionName();
		CommandStartedEvent commandStartedEvent = context.getCommandStartedEvent();

		Assert.notNull(commandStartedEvent, "CommandStartedEvent must not be null");

		if (ObjectUtils.isEmpty(collectionName)) {
			return commandStartedEvent.getCommandName();
		}

		return collectionName + "." + commandStartedEvent.getCommandName();
	}

}
