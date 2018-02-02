/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

/**
 * A simple factory for creating {@link Task} for a given {@link SubscriptionRequest}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
class TaskFactory {

	private final MongoTemplate tempate;

	/**
	 * @param template must not be {@literal null}.
	 */
	TaskFactory(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");

		this.tempate = template;
	}

	/**
	 * Create a {@link Task} for the given {@link SubscriptionRequest}.
	 *
	 * @param request must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @param errorHandler must not be {@literal null}.
	 * @return must not be {@literal null}. Consider {@code Object.class}.
	 * @throws IllegalArgumentException in case the {@link SubscriptionRequest} is unknown.
	 */
	<S, T> Task forRequest(SubscriptionRequest<S, ? super T, ? extends RequestOptions> request, Class<T> targetType,
			ErrorHandler errorHandler) {

		Assert.notNull(request, "Request must not be null!");
		Assert.notNull(targetType, "TargetType must not be null!");

		if (request instanceof ChangeStreamRequest) {
			return new ChangeStreamTask(tempate, (ChangeStreamRequest) request, targetType, errorHandler);
		} else if (request instanceof TailableCursorRequest) {
			return new TailableCursorTask(tempate, (TailableCursorRequest) request, targetType, errorHandler);
		}

		throw new IllegalArgumentException(
				"oh wow - seems you're using some fancy new feature we do not support. Please be so kind and leave us a note in the issue tracker so we can get this fixed.\nThank you!");
	}
}
