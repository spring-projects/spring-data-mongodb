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

import java.time.Duration;

import org.springframework.util.Assert;

/**
 * The {@link Subscription} is the link between the {@link SubscriptionRequest} and the actual running {@link Task}.
 * <p />
 * Due to the asynchronous nature of the {@link Task} execution a {@link Subscription} might not immediately become
 * active. {@link #isActive()} provides an answer if the underlying {@link Task} is already running.
 * <p />
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface Subscription extends Cancelable {

	/**
	 * @return {@literal true} if the subscription is currently executed.
	 */
	boolean isActive();

	/**
	 * Synchronous, <strong>blocking</strong> call that polls the current state and returns once the {@link Subscription}
	 * becomes {@link #isActive() active}.
	 * <p />
	 * If interrupted while waiting the current Subscription state is returned.
	 *
	 * @param timeout must not be {@literal null}.
	 */
	default boolean await(Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null!");

		long sleepTime = 25;

		long currentMs = System.currentTimeMillis();
		long targetMs = currentMs + timeout.toMillis();

		while (currentMs < targetMs && !isActive()) {

			try {
				Thread.sleep(sleepTime);
				currentMs += sleepTime;
			} catch (InterruptedException e) {

				Thread.interrupted();
				break;
			}
		}

		return isActive();
	}
}
