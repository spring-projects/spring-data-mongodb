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
package org.springframework.data.mongodb.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.mongodb.core.Message;
import org.springframework.data.mongodb.core.MessageListener;
import org.springframework.data.mongodb.core.Subscription;

/**
 * Utilities for testing long running asnyc message retrieval.
 *
 * @author Christoph Strobl
 */
class SubscriptionUtils {

	static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

	/**
	 * Wait for {@link Subscription#isActive() to become active} but not longer than {@link #DEFAULT_TIMEOUT}.
	 *
	 * @param subscription
	 * @throws InterruptedException
	 */
	static void awaitSubscription(Subscription subscription) throws InterruptedException {
		awaitSubscription(subscription, DEFAULT_TIMEOUT);
	}

	/**
	 * Wait for all {@link Subscription Subscriptions} to {@link Subscription#isActive() become active} but not longer
	 * than {@link #DEFAULT_TIMEOUT}.
	 *
	 * @param subscription
	 * @throws InterruptedException
	 */
	static void awaitSubscriptions(Subscription... subscriptions) throws InterruptedException {
		awaitSubscriptions(DEFAULT_TIMEOUT, subscriptions);
	}

	/**
	 * Wait for all {@link Subscription Subscriptions} to {@link Subscription#isActive() become active} but not longer
	 * than {@literal timeout}.
	 *
	 * @param timeout
	 * @param subscriptions
	 * @throws InterruptedException
	 */
	static void awaitSubscriptions(Duration timeout, Subscription... subscriptions) throws InterruptedException {

		long passedMs = 0;
		long maxMs = timeout.toMillis();

		Collection<Subscription> subscriptionList = Arrays.asList(subscriptions);

		while (!subscriptionList.stream().allMatch(Subscription::isActive) && passedMs < maxMs) {

			Thread.sleep(10);
			passedMs += 10;
		}
	}

	/**
	 * Wait for {@link Subscription#isActive() to become active} but not longer than {@literal timeout}.
	 *
	 * @param subscription
	 * @param timeout
	 * @throws InterruptedException
	 */
	static void awaitSubscription(Subscription subscription, Duration timeout) throws InterruptedException {

		long passedMs = 0;
		long maxMs = timeout.toMillis();

		while (!subscription.isActive() && passedMs < maxMs) {
			Thread.sleep(10);
			passedMs += 10;
		}
	}

	/**
	 * Wait for {@link CollectingMessageListener} to receive messages but not longer than {@link #DEFAULT_TIMEOUT}.
	 *
	 * @param listener
	 * @throws InterruptedException
	 */
	static void awaitMessages(CollectingMessageListener listener) throws InterruptedException {
		awaitMessages(listener, Integer.MAX_VALUE);
	}

	/**
	 * Wait for {@link CollectingMessageListener} to receive exactly {@literal nrMessages} messages but not longer than
	 * {@link #DEFAULT_TIMEOUT}.
	 *
	 * @param listener
	 * @param nrMessages
	 * @throws InterruptedException
	 */
	static void awaitMessages(CollectingMessageListener listener, int nrMessages) throws InterruptedException {
		awaitMessages(listener, nrMessages, DEFAULT_TIMEOUT);
	}

	/**
	 * Wait for {@link CollectingMessageListener} to receive exactly {@literal nrMessages} messages but not longer than
	 * {@literal timeout}.
	 *
	 * @param listener
	 * @param nrMessages
	 * @param timeout
	 * @throws InterruptedException
	 */
	static void awaitMessages(CollectingMessageListener listener, int nrMessages, Duration timeout)
			throws InterruptedException {

		long passedMs = 0;
		long maxMs = timeout.toMillis();

		while (listener.getTotalNumberMessagesReceived() < nrMessages && passedMs < maxMs) {
			Thread.sleep(10);
			passedMs += 10;
		}
	}

	/**
	 * {@link MessageListener} implementation collecting received {@link Message messages}.
	 *
	 * @param <M>
	 */
	static class CollectingMessageListener<M extends Message> implements MessageListener<M> {

		private volatile List<M> messages = new ArrayList<>();

		@Override
		public void onMessage(M message) {
			messages.add(message);
		}

		int getTotalNumberMessagesReceived() {
			return messages.size();
		}

		public List<M> getMessages() {
			return messages;
		}

		public M getMessage(int nr) {
			return messages.get(nr);
		}

		public M getFirstMessage() {
			return messages.get(0);
		}

		public M getLastMessage() {
			return messages.get(messages.size() - 1);
		}
	}

}
