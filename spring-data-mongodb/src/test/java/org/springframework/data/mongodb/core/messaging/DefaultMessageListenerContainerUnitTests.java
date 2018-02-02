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

import static edu.umd.cs.mtc.TestFramework.*;
import static org.assertj.core.api.Assertions.*;

import edu.umd.cs.mtc.MultithreadedTestCase;

import java.time.Duration;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.ErrorHandler;

/**
 * Unit tests for {@link DefaultMessageListenerContainer}.
 *
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMessageListenerContainerUnitTests {

	@Mock MongoTemplate template;
	@Mock ErrorHandler errorHandler;

	DefaultMessageListenerContainer container;

	@Before
	public void setUp() {
		container = new DefaultMessageListenerContainer(template);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1803
	public void throwsErrorOnNullTemplate() {
		new DefaultMessageListenerContainer(null);
	}

	@Test // DATAMONGO-1803
	public void startStopContainer() throws Throwable {
		runOnce(new MultithreadedStartStopContainer(container));
	}

	@Test // DATAMONGO-1803
	public void subscribeToContainerBeforeStartup() throws Throwable {
		runOnce(new MultithreadedSubscribeBeforeStartup(container));
	}

	@Test // DATAMONGO-1803
	public void subscribeToContainerAfterStartup() throws Throwable {
		runOnce(new MultithreadedSubscribeAfterStartup(container));
	}

	@Test // DATAMONGO-1803
	public void stopSubscriptionWhileRunning() throws Throwable {
		runOnce(new StopSubscriptionWhileRunning(container));
	}

	@Test // DATAMONGO-1803
	public void removeSubscriptionWhileRunning() throws Throwable {
		runOnce(new RemoveSubscriptionWhileRunning(container));
	}

	private static class RemoveSubscriptionWhileRunning extends MultithreadedTestCase {

		DefaultMessageListenerContainer container;
		Subscription subscription;

		public RemoveSubscriptionWhileRunning(DefaultMessageListenerContainer container) {
			this.container = container;
			subscription = container.register(new MockSubscriptionRequest(), new MockTask());
		}

		public void thread1() {

			assertTick(0);
			container.start();

			waitForTick(2);
			assertThat(container.isRunning());
			container.stop();
		}

		public void thread2() throws InterruptedException {

			waitForTick(1);
			assertThat(subscription.isActive()).isTrue();

			container.remove(subscription);
			assertThat(subscription.isActive()).isFalse();
		}
	}

	private static class StopSubscriptionWhileRunning extends MultithreadedTestCase {

		DefaultMessageListenerContainer container;
		Subscription subscription;

		public StopSubscriptionWhileRunning(DefaultMessageListenerContainer container) {
			this.container = container;
			subscription = container.register(new MockSubscriptionRequest(), new MockTask());
		}

		public void thread1() {

			assertTick(0);
			container.start();

			waitForTick(2);
			assertThat(container.isRunning());
			container.stop();
		}

		public void thread2() throws InterruptedException {

			waitForTick(1);
			assertThat(subscription.isActive()).isTrue();

			subscription.cancel();
			assertThat(subscription.isActive()).isFalse();
		}

	}

	private static class MultithreadedSubscribeAfterStartup extends MultithreadedTestCase {

		DefaultMessageListenerContainer container;

		public MultithreadedSubscribeAfterStartup(DefaultMessageListenerContainer container) {
			this.container = container;
		}

		public void thread1() {

			assertTick(0);
			container.start();

			waitForTick(2);
			container.stop();
		}

		public void thread2() throws InterruptedException {

			waitForTick(1);
			Subscription subscription = container.register(new MockSubscriptionRequest(), new MockTask());
			Thread.sleep(10);
			assertThat(subscription.isActive()).isTrue();

			waitForTick(3);
			assertThat(subscription.isActive()).isFalse();
		}

	}

	private static class MultithreadedSubscribeBeforeStartup extends MultithreadedTestCase {

		DefaultMessageListenerContainer container;

		public MultithreadedSubscribeBeforeStartup(DefaultMessageListenerContainer container) {
			this.container = container;
		}

		public void thread1() {

			assertTick(0);

			Subscription subscription = container.register(new MockSubscriptionRequest(), new MockTask());
			assertThat(subscription.isActive()).isFalse();

			waitForTick(2);
			assertThat(subscription.isActive()).isTrue();

			waitForTick(4);
			assertThat(subscription.isActive()).isFalse();
		}

		public void thread2() {

			waitForTick(1);
			container.start();

			waitForTick(3);
			container.stop();
		}

	}

	private static class MultithreadedStartStopContainer extends MultithreadedTestCase {

		DefaultMessageListenerContainer container;

		public MultithreadedStartStopContainer(DefaultMessageListenerContainer container) {
			this.container = container;
		}

		public void thread1() {

			assertTick(0);
			container.start();
			waitForTick(2);
			assertThat(container.isRunning()).isFalse();
		}

		public void thread2() {

			waitForTick(1);
			assertThat(container.isRunning()).isTrue();
			container.stop();
		}
	}

	static class MockTask implements Task {

		volatile State state;
		volatile RuntimeException error;

		@Override
		public void cancel() throws DataAccessResourceFailureException {
			state = State.CANCELLED;
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public State getState() {
			return state;
		}

		@Override
		public void run() {

			state = State.RUNNING;

			while (isActive()) {

				if (error != null) {
					throw error;
				}

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
			}
		}

		void emitError(RuntimeException error) {
			this.error = error;
		}

		@Override
		public boolean awaitStart(Duration timeout) throws InterruptedException {

			while (getState() == State.STARTING) {
				Thread.sleep(10);
			}

			return true;
		}
	}

	static class MockSubscriptionRequest implements SubscriptionRequest {

		@Override
		public MessageListener getMessageListener() {
			return message -> {};
		}

		@Override
		public RequestOptions getRequestOptions() {
			return () -> "foo";
		}
	}
}
