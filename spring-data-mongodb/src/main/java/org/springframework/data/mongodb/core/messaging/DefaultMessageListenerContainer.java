/*
 * Copyright 2018-2022 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.ObjectUtils;

/**
 * Simple {@link Executor} based {@link MessageListenerContainer} implementation for running {@link Task tasks} like
 * listening to MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change Streams</a> and tailable
 * cursors.
 * <br />
 * This message container creates long-running tasks that are executed on {@link Executor}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public class DefaultMessageListenerContainer implements MessageListenerContainer {

	private final Executor taskExecutor;
	private final TaskFactory taskFactory;
	private final Optional<ErrorHandler> errorHandler;

	private final Object lifecycleMonitor = new Object();
	private final Map<SubscriptionRequest, Subscription> subscriptions = new LinkedHashMap<>();

	private boolean running = false;

	/**
	 * Create a new {@link DefaultMessageListenerContainer}.
	 *
	 * @param template must not be {@literal null}.
	 */
	public DefaultMessageListenerContainer(MongoTemplate template) {
		this(template, new SimpleAsyncTaskExecutor());
	}

	/**
	 * Create a new {@link DefaultMessageListenerContainer} running {@link Task tasks} via the given
	 * {@literal taskExecutor}.
	 *
	 * @param template must not be {@literal null}.
	 * @param taskExecutor must not be {@literal null}.
	 */
	public DefaultMessageListenerContainer(MongoTemplate template, Executor taskExecutor) {
		this(template, taskExecutor, null);
	}

	/**
	 * Create a new {@link DefaultMessageListenerContainer} running {@link Task tasks} via the given
	 * {@literal taskExecutor} delegating {@link Exception errors} to the given {@link ErrorHandler}.
	 *
	 * @param template must not be {@literal null}. Used by the {@link TaskFactory}.
	 * @param taskExecutor must not be {@literal null}.
	 * @param errorHandler the default {@link ErrorHandler} to be used by tasks inside the container. Can be
	 *          {@literal null}.
	 */
	public DefaultMessageListenerContainer(MongoTemplate template, Executor taskExecutor,
			@Nullable ErrorHandler errorHandler) {

		Assert.notNull(template, "Template must not be null");
		Assert.notNull(taskExecutor, "TaskExecutor must not be null");

		this.taskExecutor = taskExecutor;
		this.taskFactory = new TaskFactory(template);
		this.errorHandler = Optional.ofNullable(errorHandler);
	}

	@Override
	public boolean isAutoStartup() {
		return false;
	}

	@Override
	public void stop(Runnable callback) {

		stop();
		callback.run();
	}

	@Override
	public void start() {

		synchronized (lifecycleMonitor) {

			if (this.running) {
				return;
			}

			subscriptions.values().stream() //
					.filter(it -> !it.isActive()) //
					.filter(it -> it instanceof TaskSubscription) //
					.map(TaskSubscription.class::cast) //
					.map(TaskSubscription::getTask) //
					.forEach(taskExecutor::execute);

			running = true;
		}
	}

	@Override
	public void stop() {

		synchronized (lifecycleMonitor) {

			if (this.running) {

				subscriptions.values().forEach(Cancelable::cancel);

				running = false;
			}
		}
	}

	@Override
	public boolean isRunning() {

		synchronized (this.lifecycleMonitor) {
			return running;
		}
	}

	@Override
	public int getPhase() {
		return Integer.MAX_VALUE;
	}

	@Override
	public <S, T> Subscription register(SubscriptionRequest<S, ? super T, ? extends RequestOptions> request,
			Class<T> bodyType) {

		return register(request, bodyType, errorHandler.orElseGet(
				() -> new DecoratingLoggingErrorHandler((exception) -> lookup(request).ifPresent(Subscription::cancel))));
	}

	@Override
	public <S, T> Subscription register(SubscriptionRequest<S, ? super T, ? extends RequestOptions> request,
			Class<T> bodyType, ErrorHandler errorHandler) {

		return register(request, taskFactory.forRequest(request, bodyType, errorHandler));
	}

	@Override
	public Optional<Subscription> lookup(SubscriptionRequest<?, ?, ?> request) {

		synchronized (lifecycleMonitor) {
			return Optional.ofNullable(subscriptions.get(request));
		}
	}

	public Subscription register(SubscriptionRequest request, Task task) {

		Subscription subscription = new TaskSubscription(task);

		synchronized (lifecycleMonitor) {

			if (subscriptions.containsKey(request)) {
				return subscriptions.get(request);
			}

			this.subscriptions.put(request, subscription);

			if (this.running) {
				taskExecutor.execute(task);
			}
		}

		return subscription;
	}

	@Override
	public void remove(Subscription subscription) {

		synchronized (lifecycleMonitor) {

			if (subscriptions.containsValue(subscription)) {

				if (subscription.isActive()) {
					subscription.cancel();
				}

				subscriptions.values().remove(subscription);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class TaskSubscription implements Subscription {

		private final Task task;

		TaskSubscription(Task task) {
			this.task = task;
		}

		Task getTask() {
			return task;
		}

		@Override
		public boolean isActive() {
			return task.isActive();
		}

		@Override
		public boolean await(Duration timeout) throws InterruptedException {
			return task.awaitStart(timeout);
		}

		@Override
		public void cancel() throws DataAccessResourceFailureException {
			task.cancel();
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			TaskSubscription that = (TaskSubscription) o;

			return ObjectUtils.nullSafeEquals(this.task, that.task);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(task);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private static class DecoratingLoggingErrorHandler implements ErrorHandler {

		private final Log logger = LogFactory.getLog(DecoratingLoggingErrorHandler.class);

		private final ErrorHandler delegate;

		DecoratingLoggingErrorHandler(ErrorHandler delegate) {
			this.delegate = delegate;
		}

		@Override
		public void handleError(Throwable t) {

			if (logger.isErrorEnabled()) {
				logger.error("Unexpected error occurred while listening to MongoDB", t);
			}

			delegate.handleError(t);
		}
	}
}
