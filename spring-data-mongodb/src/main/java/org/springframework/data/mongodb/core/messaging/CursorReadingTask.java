/*
 * Copyright 2018-2021 the original author or authors.
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.Message.MessageProperties;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import com.mongodb.client.MongoCursor;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> type of objects returned by the cursor.
 * @param <R> conversion target type.
 * @since 2.1
 */
abstract class CursorReadingTask<T, R> implements Task {

	private final Object lifecycleMonitor = new Object();

	private final MongoTemplate template;
	private final SubscriptionRequest<T, R, RequestOptions> request;
	private final Class<R> targetType;
	private final ErrorHandler errorHandler;
	private final CountDownLatch awaitStart = new CountDownLatch(1);

	private State state = State.CREATED;

	private MongoCursor<T> cursor;

	/**
	 * @param template must not be {@literal null}.
	 * @param request must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	CursorReadingTask(MongoTemplate template, SubscriptionRequest<?, ? super T, ? extends RequestOptions> request,
			Class<R> targetType, ErrorHandler errorHandler) {

		this.template = template;
		this.request = (SubscriptionRequest) request;
		this.targetType = targetType;
		this.errorHandler = errorHandler;
	}

	@Override
	public void run() {

		try {

			start();

			while (isRunning()) {

				try {

					T next = execute(this::getNext);

					if (next != null) {
						emitMessage(createMessage(next, targetType, request.getRequestOptions()));
					} else {
						Thread.sleep(10);
					}
				} catch (InterruptedException e) {

					synchronized (lifecycleMonitor) {
						state = State.CANCELLED;
					}
					Thread.currentThread().interrupt();
					break;
				}
			}
		} catch (RuntimeException e) {

			synchronized (lifecycleMonitor) {
				state = State.CANCELLED;
			}

			errorHandler.handleError(e);
		}
	}

	/**
	 * Initialize the Task by 1st setting the current state to {@link State#STARTING starting} indicating the
	 * initialization procedure. <br />
	 * Moving on the underlying {@link MongoCursor} gets {@link #initCursor(MongoTemplate, RequestOptions, Class) created}
	 * and is {@link #isValidCursor(MongoCursor) health checked}. Once a valid {@link MongoCursor} is created the
	 * {@link #state} is set to {@link State#RUNNING running}. If the health check is not passed the {@link MongoCursor}
	 * is immediately {@link MongoCursor#close() closed} and a new {@link MongoCursor} is requested until a valid one is
	 * retrieved or the {@link #state} changes.
	 */
	private void start() {

		synchronized (lifecycleMonitor) {
			if (!State.RUNNING.equals(state)) {
				state = State.STARTING;
			}
		}

		do {

			boolean valid = false;

			synchronized (lifecycleMonitor) {

				if (State.STARTING.equals(state)) {

					MongoCursor<T> cursor = execute(() -> initCursor(template, request.getRequestOptions(), targetType));
					valid = isValidCursor(cursor);
					if (valid) {
						this.cursor = cursor;
						state = State.RUNNING;
					} else if (cursor != null) {
						cursor.close();
					}
				}
			}

			if (!valid) {

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {

					synchronized (lifecycleMonitor) {
						state = State.CANCELLED;
					}
					Thread.currentThread().interrupt();
				}
			}
		} while (State.STARTING.equals(getState()));

		if (awaitStart.getCount() == 1) {
			awaitStart.countDown();
		}
	}

	protected abstract MongoCursor<T> initCursor(MongoTemplate template, RequestOptions options, Class<?> targetType);

	@Override
	public void cancel() throws DataAccessResourceFailureException {

		synchronized (lifecycleMonitor) {

			if (State.RUNNING.equals(state) || State.STARTING.equals(state)) {
				this.state = State.CANCELLED;
				if (cursor != null) {
					cursor.close();
				}
			}
		}
	}

	@Override
	public boolean isLongLived() {
		return true;
	}

	@Override
	public State getState() {

		synchronized (lifecycleMonitor) {
			return state;
		}
	}

	@Override
	public boolean awaitStart(Duration timeout) throws InterruptedException {

		Assert.notNull(timeout, "Timeout must not be null!");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative!");

		return awaitStart.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}

	protected Message<T, R> createMessage(T source, Class<R> targetType, RequestOptions options) {

		SimpleMessage<T, T> message = new SimpleMessage<>(source, source, MessageProperties.builder()
				.databaseName(template.getDb().getName()).collectionName(options.getCollectionName()).build());

		return new LazyMappingDelegatingMessage<>(message, targetType, template.getConverter());
	}

	private boolean isRunning() {
		return State.RUNNING.equals(getState());
	}

	@SuppressWarnings("unchecked")
	private void emitMessage(Message<T, R> message) {
		try {
			request.getMessageListener().onMessage((Message) message);
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
	}

	@Nullable
	private T getNext() {

		synchronized (lifecycleMonitor) {
			if (State.RUNNING.equals(state)) {
				return cursor.tryNext();
			}
		}

		throw new IllegalStateException(String.format("Cursor %s is not longer open.", cursor));
	}

	private static boolean isValidCursor(@Nullable MongoCursor<?> cursor) {

		if (cursor == null) {
			return false;
		}

		if (cursor.getServerCursor() == null || cursor.getServerCursor().getId() == 0) {
			return false;
		}

		return true;
	}

	/**
	 * Execute an operation and take care of translating exceptions using the {@link MongoTemplate templates}
	 * {@link org.springframework.data.mongodb.core.MongoExceptionTranslator} rethrowing the potentially translated
	 * exception.
	 *
	 * @param callback must not be {@literal null}.
	 * @param <V>
	 * @return can be {@literal null}.
	 * @throws RuntimeException The potentially translated exception.
	 */
	@Nullable
	private <V> V execute(Supplier<V> callback) {

		try {
			return callback.get();
		} catch (RuntimeException e) {

			RuntimeException translated = template.getExceptionTranslator().translateExceptionIfPossible(e);
			throw translated != null ? translated : e;
		}
	}
}
