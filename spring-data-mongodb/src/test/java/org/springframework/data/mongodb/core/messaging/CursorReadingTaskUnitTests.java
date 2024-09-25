/*
 * Copyright 2018-2024 the original author or authors.
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

import static edu.umd.cs.mtc.TestFramework.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.mtc.MultithreadedTestCase;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.core.messaging.Task.State;
import org.springframework.util.ErrorHandler;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * Unit test for mainly lifecycle issues of {@link CursorReadingTask}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class CursorReadingTaskUnitTests {

	@Mock MongoDatabase db;
	@Mock MongoCursor cursor;
	@Mock SubscriptionRequest request;
	@Mock MessageListener listener;
	@Mock RequestOptions options;
	@Mock MongoTemplate template;
	@Mock ErrorHandler errorHandler;

	ValueCapturingTaskStub task;

	@BeforeEach
	public void setUp() {

		when(request.getRequestOptions()).thenReturn(options);
		when(request.getMessageListener()).thenReturn(listener);
		when(options.getCollectionName()).thenReturn("collection-name");
		when(template.getDb()).thenReturn(db);
		when(template.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		when(db.getName()).thenReturn("mock-db");

		task = new ValueCapturingTaskStub(template, request, Object.class, cursor, errorHandler);
	}

	@Test // DATAMONGO-1803
	public void stopTaskWhileStarting() throws Throwable {
		runOnce(new MultithreadedStopDuringStartupInitialization(task, cursor));
	}

	@Test // DATAMONGO-1803
	public void stopRunningTask() throws Throwable {

		when(cursor.getServerCursor()).thenReturn(new ServerCursor(10, new ServerAddress("mock")));

		runOnce(new MultithreadedStopRunning(task, cursor));
	}

	@Test // DATAMONGO-1803
	public void stopTaskWhileEmittingMessages() throws Throwable {

		when(cursor.getServerCursor()).thenReturn(new ServerCursor(10, new ServerAddress("mock")));
		when(cursor.tryNext()).thenReturn("hooyah");

		runOnce(new MultithreadedStopRunningWhileEmittingMessages(task, cursor));

		verify(listener, times(task.getValues().size())).onMessage(any());
	}

	@Test // DATAMONGO-2173, DATAMONGO-2366
	public void writesErrorOnStartToErrorHandler() {

		ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
		Task task = new ErrorOnInitCursorTaskStub(template, request, Object.class, errorHandler);

		task.run();
		verify(errorHandler).handleError(errorCaptor.capture());
		assertThat(errorCaptor.getValue()).hasMessageStartingWith("let's get it started (ha)");
	}

	@Test // DATAMONGO-2366
	public void errorOnNextNotifiesErrorHandlerOnlyOnce() {

		ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
		when(cursor.getServerCursor()).thenReturn(new ServerCursor(10, new ServerAddress("mock")));
		when(cursor.tryNext()).thenThrow(new IllegalStateException());

		task.run();
		verify(errorHandler).handleError(errorCaptor.capture());
		assertThat(errorCaptor.getValue()).isInstanceOf(IllegalStateException.class);
	}

	private static class MultithreadedStopRunningWhileEmittingMessages extends MultithreadedTestCase {

		CursorReadingTask task;
		MongoCursor cursor;

		public MultithreadedStopRunningWhileEmittingMessages(CursorReadingTask task, MongoCursor cursor) {

			this.task = task;
			this.cursor = cursor;
		}

		public void thread1() {

			assertTick(0);

			assertThat(task.getState()).isEqualTo(State.CREATED);
			task.run();

			waitForTick(1);
			assertThat(task.isActive()).isFalse();
			assertThat(task.getState()).isEqualTo(State.CANCELLED);
			verify(cursor).close();
		}

		public void thread2() throws InterruptedException {

			while (!task.isActive()) {
				Thread.sleep(20);
			}

			verify(cursor, never()).close();
			task.cancel();
		}
	}

	private static class MultithreadedStopRunning extends MultithreadedTestCase {

		CursorReadingTask task;
		MongoCursor cursor;

		public MultithreadedStopRunning(CursorReadingTask task, MongoCursor cursor) {

			this.task = task;
			this.cursor = cursor;
		}

		public void thread1() {

			assertTick(0);

			assertThat(task.getState()).isEqualTo(State.CREATED);
			task.run();

			waitForTick(2);
			assertThat(task.isActive()).isFalse();
			assertThat(task.getState()).isEqualTo(State.CANCELLED);
			verify(cursor).close();
		}

		public void thread2() throws InterruptedException {

			waitForTick(1);
			assertThat(task.isActive()).isTrue();
			assertThat(task.getState()).isEqualTo(State.RUNNING);
			verify(cursor, never()).close();

			task.cancel();
		}
	}

	private static class MultithreadedStopDuringStartupInitialization extends MultithreadedTestCase {

		CursorReadingTask task;
		MongoCursor cursor;

		public MultithreadedStopDuringStartupInitialization(CursorReadingTask task, MongoCursor cursor) {
			this.task = task;
			this.cursor = cursor;
		}

		public void thread1() {

			assertTick(0);
			task.run();

			waitForTick(2);
			assertThat(task.isActive()).isFalse();
			assertThat(task.getState()).isEqualTo(State.CANCELLED);
			verify(cursor).close();
		}

		public void thread2() throws InterruptedException {

			waitForTick(1);
			assertThat(task.isActive()).isFalse();
			assertThat(task.getState()).isEqualTo(State.STARTING);

			task.cancel();
		}
	}

	static class ValueCapturingTaskStub extends CursorReadingTask {

		final MongoCursor cursor;
		final List<Object> values = new CopyOnWriteArrayList<>();

		public ValueCapturingTaskStub(MongoTemplate template, SubscriptionRequest request, Class<?> targetType,
				MongoCursor cursor, ErrorHandler errorHandler) {

			super(template, request, targetType, errorHandler);
			this.cursor = cursor;
		}

		@Override
		protected MongoCursor initCursor(MongoTemplate dbFactory, RequestOptions options, Class targetType) {
			return cursor;
		}

		@Override
		protected Message createMessage(Object source, Class targetType, RequestOptions options) {

			values.add(source);
			return super.createMessage(source, targetType, options);
		}

		public List<Object> getValues() {
			return values;
		}
	}

	static class ErrorOnInitCursorTaskStub extends CursorReadingTask {

		public ErrorOnInitCursorTaskStub(MongoTemplate template, SubscriptionRequest request, Class targetType,
				ErrorHandler errorHandler) {
			super(template, request, targetType, errorHandler);
		}

		@Override
		protected MongoCursor initCursor(MongoTemplate template, RequestOptions options, Class targetType) {
			throw new RuntimeException("let's get it started (ha), let's get it started in here...");
		}
	}
}
