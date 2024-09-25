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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.util.ErrorHandler;

/**
 * Unit tests for {@link TaskFactory}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class TaskFactoryUnitTests {

	@Mock MongoConverter converter;
	@Mock MongoTemplate template;
	@Mock MessageListener<Object, Object> messageListener;
	@Mock ErrorHandler errorHandler;

	private TaskFactory factory;

	@BeforeEach
	void setUp() {
		factory = new TaskFactory(template);
	}

	@Test // DATAMONGO-1803
	void requestMustNotBeNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> factory.forRequest(null, Object.class, errorHandler));
	}

	@Test // DATAMONGO-1803
	void createsChangeStreamRequestCorrectly() {

		when(template.getConverter()).thenReturn(converter);

		ChangeStreamRequestOptions options = Mockito.mock(ChangeStreamRequestOptions.class);
		Task task = factory.forRequest(new ChangeStreamRequest(messageListener, options), Object.class, errorHandler);

		assertThat(task).isInstanceOf(ChangeStreamTask.class);
	}

	@Test // DATAMONGO-1803
	void createsTailableRequestCorrectly() {

		when(template.getConverter()).thenReturn(converter);

		RequestOptions options = Mockito.mock(RequestOptions.class);
		when(options.getCollectionName()).thenReturn("collection-1");
		Task task = factory.forRequest(new TailableCursorRequest(messageListener, options), Object.class, errorHandler);

		assertThat(task).isInstanceOf(TailableCursorTask.class);
	}
}
