/*
 * Copyright 2012 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.MappingContextIsNewStrategyFactory;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.support.IsNewStrategyFactory;

/**
 * Unit tests for {@link AuditingEventListener}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class AuditingEventListenerUnitTests {

	IsNewAwareAuditingHandler<Object> handler;

	IsNewStrategyFactory factory;
	AuditingEventListener listener;

	@Before
	public void setUp() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		factory = new MappingContextIsNewStrategyFactory(mappingContext);

		handler = spy(new IsNewAwareAuditingHandler<Object>(factory));
		doNothing().when(handler).markCreated(Mockito.any(Object.class));
		doNothing().when(handler).markModified(Mockito.any(Object.class));

		listener = new AuditingEventListener(handler);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullAuditingHandler() {
		new AuditingEventListener(null);
	}

	@Test
	public void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample));

		verify(handler, times(1)).markCreated(sample);
		verify(handler, times(0)).markModified(any(Sample.class));
	}

	@Test
	public void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample));

		verify(handler, times(0)).markCreated(any(Sample.class));
		verify(handler, times(1)).markModified(sample);
	}

	static class Sample {

		@Id
		String id;
	}
}
