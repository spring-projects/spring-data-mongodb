/*
 * Copyright 2012-2014 the original author or authors.
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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link AuditingEventListener}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@RunWith(MockitoJUnitRunner.class)
public class AuditingEventListenerUnitTests {

	IsNewAwareAuditingHandler handler;

	AuditingEventListener listener;

	@Before
	public void setUp() {

		MongoMappingContext mappingContext = new MongoMappingContext();

		handler = spy(new IsNewAwareAuditingHandler(mappingContext));
		doNothing().when(handler).markCreated(Mockito.any(Object.class));
		doNothing().when(handler).markModified(Mockito.any(Object.class));

		listener = new AuditingEventListener(new ObjectFactory<IsNewAwareAuditingHandler>() {

			@Override
			public IsNewAwareAuditingHandler getObject() throws BeansException {
				return handler;
			}
		});
	}

	/**
	 * @see DATAMONGO-577
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullAuditingHandler() {
		new AuditingEventListener(null);
	}

	/**
	 * @see DATAMONGO-577
	 */
	@Test
	public void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample));

		verify(handler, times(1)).markCreated(sample);
		verify(handler, times(0)).markModified(any(Sample.class));
	}

	/**
	 * @see DATAMONGO-577
	 */
	@Test
	public void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample));

		verify(handler, times(0)).markCreated(any(Sample.class));
		verify(handler, times(1)).markModified(sample);
	}

	static class Sample {

		@Id String id;
	}
}
