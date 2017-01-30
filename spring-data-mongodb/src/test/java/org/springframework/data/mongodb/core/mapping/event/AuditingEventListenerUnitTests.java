/*
 * Copyright 2012-2017 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.Ordered;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.PersistentEntities;
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
		mappingContext.getPersistentEntity(Sample.class);

		handler = spy(new IsNewAwareAuditingHandler(new PersistentEntities(Arrays.asList(mappingContext))));
		doNothing().when(handler).markCreated(Mockito.any(Optional.class));
		doNothing().when(handler).markModified(Mockito.any(Optional.class));

		listener = new AuditingEventListener(new ObjectFactory<IsNewAwareAuditingHandler>() {

			@Override
			public IsNewAwareAuditingHandler getObject() throws BeansException {
				return handler;
			}
		});
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-577
	public void rejectsNullAuditingHandler() {
		new AuditingEventListener(null);
	}

	@Test // DATAMONGO-577
	public void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample, "collection-1"));

		verify(handler, times(1)).markCreated(Optional.of(sample));
		verify(handler, times(0)).markModified(Mockito.any(Optional.class));
	}

	@Test // DATAMONGO-577
	public void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample, "collection-1"));

		verify(handler, times(0)).markCreated(Mockito.any(Optional.class));
		verify(handler, times(1)).markModified(Optional.of(sample));
	}

	@Test
	public void hasExplicitOrder() {

		assertThat(listener, is(instanceOf(Ordered.class)));
		assertThat(listener.getOrder(), is(100));
	}

	static class Sample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}
}
