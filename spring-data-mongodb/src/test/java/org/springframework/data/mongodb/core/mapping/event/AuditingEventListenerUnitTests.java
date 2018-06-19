/*
 * Copyright 2012-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Wither;

import java.util.Arrays;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalAnswers;
import org.mockito.junit.MockitoJUnitRunner;
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

		doAnswer(AdditionalAnswers.returnsArgAt(0)).when(handler).markCreated(any());
		doAnswer(AdditionalAnswers.returnsArgAt(0)).when(handler).markModified(any());

		listener = new AuditingEventListener(() -> handler);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-577
	public void rejectsNullAuditingHandler() {
		new AuditingEventListener(null);
	}

	@Test // DATAMONGO-577
	public void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample, "collection-1"));

		verify(handler, times(1)).markCreated(sample);
		verify(handler, times(0)).markModified(any());
	}

	@Test // DATAMONGO-577
	public void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		listener.onApplicationEvent(new BeforeConvertEvent<Object>(sample, "collection-1"));

		verify(handler, times(0)).markCreated(any());
		verify(handler, times(1)).markModified(sample);
	}

	@Test
	public void hasExplicitOrder() {

		assertThat(listener, is(instanceOf(Ordered.class)));
		assertThat(listener.getOrder(), is(100));
	}

	@Test // DATAMONGO-1992
	public void propagatesChangedInstanceToEvent() {

		ImmutableSample sample = new ImmutableSample();
		BeforeConvertEvent<Object> event = new BeforeConvertEvent<>(sample, "collection");

		ImmutableSample newSample = new ImmutableSample();
		IsNewAwareAuditingHandler handler = mock(IsNewAwareAuditingHandler.class);
		doReturn(newSample).when(handler).markAudited(eq(sample));

		AuditingEventListener listener = new AuditingEventListener(() -> handler);
		listener.onApplicationEvent(event);

		assertThat(event.getSource()).isSameAs(newSample);
	}

	static class Sample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}

	@Value
	@Wither
	@AllArgsConstructor
	@NoArgsConstructor(force = true)
	static class ImmutableSample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}
}
