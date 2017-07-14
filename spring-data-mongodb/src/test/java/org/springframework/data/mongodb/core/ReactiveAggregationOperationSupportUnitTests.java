/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.aggregation.Aggregation;

/**
 * Unit tests for {@link ReactiveAggregationOperationSupport}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveAggregationOperationSupportUnitTests {

	@Mock ReactiveMongoTemplate template;
	ReactiveAggregationOperationSupport opSupport;

	@Before
	public void setUp() {
		opSupport = new ReactiveAggregationOperationSupport(template);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void throwsExceptionOnNullDomainType() {
		opSupport.aggregateAndReturn(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void throwsExceptionOnNullCollectionWhenUsed() {
		opSupport.aggregateAndReturn(Person.class).inCollection(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void throwsExceptionOnEmptyCollectionWhenUsed() {
		opSupport.aggregateAndReturn(Person.class).inCollection("");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void throwsExceptionOnNullAggregation() {
		opSupport.aggregateAndReturn(Person.class).by(null);
	}

	@Test // DATAMONGO-1719
	public void aggregateWithUntypedAggregationAndExplicitCollection() {

		opSupport.aggregateAndReturn(Person.class).inCollection("star-wars").by(newAggregation(project("foo"))).all();

		ArgumentCaptor<Class> captor = ArgumentCaptor.forClass(Class.class);
		verify(template).aggregate(any(Aggregation.class), eq("star-wars"), captor.capture());
		assertThat(captor.getValue()).isEqualTo(Person.class);
	}

	@Test // DATAMONGO-1719
	public void aggregateWithUntypedAggregation() {

		when(template.determineCollectionName(any(Class.class))).thenReturn("person");

		opSupport.aggregateAndReturn(Person.class).by(newAggregation(project("foo"))).all();

		ArgumentCaptor<Class> captor = ArgumentCaptor.forClass(Class.class);

		verify(template).determineCollectionName(captor.capture());
		verify(template).aggregate(any(Aggregation.class), eq("person"), captor.capture());

		assertThat(captor.getAllValues()).containsExactly(Person.class, Person.class);
	}

	@Test // DATAMONGO-1719
	public void aggregateWithTypeAggregation() {

		when(template.determineCollectionName(any(Class.class))).thenReturn("person");

		opSupport.aggregateAndReturn(Jedi.class).by(newAggregation(Person.class, project("foo"))).all();

		ArgumentCaptor<Class> captor = ArgumentCaptor.forClass(Class.class);

		verify(template).determineCollectionName(captor.capture());
		verify(template).aggregate(any(Aggregation.class), eq("person"), captor.capture());

		assertThat(captor.getAllValues()).containsExactly(Person.class, Jedi.class);
	}

	static class Person {}

	static class Jedi {}
}
