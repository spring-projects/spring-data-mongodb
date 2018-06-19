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
package org.springframework.data.mongodb.core;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link ExecutableMapReduceOperationSupport}.
 *
 * @author Christoph Strobl
 * @currentRead Beyond the Shadows - Brent Weeks
 */
@RunWith(MockitoJUnitRunner.class)
public class ExecutableMapReduceOperationSupportUnitTests {

	private static final String STAR_WARS = "star-wars";
	private static final String MAP_FUNCTION = "function() { emit(this.id, this.firstname) }";
	private static final String REDUCE_FUNCTION = "function(id, name) { return sum(id, name); }";

	@Mock MongoTemplate template;

	ExecutableMapReduceOperationSupport mapReduceOpsSupport;

	@Before
	public void setUp() {

		when(template.getCollectionName(eq(Person.class))).thenReturn(STAR_WARS);

		mapReduceOpsSupport = new ExecutableMapReduceOperationSupport(template);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1929
	public void throwsExceptionOnNullTemplate() {
		new ExecutableMapReduceOperationSupport(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1929
	public void throwsExceptionOnNullDomainType() {
		mapReduceOpsSupport.mapReduce(null);
	}

	@Test // DATAMONGO-1929
	public void usesExtractedCollectionName() {

		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	public void usesExplicitCollectionName() {

		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION)
				.inCollection("the-night-angel").all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq("the-night-angel"), eq(MAP_FUNCTION),
				eq(REDUCE_FUNCTION), isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	public void usesMapReduceOptionsWhenPresent() {

		MapReduceOptions options = MapReduceOptions.options();
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).with(options).all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				eq(options), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	public void usesQueryWhenPresent() {

		Query query = new BasicQuery("{ 'lastname' : 'skywalker' }");
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).matching(query).all();

		verify(template).mapReduce(eq(query), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	public void usesProjectionWhenPresent() {

		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).as(Jedi.class).all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Jedi.class));
	}

	interface Contact {}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person implements Contact {

		@Id String id;
		String firstname;
		String lastname;
		Object ability;
		Person father;
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Jedi {

		@Field("firstname") String name;
	}
}
