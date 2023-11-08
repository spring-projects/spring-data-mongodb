/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link ExecutableMapReduceOperationSupport}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @currentRead Beyond the Shadows - Brent Weeks
 */
@ExtendWith(MockitoExtension.class)
class ExecutableMapReduceOperationSupportUnitTests {

	private static final String STAR_WARS = "star-wars";
	private static final String MAP_FUNCTION = "function() { emit(this.id, this.firstname) }";
	private static final String REDUCE_FUNCTION = "function(id, name) { return sum(id, name); }";

	@Mock MongoTemplate template;

	private ExecutableMapReduceOperationSupport mapReduceOpsSupport;

	@BeforeEach
	void setUp() {
		mapReduceOpsSupport = new ExecutableMapReduceOperationSupport(template);
	}

	@Test // DATAMONGO-1929
	void throwsExceptionOnNullTemplate() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ExecutableMapReduceOperationSupport(null));
	}

	@Test // DATAMONGO-1929
	void throwsExceptionOnNullDomainType() {
		assertThatIllegalArgumentException().isThrownBy(() -> mapReduceOpsSupport.mapReduce(null));
	}

	@Test // DATAMONGO-1929
	void usesExtractedCollectionName() {

		when(template.getCollectionName(eq(Person.class))).thenReturn(STAR_WARS);
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	void usesExplicitCollectionName() {

		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION)
				.inCollection("the-night-angel").all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq("the-night-angel"), eq(MAP_FUNCTION),
				eq(REDUCE_FUNCTION), isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	void usesMapReduceOptionsWhenPresent() {

		when(template.getCollectionName(eq(Person.class))).thenReturn(STAR_WARS);
		MapReduceOptions options = MapReduceOptions.options();
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).with(options).all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				eq(options), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	void usesQueryWhenPresent() {

		when(template.getCollectionName(eq(Person.class))).thenReturn(STAR_WARS);
		Query query = new BasicQuery("{ 'lastname' : 'skywalker' }");
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).matching(query).all();

		verify(template).mapReduce(eq(query), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-2416
	void usesCriteriaWhenPresent() {

		when(template.getCollectionName(eq(Person.class))).thenReturn(STAR_WARS);
		Query query = Query.query(where("lastname").is("skywalker"));
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION)
				.matching(where("lastname").is("skywalker")).all();

		verify(template).mapReduce(eq(query), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Person.class));
	}

	@Test // DATAMONGO-1929
	void usesProjectionWhenPresent() {

		when(template.getCollectionName(eq(Person.class))).thenReturn(STAR_WARS);
		mapReduceOpsSupport.mapReduce(Person.class).map(MAP_FUNCTION).reduce(REDUCE_FUNCTION).as(Jedi.class).all();

		verify(template).mapReduce(any(Query.class), eq(Person.class), eq(STAR_WARS), eq(MAP_FUNCTION), eq(REDUCE_FUNCTION),
				isNull(), eq(Jedi.class));
	}

	interface Contact {}

	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person implements Contact {

		@Id String id;
		String firstname;
		String lastname;
		Object ability;
		Person father;

		public Person() {}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public Object getAbility() {
			return this.ability;
		}

		public Person getFather() {
			return this.father;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public void setAbility(Object ability) {
			this.ability = ability;
		}

		public void setFather(Person father) {
			this.father = father;
		}

		public String toString() {
			return "ExecutableMapReduceOperationSupportUnitTests.Person(id=" + this.getId() + ", firstname="
					+ this.getFirstname() + ", lastname=" + this.getLastname() + ", ability=" + this.getAbility() + ", father="
					+ this.getFather() + ")";
		}
	}

	static class Jedi {

		@Field("firstname") //
		String name;

		public Jedi(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String toString() {
			return "ExecutableMapReduceOperationSupportUnitTests.Jedi(name=" + this.getName() + ")";
		}
	}
}
