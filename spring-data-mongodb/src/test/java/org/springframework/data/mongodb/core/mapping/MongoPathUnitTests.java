/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MongoPath}.
 *
 * @author Mark Paluch
 */
class MongoPathUnitTests {

	MongoMappingContext mappingContext = new MongoMappingContext();

	@Test // GH-4516
	void shouldParsePaths() {

		assertThat(MongoPath.parse("foo")).hasToString("foo");
		assertThat(MongoPath.parse("foo.bar")).hasToString("foo.bar");
		assertThat(MongoPath.parse("foo.$")).hasToString("foo.$");
		assertThat(MongoPath.parse("foo.$[].baz")).hasToString("foo.$[].baz");
		assertThat(MongoPath.parse("foo.$[1234].baz")).hasToString("foo.$[1234].baz");
		assertThat(MongoPath.parse("foo.$size")).hasToString("foo.$size");
	}

	@Test // GH-4516
	void shouldTranslateFieldNames() {

		MongoPersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(MongoPath.parse("foo").applyFieldNames(mappingContext, persistentEntity)).hasToString("foo");
		assertThat(MongoPath.parse("firstName").applyFieldNames(mappingContext, persistentEntity)).hasToString("fn");
		assertThat(MongoPath.parse("firstName.$").applyFieldNames(mappingContext, persistentEntity)).hasToString("fn.$");
		assertThat(MongoPath.parse("others.$.zip").applyFieldNames(mappingContext, persistentEntity)).hasToString("os.$.z");
		assertThat(MongoPath.parse("others.$[].zip").applyFieldNames(mappingContext, persistentEntity))
				.hasToString("os.$[].z");
		assertThat(MongoPath.parse("others.$[1].zip").applyFieldNames(mappingContext, persistentEntity))
				.hasToString("os.$[1].z");
	}

	static class Person {

		@Field("fn") String firstName;

		Address address;

		@Field("o") Address other;
		@Field("os") List<Address> others;
	}

	static class Address {

		@Field("z") String zip;
	}
}
