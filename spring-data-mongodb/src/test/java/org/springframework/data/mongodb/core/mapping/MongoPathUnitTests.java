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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;

/**
 * Unit tests for {@link MongoPath.RawMongoPath}.
 *
 * @author Mark Paluch
 */
class MongoPathUnitTests {

	MongoMappingContext mappingContext = new MongoMappingContext();
	QueryMapper queryMapper = new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext));

	@Test // GH-4516
	void shouldParsePaths() {

		assertThat(MongoPath.RawMongoPath.parse("foo")).hasToString("foo");
		assertThat(MongoPath.RawMongoPath.parse("foo.bar")).hasToString("foo.bar");
		assertThat(MongoPath.RawMongoPath.parse("foo.$")).hasToString("foo.$");
		assertThat(MongoPath.RawMongoPath.parse("foo.$[].baz")).hasToString("foo.$[].baz");
		assertThat(MongoPath.RawMongoPath.parse("foo.$[1234].baz")).hasToString("foo.$[1234].baz");
		assertThat(MongoPath.RawMongoPath.parse("foo.$size")).hasToString("foo.$size");
	}

	@Test // GH-4516
	void shouldTranslateFieldNames() {

		MongoPersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(Person.class);
		MongoPaths paths = new MongoPaths(mappingContext);

		assertThat(paths.mappedPath(MongoPath.RawMongoPath.parse("foo"), persistentEntity.getTypeInformation()))
				.hasToString("foo");
		assertThat(paths.mappedPath(MongoPath.RawMongoPath.parse("firstName"), persistentEntity.getTypeInformation()))
				.hasToString("fn");
		assertThat(paths.mappedPath(MongoPath.RawMongoPath.parse("firstName.$"), persistentEntity.getTypeInformation()))
				.hasToString("fn.$");
		assertThat(paths.mappedPath(MongoPath.RawMongoPath.parse("others.$.zip"), persistentEntity.getTypeInformation()))
				.hasToString("os.$.z");
		assertThat(paths.mappedPath(MongoPath.RawMongoPath.parse("others.$[].zip"), persistentEntity.getTypeInformation()))
				.hasToString("os.$[].z");
		assertThat(paths.mappedPath(MongoPath.RawMongoPath.parse("others.$[1].zip"), persistentEntity.getTypeInformation()))
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
