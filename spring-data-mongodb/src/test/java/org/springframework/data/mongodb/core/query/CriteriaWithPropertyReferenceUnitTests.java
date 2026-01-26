/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.springframework.data.core.TypedPropertyPath;

/**
 * Unit tests for {@link Criteria} with property references.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
class CriteriaWithPropertyReferenceUnitTests {

	static Criteria base = new Criteria("name").is("Bubba");

	static List<Fixture> compare = List.of( //
			new Fixture( //
					"constructor", //
					Criteria.where((TestEntity e) -> e.name), //
					new Criteria("name") //
			), //
			new Fixture( //
					"path", //
					Criteria.where(TypedPropertyPath.of((TestEntity e) -> e.referenced).then(r -> r.value)), //
					new Criteria("referenced.value") //
			), //
			new Fixture( //
					"where", //
					Criteria.where((TestEntity e) -> e.name), //
					new Criteria("name") //
			), //
			new Fixture( //
					"and", //
					base.and((TestEntity e) -> e.age), //
					base.and("age") //
			) //
	);

	@FieldSource
	@ParameterizedTest // GH-5135
	void compare(Fixture fixture) {
		assertThat(fixture.underTest).describedAs(fixture.description).isEqualTo(fixture.expected);
	}

	record Fixture(String description, Criteria underTest, Criteria expected) {
	}

	record TestEntity(String name, Long age, Referenced referenced) {
	}

	record Referenced(String value) {
	}
}
