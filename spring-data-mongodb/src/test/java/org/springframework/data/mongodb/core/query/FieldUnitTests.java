/*
 * Copyright 2013-2023 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Field}.
 *
 * @author Oliver Gierke
 * @author Owen Q
 * @author Mark Paluch
 */
class FieldUnitTests {

	@Test
	void sameObjectSetupCreatesEqualField() {

		Field left = new Field().elemMatch("key", Criteria.where("foo").is("bar"));
		Field right = new Field().elemMatch("key", Criteria.where("foo").is("bar"));

		assertThat(left).isEqualTo(right);
		assertThat(right).isEqualTo(left);
		assertThat(left.getFieldsObject()).isEqualTo("{key: { $elemMatch: {foo:\"bar\"}}}");
	}

	@Test // DATAMONGO-2294
	void rendersInclusionCorrectly() {

		Field fields = new Field().include("foo", "bar").include("baz");

		assertThat(fields.getFieldsObject()).isEqualTo("{foo:1, bar:1, baz:1}");
	}

	@Test
	void differentObjectSetupCreatesEqualField() {

		Field left = new Field().elemMatch("key", Criteria.where("foo").is("bar"));
		Field right = new Field().elemMatch("key", Criteria.where("foo").is("foo"));

		assertThat(left).isNotEqualTo(right);
		assertThat(right).isNotEqualTo(left);
	}

	@Test // DATAMONGO-2294
	void rendersExclusionCorrectly() {

		Field fields = new Field().exclude("foo", "bar").exclude("baz");

		assertThat(fields.getFieldsObject()).isEqualTo("{foo:0, bar:0, baz:0}");
	}
}
