/*
 * Copyright 2012-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Sort.Direction;

/**
 * Unit tests for {@link IndexField}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class IndexFieldUnitTests {

	@Test
	public void createsPlainIndexFieldCorrectly() {

		IndexField field = IndexField.create("foo", Direction.ASC);

		assertThat(field.getKey()).isEqualTo("foo");
		assertThat(field.getDirection()).isEqualTo(Direction.ASC);
		assertThat(field.isGeo()).isFalse();
	}

	@Test
	public void createsGeoIndexFieldCorrectly() {

		IndexField field = IndexField.geo("foo");

		assertThat(field.getKey()).isEqualTo("foo");
		assertThat(field.getDirection()).isNull();
		assertThat(field.isGeo()).isTrue();
	}

	@Test
	public void correctEqualsForPlainFields() {

		IndexField first = IndexField.create("foo", Direction.ASC);
		IndexField second = IndexField.create("foo", Direction.ASC);

		assertThat(first).isEqualTo(second);
		assertThat(second).isEqualTo(first);
	}

	@Test
	public void correctEqualsForGeoFields() {

		IndexField first = IndexField.geo("bar");
		IndexField second = IndexField.geo("bar");

		assertThat(first).isEqualTo(second);
		assertThat(second).isEqualTo(first);
	}

	@Test // DATAMONGO-1183
	public void correctTypeForHashedFields() {
		assertThat(IndexField.hashed("key").isHashed()).isTrue();
	}

	@Test // DATAMONGO-1183
	public void correctEqualsForHashedFields() {

		IndexField first = IndexField.hashed("bar");
		IndexField second = IndexField.hashed("bar");

		assertThat(first).isEqualTo(second);
		assertThat(second).isEqualTo(first);
	}
}
