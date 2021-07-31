/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.*;

import java.util.Arrays;
import java.util.Collections;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Round}.
 *
 * @author Christoph Strobl
 */
public class ArithmeticOperatorsUnitTests {

	@Test // DATAMONGO-2370
	void roundShouldWithoutPlace() {

		assertThat(valueOf("field").round().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$round", Collections.singletonList("$field")));
	}

	@Test // DATAMONGO-2370
	void roundShouldWithPlace() {

		assertThat(valueOf("field").roundToPlace(3).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$round", Arrays.asList("$field", 3)));
	}

	@Test // DATAMONGO-2370
	void roundShouldWithPlaceFromField() {

		assertThat(valueOf("field").round().placeOf("my-field").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$round", Arrays.asList("$field", "$my-field")));
	}

	@Test // DATAMONGO-2370
	void roundShouldWithPlaceFromExpression() {

		assertThat(valueOf("field").round().placeOf((ctx -> new Document("$first", "$source")))
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(new Document("$round", Arrays.asList("$field", new Document("$first", "$source"))));
	}

	@Test // GH-3724
	void rendersRank() {
		assertThat(rand().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(new Document("$rand", new Document()));
	}
}
