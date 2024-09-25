/*
 * Copyright 2021-2024 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.ConditionalOperators.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConditionalOperators}.
 *
 * @author Christoph Strobl
 */
class ConditionalOperatorsUnitTests {

	@Test // GH-3720
	void rendersIfNullWithMultipleConditionalValuesCorrectly() {

		assertThat(ifNull("description").orIfNull("quantity").then("Unspecified").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ifNull: [ \"$description\", \"$quantity\", \"Unspecified\" ] }"));
	}
}
