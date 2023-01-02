/*
 * Copyright 2021-2023 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.DocumentOperators.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DocumentOperators}.
 *
 * @author Christoph Strobl
 */
class DocumentOperatorsUnitTests {

	@Test // GH-3715
	void rendersRank() {
		assertThat(rank().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $rank: {  } }");
	}

	@Test // GH-3715
	void rendersDenseRank() {
		assertThat(denseRank().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $denseRank: {  } }");
	}

	@Test // GH-3717
	void rendersDocumentNumber() {
		assertThat(documentNumber().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $documentNumber: {  } }");
	}

	@Test // GH-3727
	void rendersShift() {

		assertThat(valueOf("quantity").shift(1).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $shift: { output: \"$quantity\", by: 1 } }");
	}

	@Test // GH-3727
	void rendersShiftWithDefault() {

		assertThat(valueOf("quantity").shift(1).defaultTo("Not available").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $shift: { output: \"$quantity\", by: 1, default: \"Not available\" } }");
	}
}
