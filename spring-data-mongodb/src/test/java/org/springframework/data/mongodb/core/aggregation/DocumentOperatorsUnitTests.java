/*
 * Copyright 2021 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.DocumentOperators.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * @author Christoph Strobl
 */
class DocumentOperatorsUnitTests {

	@Test // GH-3715
	void rendersRank() {
		assertThat(rank().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(new Document("$rank", new Document()));
	}

	@Test // GH-3715
	void rendersDenseRank() {
		assertThat(denseRank().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$denseRank", new Document()));
	}
}
