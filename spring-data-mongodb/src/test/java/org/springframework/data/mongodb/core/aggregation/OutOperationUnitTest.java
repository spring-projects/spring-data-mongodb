/*
 * Copyright 2016-2025 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OutOperation}.
 *
 * @author Nikolay Bogdanov
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class OutOperationUnitTest {

	@Test // DATAMONGO-1418
	void shouldCheckNPEInCreation() {
		assertThatIllegalArgumentException().isThrownBy(() -> new OutOperation(null));
	}

	@Test // DATAMONGO-2259
	void shouldUsePreMongoDB42FormatWhenOnlyCollectionIsPresent() {
		assertThat(out("out-col").toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(new Document("$out", "out-col"));
	}

	@Test // DATAMONGO-2259, GH-4969
	void shouldRenderDocument() {

		assertThat(out("out-col").in("database-2").toDocument(Aggregation.DEFAULT_CONTEXT))
				.containsEntry("$out.coll", "out-col") //
				.containsEntry("$out.db", "database-2");
	}

}
