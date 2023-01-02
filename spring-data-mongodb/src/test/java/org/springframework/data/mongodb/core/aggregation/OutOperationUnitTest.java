/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OutOperation}.
 *
 * @author Nikolay Bogdanov
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class OutOperationUnitTest {

	@Test // DATAMONGO-1418
	public void shouldCheckNPEInCreation() {
		assertThatIllegalArgumentException().isThrownBy(() -> new OutOperation(null));
	}

	@Test // DATAMONGO-2259
	public void shouldUsePreMongoDB42FormatWhenOnlyCollectionIsPresent() {
		assertThat(out("out-col").toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(new Document("$out", "out-col"));
	}

	@Test // DATAMONGO-2259
	public void shouldUseMongoDB42ExtendedFormatWhenAdditionalParametersPresent() {

		assertThat(out("out-col").insertDocuments().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$out", new Document("to", "out-col").append("mode", "insertDocuments")));
	}

	@Test // DATAMONGO-2259
	public void shouldRenderExtendedFormatWithJsonStringKey() {

		assertThat(out("out-col").insertDocuments() //
				.in("database-2") //
				.uniqueKey("{ 'field-1' : 1, 'field-2' : 1}") //
				.toDocument(Aggregation.DEFAULT_CONTEXT)) //
						.containsEntry("$out.to", "out-col") //
						.containsEntry("$out.mode", "insertDocuments") //
						.containsEntry("$out.db", "database-2") //
						.containsEntry("$out.uniqueKey", new Document("field-1", 1).append("field-2", 1));
	}

	@Test // DATAMONGO-2259
	public void shouldRenderExtendedFormatWithSingleFieldKey() {

		assertThat(out("out-col").insertDocuments().in("database-2") //
				.uniqueKey("field-1").toDocument(Aggregation.DEFAULT_CONTEXT)) //
						.containsEntry("$out.to", "out-col") //
						.containsEntry("$out.mode", "insertDocuments") //
						.containsEntry("$out.db", "database-2") //
						.containsEntry("$out.uniqueKey", new Document("field-1", 1));
	}

	@Test // DATAMONGO-2259
	public void shouldRenderExtendedFormatWithMultiFieldKey() {

		assertThat(out("out-col").insertDocuments().in("database-2") //
				.uniqueKeyOf(Arrays.asList("field-1", "field-2")) //
				.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$out.to", "out-col") //
						.containsEntry("$out.mode", "insertDocuments") //
						.containsEntry("$out.db", "database-2") //
						.containsEntry("$out.uniqueKey", new Document("field-1", 1).append("field-2", 1));
	}

	@Test // DATAMONGO-2259
	public void shouldErrorOnExtendedFormatWithoutMode() {

		assertThatThrownBy(() -> out("out-col").in("database-2").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isInstanceOf(IllegalStateException.class);
	}

}
