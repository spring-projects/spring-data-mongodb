/*
 * Copyright 2016-2024 the original author or authors.
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

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.DocumentTestUtils;

/**
 * Unit tests for {@link LookupOperation}.
 *
 * @author Alessio Fachechi
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LookupOperationUnitTests {

	@Test // DATAMONGO-1326
	public void rejectsNullForFrom() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> new LookupOperation(null, Fields.field("localField"), Fields.field("foreignField"), Fields.field("as")));
	}

	@Test // DATAMONGO-1326
	public void rejectsNullLocalFieldField() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> new LookupOperation(Fields.field("from"), null, Fields.field("foreignField"), Fields.field("as")));
	}

	@Test // DATAMONGO-1326
	public void rejectsNullForeignField() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> new LookupOperation(Fields.field("from"), Fields.field("localField"), null, Fields.field("as")));
	}

	@Test // DATAMONGO-1326
	public void rejectsNullForAs() {
		assertThatIllegalArgumentException().isThrownBy(() -> new LookupOperation(Fields.field("from"),
				Fields.field("localField"), Fields.field("foreignField"), null));
	}

	@Test // DATAMONGO-1326
	public void lookupOperationWithValues() {

		LookupOperation lookupOperation = Aggregation.lookup("a", "b", "c", "d");

		Document lookupClause = extractDocumentFromLookupOperation(lookupOperation);

		assertThat(lookupClause).containsEntry("from", "a") //
				.containsEntry("localField", "b") //
				.containsEntry("foreignField", "c") //
				.containsEntry("as", "d");
	}

	@Test // DATAMONGO-1326
	public void lookupOperationExposesAsField() {

		LookupOperation lookupOperation = Aggregation.lookup("a", "b", "c", "d");

		assertThat(lookupOperation.getFields().exposesNoFields()).isFalse();
		assertThat(lookupOperation.getFields().exposesSingleFieldOnly()).isTrue();
		assertThat(lookupOperation.getFields().getField("d")).isNotNull();
	}

	private Document extractDocumentFromLookupOperation(LookupOperation lookupOperation) {

		Document document = lookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document lookupClause = DocumentTestUtils.getAsDocument(document, "$lookup");
		return lookupClause;
	}

	@Test // DATAMONGO-1326
	public void builderRejectsNullFromField() {
		assertThatIllegalArgumentException().isThrownBy(() -> LookupOperation.newLookup().from(null));
	}

	@Test // DATAMONGO-1326
	public void builderRejectsNullLocalField() {
		assertThatIllegalArgumentException().isThrownBy(() -> LookupOperation.newLookup().from("a").localField(null));
	}

	@Test // DATAMONGO-1326
	public void builderRejectsNullForeignField() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> LookupOperation.newLookup().from("a").localField("b").foreignField(null));
	}

	@Test // DATAMONGO-1326
	public void builderRejectsNullAsField() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> LookupOperation.newLookup().from("a").localField("b").foreignField("c").as(null));
	}

	@Test // DATAMONGO-1326
	public void lookupBuilderBuildsCorrectClause() {

		LookupOperation lookupOperation = LookupOperation.newLookup().from("a").localField("b").foreignField("c").as("d");

		Document lookupClause = extractDocumentFromLookupOperation(lookupOperation);

		assertThat(lookupClause).containsEntry("from", "a") //
				.containsEntry("localField", "b") //
				.containsEntry("foreignField", "c") //
				.containsEntry("as", "d");
	}

	@Test // DATAMONGO-1326
	public void lookupBuilderExposesFields() {

		LookupOperation lookupOperation = LookupOperation.newLookup().from("a").localField("b").foreignField("c").as("d");

		assertThat(lookupOperation.getFields().exposesNoFields()).isFalse();
		assertThat(lookupOperation.getFields().exposesSingleFieldOnly()).isTrue();
		assertThat(lookupOperation.getFields().getField("d")).isNotNull();
	}
}
