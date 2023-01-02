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

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.DocumentTestUtils;

/**
 * Unit tests for {@link UnwindOperation}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class UnwindOperationUnitTests {

	@Test // DATAMONGO-1391
	public void unwindWithPathOnlyShouldUsePreMongo32Syntax() {

		UnwindOperation unwindOperation = Aggregation.unwind("a");

		Document pipeline = unwindOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(pipeline).containsEntry("$unwind", "$a");
	}

	@Test // DATAMONGO-1391
	public void unwindWithArrayIndexShouldUseMongo32Syntax() {

		UnwindOperation unwindOperation = Aggregation.unwind("a", "index");

		Document unwindClause = extractDocumentFromUnwindOperation(unwindOperation);

		assertThat(unwindClause).containsEntry("path", "$a").//
				containsEntry("preserveNullAndEmptyArrays", false).//
				containsEntry("includeArrayIndex", "index");
	}

	@Test // DATAMONGO-1391
	public void unwindWithArrayIndexShouldExposeArrayIndex() {

		UnwindOperation unwindOperation = Aggregation.unwind("a", "index");

		assertThat(unwindOperation.getFields().getField("index")).isNotNull();
	}

	@Test // DATAMONGO-1391
	public void plainUnwindShouldNotExposeIndex() {

		UnwindOperation unwindOperation = Aggregation.unwind("a");

		assertThat(unwindOperation.getFields().exposesNoFields()).isTrue();
	}

	@Test // DATAMONGO-1391
	public void unwindWithPreserveNullShouldUseMongo32Syntax() {

		UnwindOperation unwindOperation = Aggregation.unwind("a", true);

		Document unwindClause = extractDocumentFromUnwindOperation(unwindOperation);

		assertThat(unwindClause).containsEntry("path", "$a").//
				containsEntry("preserveNullAndEmptyArrays", true).//
				doesNotContainKey("includeArrayIndex");
	}

	@Test // DATAMONGO-1391
	public void lookupBuilderBuildsCorrectClause() {

		UnwindOperation unwindOperation = UnwindOperation.newUnwind().path("$foo").noArrayIndex().skipNullAndEmptyArrays();
		Document pipeline = unwindOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(pipeline).containsEntry("$unwind", "$foo");
	}

	@Test // DATAMONGO-1391
	public void lookupBuilderBuildsCorrectClauseForMongo32() {

		UnwindOperation unwindOperation = UnwindOperation.newUnwind().path("$foo").arrayIndex("myindex")
				.preserveNullAndEmptyArrays();

		Document unwindClause = extractDocumentFromUnwindOperation(unwindOperation);

		assertThat(unwindClause).containsEntry("path", "$foo").//
				containsEntry("preserveNullAndEmptyArrays", true).//
				containsEntry("includeArrayIndex", "myindex");
	}

	private Document extractDocumentFromUnwindOperation(UnwindOperation unwindOperation) {

		Document document = unwindOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document unwindClause = DocumentTestUtils.getAsDocument(document, "$unwind");
		return unwindClause;
	}
}
