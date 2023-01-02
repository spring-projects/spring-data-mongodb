/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.CollectionOptions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.validation.Validator;

/**
 * @author Christoph Strobl
 */
class CollectionOptionsUnitTests {

	@Test // GH-4210
	void emptyEquals() {
		assertThat(empty()).isEqualTo(empty());
	}

	@Test // GH-4210
	void collectionProperties() {
		assertThat(empty().maxDocuments(10).size(1).disableValidation())
				.isEqualTo(empty().maxDocuments(10).size(1).disableValidation());
	}

	@Test // GH-4210
	void changedRevisionsEquals() {
		assertThat(emitChangedRevisions()).isNotEqualTo(empty()).isEqualTo(emitChangedRevisions());
	}

	@Test // GH-4210
	void cappedEquals() {
		assertThat(empty().capped()).isNotEqualTo(empty()).isEqualTo(empty().capped());
	}

	@Test // GH-4210
	void collationEquals() {

		assertThat(empty().collation(Collation.of("en_US"))) //
				.isEqualTo(empty().collation(Collation.of("en_US"))) //
				.isNotEqualTo(empty()) //
				.isNotEqualTo(empty().collation(Collation.of("de_AT")));
	}

	@Test // GH-4210
	void timeSeriesEquals() {

		assertThat(empty().timeSeries(TimeSeriesOptions.timeSeries("tf"))) //
				.isEqualTo(empty().timeSeries(TimeSeriesOptions.timeSeries("tf"))) //
				.isNotEqualTo(empty()) //
				.isNotEqualTo(empty().timeSeries(TimeSeriesOptions.timeSeries("other")));
	}

	@Test // GH-4210
	void validatorEquals() {

		assertThat(empty().validator(Validator.document(new Document("one", "two")))) //
				.isEqualTo(empty().validator(Validator.document(new Document("one", "two")))) //
				.isNotEqualTo(empty()) //
				.isNotEqualTo(empty().validator(Validator.document(new Document("three", "four"))))
				.isNotEqualTo(empty().validator(Validator.document(new Document("one", "two"))).moderateValidation());
	}
}
