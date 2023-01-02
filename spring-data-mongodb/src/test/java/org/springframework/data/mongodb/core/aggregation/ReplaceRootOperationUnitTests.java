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
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplaceRootDocumentOperation;

/**
 * Unit tests for {@link ReplaceRootOperation}.
 *
 * @author Mark Paluch
 */
class ReplaceRootOperationUnitTests {

	@Test // DATAMONGO-1550
	void rejectsNullField() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReplaceRootOperation((Field) null));
	}

	@Test // DATAMONGO-1550
	void rejectsNullExpression() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReplaceRootOperation((AggregationExpression) null));
	}

	@Test // DATAMONGO-1550
	void shouldRenderCorrectly() {

		ReplaceRootOperation operation = ReplaceRootDocumentOperation.builder()
				.withDocument(new Document("hello", "world"));
		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject).isEqualTo(Document.parse("{ $replaceRoot : { newRoot: { hello: \"world\" } } }"));
	}

	@Test // DATAMONGO-1550
	void shouldRenderExpressionCorrectly() {

		ReplaceRootOperation operation = new ReplaceRootOperation(VariableOperators //
				.mapItemsOf("array") //
				.as("element") //
				.andApply(ArithmeticOperators.valueOf("$$element").multiplyBy(10)));

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject).isEqualTo(Document.parse("{ $replaceRoot : { newRoot : { "
				+ "$map : { input : \"$array\" , as : \"element\" , in : { $multiply : [ \"$$element\" , 10]} } " + "} } }"));
	}

	@Test // DATAMONGO-1550
	void shouldComposeDocument() {

		ReplaceRootOperation operation = ReplaceRootDocumentOperation.builder().withDocument() //
				.andValue("value").as("key") //
				.and(ArithmeticOperators.valueOf("$$element").multiplyBy(10)).as("multiply");

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject).isEqualTo(Document
				.parse("{ $replaceRoot : { newRoot: { key: \"value\", multiply: { $multiply : [ \"$$element\" , 10]} } } }"));
	}

	@Test // DATAMONGO-1550
	void shouldComposeSubDocument() {

		Document partialReplacement = new Document("key", "override").append("key2", "value2");

		ReplaceRootOperation operation = ReplaceRootDocumentOperation.builder().withDocument() //
				.andValue("value").as("key") //
				.andValuesOf(partialReplacement);

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject)
				.isEqualTo(Document.parse("{ $replaceRoot : { newRoot: { key: \"override\", key2: \"value2\"} } } }"));
	}

	@Test // DATAMONGO-1550
	void shouldNotExposeFields() {

		ReplaceRootOperation operation = new ReplaceRootOperation(Fields.field("field"));

		assertThat(operation.getFields().exposesNoFields()).isTrue();
		assertThat(operation.getFields().exposesSingleFieldOnly()).isFalse();
	}
}
