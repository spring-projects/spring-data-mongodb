/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplaceRootDocumentOperation;

/**
 * Unit tests for {@link ReplaceRootOperation}.
 *
 * @author Mark Paluch
 */
public class ReplaceRootOperationUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1550
	public void rejectsNullField() {
		new ReplaceRootOperation((Field) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1550
	public void rejectsNullExpression() {
		new ReplaceRootOperation((AggregationExpression) null);
	}

	@Test // DATAMONGO-1550
	public void shouldRenderCorrectly() {

		ReplaceRootOperation operation = ReplaceRootDocumentOperation.builder()
				.withDocument(new Document("hello", "world"));
		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, is(Document.parse("{ $replaceRoot : { newRoot: { hello: \"world\" } } }")));
	}

	@Test // DATAMONGO-1550
	public void shouldRenderExpressionCorrectly() {

		ReplaceRootOperation operation = new ReplaceRootOperation(VariableOperators //
				.mapItemsOf("array") //
				.as("element") //
				.andApply(AggregationFunctionExpressions.MULTIPLY.of("$$element", 10)));

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, is(Document.parse("{ $replaceRoot : { newRoot : { "
				+ "$map : { input : \"$array\" , as : \"element\" , in : { $multiply : [ \"$$element\" , 10]} } " + "} } }")));
	}

	@Test // DATAMONGO-1550
	public void shouldComposeDocument() {

		ReplaceRootOperation operation = ReplaceRootDocumentOperation.builder().withDocument() //
				.andValue("value").as("key") //
				.and(AggregationFunctionExpressions.MULTIPLY.of("$$element", 10)).as("multiply");

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, is(Document
				.parse("{ $replaceRoot : { newRoot: { key: \"value\", multiply: { $multiply : [ \"$$element\" , 10]} } } }")));
	}

	@Test // DATAMONGO-1550
	public void shouldComposeSubDocument() {

		Document partialReplacement = new Document("key", "override").append("key2", "value2");

		ReplaceRootOperation operation = ReplaceRootDocumentOperation.builder().withDocument() //
				.andValue("value").as("key") //
				.andValuesOf(partialReplacement);

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, is(Document.parse("{ $replaceRoot : { newRoot: { key: \"override\", key2: \"value2\"} } } }")));
	}

	@Test // DATAMONGO-1550
	public void shouldNotExposeFields() {

		ReplaceRootOperation operation = new ReplaceRootOperation(Fields.field("field"));

		assertThat(operation.getFields().exposesNoFields(), is(true));
		assertThat(operation.getFields().exposesSingleFieldOnly(), is(false));
	}
}
