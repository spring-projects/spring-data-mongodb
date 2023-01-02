/*
 * Copyright 2019-2023 the original author or authors.
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

/**
 * Unit tests for {@link ReplaceRootOperation}.
 *
 * @author Christoph Strobl
 */
class ReplaceWithOperationUnitTests {

	@Test // DATAMONGO-2331
	void rejectsNullField() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReplaceWithOperation(null));
	}

	@Test // DATAMONGO-2331
	void shouldRenderValueCorrectly() {

		ReplaceWithOperation operation = ReplaceWithOperation.replaceWithValue(new Document("hello", "world"));
		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject).isEqualTo(Document.parse("{ $replaceWith : { hello: \"world\" } }"));
	}

	@Test // DATAMONGO-2331
	void shouldRenderExpressionCorrectly() {

		ReplaceWithOperation operation = ReplaceWithOperation.replaceWithValueOf(VariableOperators //
				.mapItemsOf("array") //
				.as("element") //
				.andApply(ArithmeticOperators.valueOf("$$element").multiplyBy(10)));

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject).isEqualTo(Document.parse("{ $replaceWith :  { "
				+ "$map : { input : \"$array\" , as : \"element\" , in : { $multiply : [ \"$$element\" , 10]} } " + "} }"));
	}
}
