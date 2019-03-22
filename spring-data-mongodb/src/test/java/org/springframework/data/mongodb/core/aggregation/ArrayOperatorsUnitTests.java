/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.ArrayToObject;

/**
 * Unit tests for {@link ArrayOperators}
 * 
 * @author Christoph Strobl
 * @currentRead Royal Assassin - Robin Hobb
 */
public class ArrayOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$stablemaster\" : \"burrich\" }";
	static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2052
	public void toArrayWithFieldReference() {

		assertThat(ArrayOperators.arrayOf("regal").toObject().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $arrayToObject: \"$regal\" } "));
	}

	@Test // DATAMONGO-2052
	public void toArrayWithExpression() {

		assertThat(ArrayOperators.arrayOf(EXPRESSION).toObject().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $arrayToObject: " + EXPRESSION_STRING + "} "));
	}

	@Test // DATAMONGO-2052
	public void toArrayWithArgumentList() {

		List<List<String>> source = new ArrayList<>();
		source.add(Arrays.asList("king", "shrewd"));
		source.add(Arrays.asList("prince", "verity"));

		assertThat(ArrayToObject.arrayToObject(source).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $arrayToObject: [ [ \"king\", \"shrewd\"], [ \"prince\", \"verity\" ] ] } "));
	}
}
