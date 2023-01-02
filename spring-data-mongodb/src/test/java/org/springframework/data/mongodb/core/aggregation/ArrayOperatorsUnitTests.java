/*
 * Copyright 2018-2023 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.ArrayToObject;

/**
 * Unit tests for {@link ArrayOperators}
 *
 * @author Christoph Strobl
 * @author Shashank Sharma
 * @author Divya Srivastava
 * @currentRead Royal Assassin - Robin Hobb
 */
public class ArrayOperatorsUnitTests {

	static final List<Object> VALUE_LIST = Arrays.asList(1, "2", new Document("_id", 3));
	static final String VALUE_LIST_STRING = "[1, \"2\", { \"_id\" : 3 }]";
	static final String EXPRESSION_STRING = "{ \"$stablemaster\" : \"burrich\" }";
	static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2052
	public void toArrayWithFieldReference() {

		assertThat(ArrayOperators.arrayOf("regal").toObject().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $arrayToObject: \"$regal\" } ");
	}

	@Test // DATAMONGO-2052
	public void toArrayWithExpression() {

		assertThat(ArrayOperators.arrayOf(EXPRESSION).toObject().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $arrayToObject: " + EXPRESSION_STRING + "} ");
	}

	@Test // DATAMONGO-2052
	public void toArrayWithArgumentList() {

		List<List<String>> source = new ArrayList<>();
		source.add(Arrays.asList("king", "shrewd"));
		source.add(Arrays.asList("prince", "verity"));

		assertThat(ArrayToObject.arrayToObject(source).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $arrayToObject: [ [ \"king\", \"shrewd\"], [ \"prince\", \"verity\" ] ] } ");
	}

	@Test // DATAMONGO-2287
	public void arrayElementAtWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).elementAt(1).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $arrayElemAt: [ " + VALUE_LIST_STRING + ", 1] } ");
	}

	@Test // DATAMONGO-2287
	public void concatWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).concat("field").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $concatArrays: [ " + VALUE_LIST_STRING + ", \"$field\"] } ");
	}

	@Test // DATAMONGO-2287
	public void filterWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).filter().as("var").by(new Document())
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo("{ $filter: { \"input\" : " + VALUE_LIST_STRING + ", \"as\" :  \"var\", \"cond\" : {}  } } ");
	}

	@Test // DATAMONGO-2287
	public void lengthWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).length().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $size: [ " + VALUE_LIST_STRING + "] } ");
	}

	@Test // DATAMONGO-2287
	public void sliceWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).slice().itemCount(3).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $slice: [ " + VALUE_LIST_STRING + ", 3] } ");
	}

	@Test // DATAMONGO-2287
	public void indexOfWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).indexOf("s1p").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $indexOfArray: [ " + VALUE_LIST_STRING + ", \"s1p\"] } ");
	}

	@Test // DATAMONGO-2287
	public void reverseWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).reverse().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $reverseArray: [ " + VALUE_LIST_STRING + "] } ");
	}

	@Test // DATAMONGO-2287
	public void zipWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).zipWith("field").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $zip: { \"inputs\": [" + VALUE_LIST_STRING + ", \"$field\"]} } ");
	}

	@Test // DATAMONGO-2287
	public void inWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).containsValue("$userName").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ \"$in\" : [\"$userName\", " + VALUE_LIST_STRING + "] }");
	}

	@Test // GH-3694
	public void firstWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).first().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ \"$first\" : " + VALUE_LIST_STRING + "}");
	}

	@Test // GH-3694
	public void firstWithExpression() {

		assertThat(ArrayOperators.arrayOf(EXPRESSION).first().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ \"$first\" : " + EXPRESSION_STRING + "}");
	}

	@Test // GH-3694
	public void firstWithFieldReference() {

		assertThat(ArrayOperators.arrayOf("field").first().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $first : \"$field\" }");
	}

	@Test // GH-3694
	public void lastWithValueList() {

		assertThat(ArrayOperators.arrayOf(VALUE_LIST).last().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ \"$last\" : " + VALUE_LIST_STRING + "}");
	}

	@Test // GH-3694
	public void lastWithExpression() {

		assertThat(ArrayOperators.arrayOf(EXPRESSION).last().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ \"$last\" : " + EXPRESSION_STRING + "}");
	}

	@Test // GH-3694
	public void lastWithFieldReference() {

		assertThat(ArrayOperators.arrayOf("field").last().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $last : \"$field\" }");
	}

	@Test // GH-4139
	void sortByWithFieldRef() {

		assertThat(ArrayOperators.arrayOf("team").sort(Sort.by("name")).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $sortArray: { input: \"$team\", sortBy: { name: 1 } } }");
	}
}
