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
import static org.springframework.data.mongodb.core.aggregation.ArrayOperators.Filter.filter;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class FilterExpressionUnitTests {

	private AggregationOperationContext aggregationContext;
	private MongoMappingContext mappingContext;

	@BeforeEach
	void setUp() {

		mappingContext = new MongoMappingContext();
		aggregationContext = new TypeBasedAggregationOperationContext(Sales.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));
	}

	@Test // DATAMONGO-1491
	void shouldConstructFilterExpressionCorrectly() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project()
						.and(filter("items").as("item").by(ComparisonOperators.valueOf("item.price").greaterThanEqualToValue(100)))
						.as("items"));

		Document $filter = extractFilterOperatorFromDocument(agg.toDocument("sales", aggregationContext));
		Document expected = Document.parse("{" + //
				"input: \"$items\"," + //
				"as: \"item\"," + //
				"cond: { $gte: [ \"$$item.price\", 100 ] }" + //
				"}");

		assertThat($filter).isEqualTo(new Document(expected));
	}

	@Test // DATAMONGO-1491
	void shouldConstructFilterExpressionCorrectlyWhenUsingFilterOnProjectionBuilder() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class, Aggregation.project().and("items")
				.filter("item", ComparisonOperators.valueOf("item.price").greaterThanEqualToValue(100)).as("items"));

		Document $filter = extractFilterOperatorFromDocument(agg.toDocument("sales", aggregationContext));
		Document expected = Document.parse("{" + //
				"input: \"$items\"," + //
				"as: \"item\"," + //
				"cond: { $gte: [ \"$$item.price\", 100 ] }" + //
				"}");

		assertThat($filter).isEqualTo(expected);
	}

	@Test // DATAMONGO-1491
	void shouldConstructFilterExpressionCorrectlyWhenInputMapToArray() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project().and(filter(Arrays.<Object> asList(1, "a", 2, null, 3.1D, 4, "5")).as("num")
						.by(ComparisonOperators.valueOf("num").greaterThanEqualToValue(3))).as("items"));

		Document $filter = extractFilterOperatorFromDocument(agg.toDocument("sales", aggregationContext));
		Document expected = Document.parse("{" + //
				"input: [ 1, \"a\", 2, null, 3.1, 4, \"5\" ]," + //
				"as: \"num\"," + //
				"cond: { $gte: [ \"$$num\", 3 ] }" + //
				"}");

		assertThat($filter).isEqualTo(expected);
	}

	@Test // DATAMONGO-2320
	void shouldConstructFilterExpressionCorrectlyWhenConditionContainsFieldReference() {

		Aggregation agg = Aggregation.newAggregation(Aggregation.project().and((ctx) -> new Document()).as("field-1")
				.and(filter("items").as("item").by(ComparisonOperators.valueOf("item.price").greaterThan("field-1")))
				.as("items"));

		Document $filter = extractFilterOperatorFromDocument(agg.toDocument("sales", Aggregation.DEFAULT_CONTEXT));
		Document expected = Document.parse("{" + //
				"input: \"$items\"," + //
				"as: \"item\"," + //
				"cond: { $gt: [ \"$$item.price\", \"$field-1\" ] }" + //
				"}");

		assertThat($filter).isEqualTo(new Document(expected));
	}

	@Test // GH-4394
	void filterShouldAcceptExpression() {

		Document $filter = ArrayOperators.arrayOf(ObjectOperators.valueOf("data.metadata").toArray()).filter().as("item")
				.by(ComparisonOperators.valueOf("item.price").greaterThan("field-1")).toDocument(Aggregation.DEFAULT_CONTEXT);

		Document expected = Document.parse("""
				{ $filter : {
					input: { $objectToArray: "$data.metadata" },
					as: "item",
					cond: { $gt: [ "$$item.price", "$field-1" ] }
				}}
				""");

		assertThat($filter).isEqualTo(expected);
	}

	private Document extractFilterOperatorFromDocument(Document source) {

		List<Object> pipeline = DocumentTestUtils.getAsDBList(source, "pipeline");
		Document $project = DocumentTestUtils.getAsDocument((Document) pipeline.get(0), "$project");
		Document items = DocumentTestUtils.getAsDocument($project, "items");
		return DocumentTestUtils.getAsDocument(items, "$filter");
	}

	static class Sales {

		List<Object> items;
	}
}
