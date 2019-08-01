/*
 * Copyright 2016. the original author or authors.
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class FilterExpressionUnitTests {

	@Mock MongoDbFactory mongoDbFactory;

	private AggregationOperationContext aggregationContext;
	private MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		mappingContext = new MongoMappingContext();
		aggregationContext = new TypeBasedAggregationOperationContext(Sales.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), mappingContext)));
	}

	@Test // DATAMONGO-1491
	public void shouldConstructFilterExpressionCorrectly() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project()
						.and(filter("items").as("item").by(AggregationFunctionExpressions.GTE.of(Fields.field("item.price"), 100)))
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
	public void shouldConstructFilterExpressionCorrectlyWhenUsingFilterOnProjectionBuilder() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class, Aggregation.project().and("items")
				.filter("item", AggregationFunctionExpressions.GTE.of(Fields.field("item.price"), 100)).as("items"));

		Document $filter = extractFilterOperatorFromDocument(agg.toDocument("sales", aggregationContext));
		Document expected = Document.parse("{" + //
				"input: \"$items\"," + //
				"as: \"item\"," + //
				"cond: { $gte: [ \"$$item.price\", 100 ] }" + //
				"}");

		assertThat($filter).isEqualTo(expected);
	}

	@Test // DATAMONGO-1491
	public void shouldConstructFilterExpressionCorrectlyWhenInputMapToArray() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project().and(filter(Arrays.<Object> asList(1, "a", 2, null, 3.1D, 4, "5")).as("num")
						.by(AggregationFunctionExpressions.GTE.of(Fields.field("num"), 3))).as("items"));

		Document $filter = extractFilterOperatorFromDocument(agg.toDocument("sales", aggregationContext));
		Document expected = Document.parse("{" + //
				"input: [ 1, \"a\", 2, null, 3.1, 4, \"5\" ]," + //
				"as: \"num\"," + //
				"cond: { $gte: [ \"$$num\", 3 ] }" + //
				"}");

		assertThat($filter).isEqualTo(expected);
	}

	@Test // DATAMONGO-2320
	public void shouldConstructFilterExpressionCorrectlyWhenConditionContainsFieldReference() {

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
