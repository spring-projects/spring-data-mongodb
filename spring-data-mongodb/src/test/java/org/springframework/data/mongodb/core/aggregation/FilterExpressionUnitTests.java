/*
 * Copyright 2016. the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

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

	/**
	 * @see DATAMONGO-1491
	 */
	@Test
	public void shouldConstructFilterExpressionCorrectly() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project()
						.and(filter("items").as("item").by(AggregationFunctionExpressions.GTE.of(Fields.field("item.price"), 100)))
						.as("items"));

		DBObject dbo = agg.toDbObject("sales", aggregationContext);

		List<Object> pipeline = DBObjectTestUtils.getAsList(dbo, "pipeline");
		DBObject $project = DBObjectTestUtils.getAsDBObject((DBObject) pipeline.get(0), "$project");
		DBObject items = DBObjectTestUtils.getAsDBObject($project, "items");
		DBObject $filter = DBObjectTestUtils.getAsDBObject(items, "$filter");

		DBObject expected = (DBObject) JSON.parse("{" + //
				"input: \"$items\"," + //
				"as: \"item\"," + //
				"cond: { $gte: [ \"$$item.price\", 100 ] }" + //
				"}");

		assertThat($filter, is(expected));
	}

	/**
	 * @see DATAMONGO-1491
	 */
	@Test
	public void shouldConstructFilterExpressionCorrectlyWhenUsingFilterOnProjectionBuilder() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class, Aggregation.project().and("items")
				.filter("item", AggregationFunctionExpressions.GTE.of(Fields.field("item.price"), 100)).as("items"));

		DBObject dbo = agg.toDbObject("sales", aggregationContext);

		List<Object> pipeline = DBObjectTestUtils.getAsList(dbo, "pipeline");
		DBObject $project = DBObjectTestUtils.getAsDBObject((DBObject) pipeline.get(0), "$project");
		DBObject items = DBObjectTestUtils.getAsDBObject($project, "items");
		DBObject $filter = DBObjectTestUtils.getAsDBObject(items, "$filter");

		DBObject expected = (DBObject) JSON.parse("{" + //
				"input: \"$items\"," + //
				"as: \"item\"," + //
				"cond: { $gte: [ \"$$item.price\", 100 ] }" + //
				"}");

		assertThat($filter, is(expected));
	}

	/**
	 * @see DATAMONGO-1491
	 */
	@Test
	public void shouldConstructFilterExpressionCorrectlyWhenInputMapToArray() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project().and(filter(Arrays.<Object> asList(1, "a", 2, null, 3.1D, 4, "5")).as("num")
						.by(AggregationFunctionExpressions.GTE.of(Fields.field("num"), 3))).as("items"));

		DBObject dbo = agg.toDbObject("sales", aggregationContext);

		List<Object> pipeline = DBObjectTestUtils.getAsList(dbo, "pipeline");
		DBObject $project = DBObjectTestUtils.getAsDBObject((DBObject) pipeline.get(0), "$project");
		DBObject items = DBObjectTestUtils.getAsDBObject($project, "items");
		DBObject $filter = DBObjectTestUtils.getAsDBObject(items, "$filter");

		DBObject expected = (DBObject) JSON.parse("{" + //
				"input: [ 1, \"a\", 2, null, 3.1, 4, \"5\" ]," + //
				"as: \"num\"," + //
				"cond: { $gte: [ \"$$num\", 3 ] }" + //
				"}");

		assertThat($filter, is(expected));
	}

	static class Sales {

		List<Object> items;
	}
}
