/*
 * Copyright 2019-2022 the original author or authors.
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

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link CountQuery}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class CountQueryUnitTests {

	QueryMapper mapper;
	MongoMappingContext context;
	MappingMongoConverter converter;

	MongoDatabaseFactory factory = mock(MongoDatabaseFactory.class);

	@BeforeEach
	public void setUp() {

		this.context = new MongoMappingContext();

		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), context);
		this.converter.afterPropertiesSet();

		this.mapper = new QueryMapper(converter);
	}

	@Test // DATAMONGO-2059
	public void nearToGeoWithinWithoutDistance() {

		Query source = query(where("location").near(new Point(-73.99171, 40.738868)));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document
				.parse("{\"location\": {\"$geoWithin\": {\"$center\": [[-73.99171, 40.738868], 1.7976931348623157E308]}}}"));
	}

	@Test // DATAMONGO-2059
	public void nearAndExisting$and() {

		Query source = query(where("location").near(new Point(-73.99171, 40.738868)).minDistance(0.01))
				.addCriteria(new Criteria().andOperator(where("foo").is("bar")));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document.parse("{\"$and\":[" //
				+ "{\"foo\":\"bar\"}" //
				+ "{\"$nor\":[{\"location\":{\"$geoWithin\":{\"$center\":[ [ -73.99171, 40.738868 ], 0.01]}}}]},"//
				+ "  {\"location\":{\"$geoWithin\":{\"$center\":[ [ -73.99171, 40.738868 ], 1.7976931348623157E308]}}},"//
				+ "]}"));
	}

	@Test // DATAMONGO-2059
	public void nearSphereToGeoWithinWithoutDistance() {

		Query source = query(where("location").nearSphere(new Point(-73.99171, 40.738868)));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document.parse(
				"{\"location\": {\"$geoWithin\": {\"$centerSphere\": [[-73.99171, 40.738868], 1.7976931348623157E308]}}}"));
	}

	@Test // DATAMONGO-2059
	public void nearToGeoWithinWithMaxDistance() {

		Query source = query(where("location").near(new Point(-73.99171, 40.738868)).maxDistance(10));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(
				org.bson.Document.parse("{\"location\": {\"$geoWithin\": {\"$center\": [[-73.99171, 40.738868], 10.0]}}}"));
	}

	@Test // DATAMONGO-2059
	public void nearSphereToGeoWithinWithMaxDistance() {

		Query source = query(where("location").nearSphere(new Point(-73.99171, 40.738868)).maxDistance(10));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document
				.parse("{\"location\": {\"$geoWithin\": {\"$centerSphere\": [[-73.99171, 40.738868], 10.0]}}}"));
	}

	@Test // DATAMONGO-2059
	public void nearToGeoWithinWithMinDistance() {

		Query source = query(where("location").near(new Point(-73.99171, 40.738868)).minDistance(0.01));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document.parse(
				"{\"$and\":[{\"$nor\":[{\"location\":{\"$geoWithin\":{\"$center\":[ [ -73.99171, 40.738868 ], 0.01]}}}]},"
						+ "  {\"location\":{\"$geoWithin\":{\"$center\":[ [ -73.99171, 40.738868 ], 1.7976931348623157E308]}}}]}"));
	}

	@Test // DATAMONGO-2059
	public void nearToGeoWithinWithMaxDistanceAndCombinedWithOtherCriteria() {

		Query source = query(
				where("name").is("food").and("location").near(new Point(-73.99171, 40.738868)).maxDistance(10));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document
				.parse("{\"name\": \"food\", \"location\": {\"$geoWithin\": {\"$center\": [[-73.99171, 40.738868], 10.0]}}}"));
	}

	@Test // DATAMONGO-2059
	public void nearToGeoWithinWithMinDistanceOrCombinedWithOtherCriteria() {

		Query source = query(new Criteria().orOperator(where("name").is("food"),
				where("location").near(new Point(-73.99171, 40.738868)).minDistance(0.01)));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document.parse(
				"{\"$or\" : [ { \"name\": \"food\" }, {\"$and\":[{\"$nor\":[{\"location\":{\"$geoWithin\":{\"$center\":[ [ -73.99171, 40.738868 ], 0.01]}}}]},{\"location\":{\"$geoWithin\":{\"$center\":[ [ -73.99171, 40.738868 ], 1.7976931348623157E308]}}}]} ]}"));
	}

	@Test // DATAMONGO-2059
	public void nearToGeoWithinWithMaxDistanceOrCombinedWithOtherCriteria() {

		Query source = query(new Criteria().orOperator(where("name").is("food"),
				where("location").near(new Point(-73.99171, 40.738868)).maxDistance(10)));
		org.bson.Document target = postProcessQueryForCount(source);

		assertThat(target).isEqualTo(org.bson.Document.parse(
				"{\"$or\" : [ { \"name\": \"food\" }, {\"location\": {\"$geoWithin\": {\"$center\": [[-73.99171, 40.738868], 10.0]}}} ]}"));
	}

	private org.bson.Document postProcessQueryForCount(Query source) {

		org.bson.Document intermediate = mapper.getMappedObject(source.getQueryObject(), (MongoPersistentEntity<?>) null);
		return CountQuery.of(intermediate).toQueryDocument();
	}
}
