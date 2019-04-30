/*
 * Copyright 2013-2019 the original author or authors.
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

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Distance;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link GeoNearOperation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class GeoNearOperationUnitTests {

	@Test // DATAMONGO-1127
	public void rendersNearQueryAsAggregationOperation() {

		NearQuery query = NearQuery.near(10.0, 10.0);
		GeoNearOperation operation = new GeoNearOperation(query, "distance");
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document nearClause = DocumentTestUtils.getAsDocument(document, "$geoNear");

		Document expected = new Document(query.toDocument()).append("distanceField", "distance");
		assertThat(nearClause).isEqualTo(expected);
	}

	@Test // DATAMONGO-2050
	public void rendersNearQueryWithKeyCorrectly() {

		NearQuery query = NearQuery.near(10.0, 10.0);
		GeoNearOperation operation = new GeoNearOperation(query, "distance").useIndex("geo-index-1");
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(DocumentTestUtils.getAsDocument(document, "$geoNear")).containsEntry("key", "geo-index-1");
	}

	@Test // DATAMONGO-2264
	public void rendersMaxDistanceCorrectly() {

		NearQuery query = NearQuery.near(10.0, 20.0).maxDistance(new Distance(30.0));

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).maxDistance(30.0).doc());
	}

	@Test // DATAMONGO-2264
	public void rendersMinDistanceCorrectly() {

		NearQuery query = NearQuery.near(10.0, 20.0).minDistance(new Distance(30.0));

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).minDistance(30.0).doc());
	}

	@Test // DATAMONGO-2264
	public void rendersSphericalCorrectly() {

		NearQuery query = NearQuery.near(10.0, 20.0).spherical(true);

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).spherical(true).doc());
	}

	@Test // DATAMONGO-2264
	public void rendersDistanceMultiplier() {

		NearQuery query = NearQuery.near(10.0, 20.0).inKilometers();

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).spherical(true).distanceMultiplier(6378.137).doc());
	}

	@Test // DATAMONGO-2264
	public void rendersIndexKey() {

		NearQuery query = NearQuery.near(10.0, 20.0);

		assertThat(new GeoNearOperation(query, "distance").useIndex("index-1").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).key("index-1").doc());
	}

	@Test // DATAMONGO-2264
	public void rendersQuery() {

		NearQuery query = NearQuery.near(10.0, 20.0).query(Query.query(Criteria.where("city").is("Austin")));

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).query(new Document("city", "Austin")).doc());
	}

	@Test // DATAMONGO-2264
	public void rendersMappedQuery() {

		NearQuery query = NearQuery.near(10.0, 20.0).query(Query.query(Criteria.where("city").is("Austin")));

		assertThat(
				new GeoNearOperation(query, "distance").toPipelineStages(typedAggregationOperationContext(GeoDocument.class)))
						.containsExactly($geoNear().near(10.0, 20.0).query(new Document("ci-ty", "Austin")).doc());
	}

	@Test // DATAMONGO-2264
	public void appliesSkipFromNearQuery() {

		NearQuery query = NearQuery.near(10.0, 20.0).skip(10L);

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).doc(), new Document("$skip", 10L));
	}

	@Test // DATAMONGO-2264
	public void appliesLimitFromNearQuery() {

		NearQuery query = NearQuery.near(10.0, 20.0).limit(10L);

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).doc(), new Document("$limit", 10L));
	}

	@Test // DATAMONGO-2264
	public void appliesSkipAndLimitInOrder() {

		NearQuery query = NearQuery.near(10.0, 20.0).limit(10L).skip(3L);

		assertThat(new GeoNearOperation(query, "distance").toPipelineStages(Aggregation.DEFAULT_CONTEXT))
				.containsExactly($geoNear().near(10.0, 20.0).doc(), new Document("$skip", 3L), new Document("$limit", 10L));
	}

	private TypeBasedAggregationOperationContext typedAggregationOperationContext(Class<?> type) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		return new TypeBasedAggregationOperationContext(type, mappingContext, new QueryMapper(converter));
	}

	GeoNearDocumentBuilder $geoNear() {
		return new GeoNearDocumentBuilder();
	}

	static class GeoDocument {

		@Id String id;
		@Field("ci-ty") String city;
	}

	static class GeoNearDocumentBuilder {

		Document target = new Document("distanceField", "distance").append("distanceMultiplier", 1.0D).append("spherical",
				false);

		GeoNearDocumentBuilder maxDistance(@Nullable Number value) {

			if (value != null) {
				target.put("maxDistance", value);
			} else {
				target.remove("maxDistance");
			}
			return this;
		}

		GeoNearDocumentBuilder minDistance(@Nullable Number value) {

			if (value != null) {
				target.put("minDistance", value);
			} else {
				target.remove("minDistance");
			}
			return this;
		}

		GeoNearDocumentBuilder near(Number... coordinates) {

			target.put("near", Arrays.asList(coordinates));
			return this;
		}

		GeoNearDocumentBuilder spherical(@Nullable Boolean value) {

			if (value != null) {
				target.put("spherical", value);
			} else {
				target.remove("spherical");
			}
			return this;
		}

		GeoNearDocumentBuilder distanceField(@Nullable String value) {

			if (value != null) {
				target.put("distanceField", value);
			} else {
				target.remove("distanceField");
			}
			return this;
		}

		GeoNearDocumentBuilder distanceMultiplier(Number value) {

			if (value != null) {
				target.put("distanceMultiplier", value);
			} else {
				target.remove("distanceMultiplier");
			}
			return this;
		}

		GeoNearDocumentBuilder key(String value) {

			if (value != null) {
				target.put("key", value);
			} else {
				target.remove("key");
			}
			return this;
		}

		GeoNearDocumentBuilder query(Document value) {

			if (value != null) {
				target.put("query", value);
			} else {
				target.remove("query");
			}
			return this;
		}

		Document doc() {
			return new Document("$geoNear", new Document(target));
		}

	}

	// TODO: we need to test this to the full extend
}
