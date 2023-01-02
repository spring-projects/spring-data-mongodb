/*
 * Copyright 2020-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.MergeOperation.*;
import static org.springframework.data.mongodb.core.aggregation.MergeOperation.WhenDocumentsMatch.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Sum;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link MergeOperation}.
 *
 * @author Christoph Strobl
 */
class MergeOperationUnitTests {

	private static final String OUT_COLLECTION = "target-collection";
	private static final String OUT_DB = "target-db";

	private static final Document OUT = new Document("db", OUT_DB).append("coll", OUT_COLLECTION);

	@Test // DATAMONGO-2363
	void justCollection() {

		assertThat(mergeInto(OUT_COLLECTION).toDocument(DEFAULT_CONTEXT)).isEqualTo(new Document("$merge", OUT_COLLECTION));
	}

	@Test // DATAMONGO-2363
	void collectionInDatabase() {

		assertThat(merge().intoCollection(OUT_COLLECTION).inDatabase("target-db").build().toDocument(DEFAULT_CONTEXT))
				.isEqualTo(new Document("$merge", new Document("into", OUT)));
	}

	@Test // DATAMONGO-2363
	void singleOn() {

		assertThat(merge().intoCollection(OUT_COLLECTION).on("id-field").build().toDocument(DEFAULT_CONTEXT))
				.isEqualTo(new Document("$merge", new Document("into", OUT_COLLECTION).append("on", "id-field")));
	}

	@Test // DATAMONGO-2363
	void multipleOn() {

		assertThat(merge().intoCollection(OUT_COLLECTION).on("field-1", "field-2").build().toDocument(DEFAULT_CONTEXT))
				.isEqualTo(new Document("$merge",
						new Document("into", OUT_COLLECTION).append("on", Arrays.asList("field-1", "field-2"))));
	}

	@Test // DATAMONGO-2363
	void collectionAndSimpleArgs() {

		assertThat(merge().intoCollection(OUT_COLLECTION).on("_id").whenMatched(replaceDocument())
				.whenNotMatched(WhenDocumentsDontMatch.insertNewDocument()).build().toDocument(DEFAULT_CONTEXT))
						.isEqualTo(new Document("$merge", new Document("into", OUT_COLLECTION).append("on", "_id")
								.append("whenMatched", "replace").append("whenNotMatched", "insert")));
	}

	@Test // DATAMONGO-2363
	void whenMatchedWithAggregation() {

		String expected = "{ \"$merge\" : {\"into\": \"" + OUT_COLLECTION + "\", \"whenMatched\":  ["
				+ "{ \"$addFields\" : {" //
				+ "\"thumbsup\": { \"$sum\":[ \"$thumbsup\", \"$$new.thumbsup\" ] },"
				+ "\"thumbsdown\": { \"$sum\": [ \"$thumbsdown\", \"$$new.thumbsdown\" ] } } } ]" //
				+ "} }";

		Aggregation update = Aggregation
				.newAggregation(AddFieldsOperation.addField("thumbsup").withValueOf(Sum.sumOf("thumbsup").and("$$new.thumbsup"))
						.addField("thumbsdown").withValueOf(Sum.sumOf("thumbsdown").and("$$new.thumbsdown")).build());

		assertThat(
				merge().intoCollection(OUT_COLLECTION).whenDocumentsMatchApply(update).build().toDocument(DEFAULT_CONTEXT))
						.isEqualTo(Document.parse(expected));
	}

	@Test // DATAMONGO-2363
	void mapsFieldNames() {

		assertThat(merge().intoCollection("newrestaurants").on("date", "postCode").build()
				.toDocument(contextFor(Restaurant.class))).isEqualTo(
						Document.parse("{ \"$merge\": { \"into\": \"newrestaurants\", \"on\": [ \"date\", \"post_code\" ] } }"));
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter)).continueOnMissingFieldReference();
	}

	static class Restaurant {

		@Field("post_code") String postCode;
	}
}
