/*
 * Copyright 2026-present the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.bulk.BulkWriteResult;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.data.util.Pair;

import com.mongodb.ClientBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * Integration tests for {@link Bulk}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
class MongoTemplateBulkTests {

	@Client private static MongoClient mongoClient;

	@Template(initialEntitySet = { BaseDoc.class, SpecialDoc.class }) //
	private static MongoTestTemplate operations;

	@BeforeEach
	void setUp() {
		operations.flushDatabase();
	}

	@Test // GH-5087
	void bulkWriteMultipleCollections() {

		operations.flushDatabase();

		BaseDoc doc1 = new BaseDoc();
		doc1.id = "id-doc1";
		doc1.value = "value-doc1";

		BaseDoc doc2 = new BaseDoc();
		doc2.id = "id-doc2";
		doc2.value = "value-doc2";

		Bulk bulk = Bulk
				.create(builder -> builder
								.inCollection(BaseDoc.class,
										ops -> ops.insert(doc1).insert(doc2).upsert(where("_id").is("id-doc3"),
												new Update().set("value", "upserted")))
						.inCollection(SpecialDoc.class, it -> it.insert(new SpecialDoc())));

		operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		Long inBaseDocCollection = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long inSpecialCollection = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(inBaseDocCollection).isEqualTo(3L);
		assertThat(inSpecialCollection).isOne();
	}

	@Test // GH-5087
	void insertOrderedAcrossCollections() {

		BaseDoc doc1 = newDoc("1");
		BaseDoc doc2 = newDoc("2");
		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";
		specialDoc.value = "value-special";

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc2))
				.inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.insertCount()).isEqualTo(3);

		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isEqualTo(2L);
		assertThat(specialCount).isOne();
	}

	@Test // GH-5087
	void insertOrderedFailsStopsAtDuplicateInCollection() {

		BaseDoc doc1 = newDoc("1");
		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc1))
				.inCollection(SpecialDoc.class, it -> it.insert(new SpecialDoc())).build();

		assertThatThrownBy(() -> operations.bulkWrite(bulk, BulkWriteOptions.ordered())) //
				// .isInstanceOf(BulkOperationException.class) // TODO
				.hasCauseInstanceOf(ClientBulkWriteException.class) //
				.extracting(Throwable::getCause) //
				.satisfies(it -> {
					ClientBulkWriteException ex = (ClientBulkWriteException) it;
					assertThat(ex.getPartialResult().get().getInsertedCount()).isOne();
					assertThat(ex.getWriteErrors()).isNotNull();
					assertThat(ex.getWriteErrors().size()).isOne();
				});

		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isOne();
		assertThat(specialCount).isZero();
	}

	@Test // GH-5087
	void insertUnOrderedAcrossCollections() {

		BaseDoc doc1 = newDoc("1");
		BaseDoc doc2 = newDoc("2");
		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc2))
				.inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.unordered());

		assertThat(result.insertCount()).isEqualTo(3);
		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isEqualTo(2L);
		assertThat(specialCount).isOne();
	}

	@Test // GH-5087
	void insertUnOrderedContinuesOnErrorInOneCollection() {

		BaseDoc doc1 = newDoc("1");
		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc1))
				.inCollection(SpecialDoc.class, it -> it.insert(new SpecialDoc())).build();

		assertThatThrownBy(() -> operations.bulkWrite(bulk, BulkWriteOptions.unordered())) //
				// .isInstanceOf(BulkOperationException.class) // TODO
				.hasCauseInstanceOf(ClientBulkWriteException.class) //
				.extracting(Throwable::getCause) //
				.satisfies(it -> {
					ClientBulkWriteException ex = (ClientBulkWriteException) it;
					assertThat(ex.getPartialResult().get().getInsertedCount()).isEqualTo(2);
					assertThat(ex.getWriteErrors()).isNotNull();
					assertThat(ex.getWriteErrors().size()).isOne();
				});

		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isOne();
		assertThat(specialCount).isOne();
	}

	@Test // GH-5087
	void updateOneAcrossCollections() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		List<Pair<Query, UpdateDefinition>> updatesBase = Arrays
				.asList(Pair.of(queryWhere("value", "value1"), set("value", "value3")));
		List<Pair<Query, UpdateDefinition>> updatesSpecial = Arrays
				.asList(Pair.of(queryWhere("value", "value1"), set("value", "value3")));

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> updatesBase.forEach(p -> ops.updateOne(p.getFirst(), p.getSecond())))
				.inCollection(SpecialDoc.class, ops -> updatesSpecial.forEach(p -> ops.updateOne(p.getFirst(), p.getSecond())))
				.build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.modifiedCount()).isEqualTo(2);

		Long baseWithValue3 = operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value3")));
		Long specialWithValue3 = operations.execute(SpecialDoc.class,
				col -> col.countDocuments(new Document("value", "value3")));
		assertThat(baseWithValue3).isEqualTo(1L);
		assertThat(specialWithValue3).isEqualTo(1L);
	}

	@Test // GH-5087
	void updateOneObject() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> ops.replaceIfExists(queryWhere("value", "value1"), new BaseDoc("value3")))
				.inCollection(SpecialDoc.class,
						ops -> ops.replaceIfExists(queryWhere("value", "value2"), new SpecialDoc("value3")))
				.build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.modifiedCount()).isEqualTo(2);

		Long baseWithValue3 = operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value3")));
		Long specialWithValue3 = operations.execute(SpecialDoc.class,
				col -> col.countDocuments(new Document("value", "value3")));
		assertThat(baseWithValue3).isEqualTo(1L);
		assertThat(specialWithValue3).isEqualTo(1L);
	}

	@Test // GH-5087
	void updateMultiAcrossCollections() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		List<Pair<Query, UpdateDefinition>> updatesBase = Arrays.asList(
				Pair.of(queryWhere("value", "value1"), set("value", "value3")),
				Pair.of(queryWhere("value", "value2"), set("value", "value4")));
		List<Pair<Query, UpdateDefinition>> updatesSpecial = Arrays.asList(
				Pair.of(queryWhere("value", "value1"), set("value", "value3")),
				Pair.of(queryWhere("value", "value2"), set("value", "value4")));

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> updatesBase.forEach(p -> ops.updateMulti(p.getFirst(), p.getSecond())))
				.inCollection(SpecialDoc.class,
						ops -> updatesSpecial.forEach(p -> ops.updateMulti(p.getFirst(), p.getSecond())))
				.build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.modifiedCount()).isEqualTo(8);

		Long baseValue3 = operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value3")));
		Long baseValue4 = operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value4")));
		Long specialValue3 = operations.execute(SpecialDoc.class,
				col -> col.countDocuments(new Document("value", "value3")));
		Long specialValue4 = operations.execute(SpecialDoc.class,
				col -> col.countDocuments(new Document("value", "value4")));
		assertThat(baseValue3).isEqualTo(2L);
		assertThat(baseValue4).isEqualTo(2L);
		assertThat(specialValue3).isEqualTo(2L);
		assertThat(specialValue4).isEqualTo(2L);
	}

	@Test // GH-5087
	void upsertDoesUpdateInEachCollection() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> ops.upsert(queryWhere("value", "value1"), set("value", "value2")))
				.inCollection(SpecialDoc.class, ops -> ops.upsert(queryWhere("value", "value1"), set("value", "value2")))
				.build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.matchedCount()).isEqualTo(4);
		assertThat(result.modifiedCount()).isEqualTo(4);
		assertThat(result.insertCount()).isZero();
		assertThat(result.upsertCount()).isZero();
	}

	@Test // GH-5087
	void upsertDoesInsertInEachCollection() {

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> ops.upsert(queryWhere("_id", "new-id-1"), set("value", "upserted1")))
				.inCollection(SpecialDoc.class, ops -> ops.upsert(queryWhere("_id", "new-id-2"), set("value", "upserted2")))
				.build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.matchedCount()).isZero();
		assertThat(result.modifiedCount()).isZero();
		assertThat(result.upsertCount()).isEqualTo(2);

		assertThat(operations.findOne(queryWhere("_id", "new-id-1"), BaseDoc.class)).isNotNull();
		assertThat(operations.findOne(queryWhere("_id", "new-id-2"), SpecialDoc.class)).isNotNull();
	}

	@Test // GH-5087
	void removeAcrossCollections() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		List<Query> removesBase = Arrays.asList(queryWhere("_id", "1"), queryWhere("value", "value2"));
		List<Query> removesSpecial = Arrays.asList(queryWhere("_id", "1"), queryWhere("value", "value2"));

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> removesBase.forEach(ops::remove))
				.inCollection(SpecialDoc.class, ops -> removesSpecial.forEach(ops::remove)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.deleteCount()).isEqualTo(6);

		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isOne();
		assertThat(specialCount).isOne();
	}

	@Test // GH-5087
	void replaceOneAcrossCollections() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		Document replacementBase = rawDoc("1", "replaced-base");
		Document replacementSpecial = new Document("_id", "1").append("value", "replaced-special").append("specialValue",
				"special");

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> ops.replaceOne(queryWhere("_id", "1"), replacementBase))
				.inCollection(SpecialDoc.class, ops -> ops.replaceOne(queryWhere("_id", "1"), replacementSpecial)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.matchedCount()).isEqualTo(2);
		assertThat(result.modifiedCount()).isEqualTo(2);

		Document inBase = operations.execute(BaseDoc.class, col -> col.find(new Document("_id", "1")).first());
		Document inSpecial = operations.execute(SpecialDoc.class, col -> col.find(new Document("_id", "1")).first());
		assertThat(inBase).containsEntry("value", "replaced-base");
		assertThat(inSpecial).containsEntry("value", "replaced-special").containsEntry("specialValue", "special");
	}

	@Test // GH-5087
	void replaceOneWithUpsertInCollection() {

		Document replacement = rawDoc("new-id", "upserted-value");

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, ops -> ops.replaceOne(queryWhere("_id", "new-id"), replacement)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.matchedCount()).isZero();
		assertThat(result.modifiedCount()).isZero();
		assertThat(result.upsertCount()).isOne();

		assertThat(operations.findOne(queryWhere("_id", "new-id"), BaseDoc.class)).isNotNull();
	}

	@Test // GH-5087
	void mixedBulkOrderedAcrossCollections() {

		BaseDoc doc1 = newDoc("1", "v1");
		SpecialDoc doc2 = new SpecialDoc();
		doc2.id = "2";
		doc2.value = "v2";

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class,
				ops -> ops.insert(doc1).updateOne(queryWhere("_id", "1"), set("value", "v2")).remove(queryWhere("value", "v2"))) //
				.inCollection(SpecialDoc.class, it -> it.insert(doc2)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.insertCount()).isEqualTo(2);
		assertThat(result.modifiedCount()).isOne();
		assertThat(result.deleteCount()).isOne();

		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isZero();
		assertThat(specialCount).isOne();
	}

	@Test // GH-5087
	void mixedBulkOrderedWithListAcrossCollections() {

		List<BaseDoc> insertsBase = Arrays.asList(newDoc("1", "v1"), newDoc("2", "v2"), newDoc("3", "v2"));
		List<Pair<Query, UpdateDefinition>> updatesBase = Arrays
				.asList(Pair.of(queryWhere("value", "v2"), set("value", "v3")));
		List<Query> removesBase = Arrays.asList(queryWhere("_id", "1"));

		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "s1";
		specialDoc.value = "sv1";

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> {
			ops.insertAll(insertsBase);
			updatesBase.forEach(p -> ops.updateMulti(p.getFirst(), p.getSecond()));
			removesBase.forEach(ops::remove);
		}).inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		BulkWriteResult result = operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		assertThat(result.insertCount()).isEqualTo(4);
		assertThat(result.modifiedCount()).isEqualTo(2);
		assertThat(result.deleteCount()).isOne();

		Long baseCount = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long specialCount = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(baseCount).isEqualTo(2L);
		assertThat(specialCount).isOne();
	}

	@Test // GH-5087
	void insertShouldConsiderInheritancePerCollection() {

		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";
		specialDoc.value = "normal-value";
		specialDoc.specialValue = "special-value";

		Bulk bulk = Bulk.builder().inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered());

		BaseDoc doc = operations.findOne(queryWhere("_id", specialDoc.id), BaseDoc.class,
				operations.getCollectionName(SpecialDoc.class));
		assertThat(doc).isNotNull();
		assertThat(doc).isInstanceOf(SpecialDoc.class);
	}

	@Test // GH-5087
	void switchingDatabasesBackAndForth/* srly, why? */() {

		mongoClient.getDatabase(operations.getDb().getName()).drop();
		mongoClient.getDatabase("bulk-ops-db-2").drop();
		mongoClient.getDatabase("bulk-ops-db-3").drop();

		Bulk bulk = Bulk.builder().inCollection("c1", it -> it.insert(newDoc("c1-id-1", "v1"))).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered());
		mongoClient.getDatabase("bulk-ops-db-2").getCollection("c1").insertOne(rawDoc("c1-id-1", "v1"));
		mongoClient.getDatabase("bulk-ops-db-3").getCollection("c1").insertOne(rawDoc("c1-id-1", "v1"));

		Document inDefaultDB = mongoClient.getDatabase(operations.getDb().getName()).getCollection("c1")
				.find(new Document("_id", "c1-id-1")).first();
		Document inDB2 = mongoClient.getDatabase("bulk-ops-db-2").getCollection("c1").find(new Document("_id", "c1-id-1"))
				.first();
		Document inDB3 = mongoClient.getDatabase("bulk-ops-db-3").getCollection("c1").find(new Document("_id", "c1-id-1"))
				.first();
		assertThat(inDefaultDB).isNotNull();
		assertThat(inDB2).isNotNull();
		assertThat(inDB3).isNotNull();
	}

	private void insertSomeDocumentsIntoBaseDoc() {
		String coll = operations.getCollectionName(BaseDoc.class);
		operations.execute(coll, col -> {
			col.insertOne(rawDoc("1", "value1"));
			col.insertOne(rawDoc("2", "value1"));
			col.insertOne(rawDoc("3", "value2"));
			col.insertOne(rawDoc("4", "value2"));
			return null;
		});
	}

	private void insertSomeDocumentsIntoSpecialDoc() {
		String coll = operations.getCollectionName(SpecialDoc.class);
		operations.execute(coll, col -> {
			col.insertOne(rawDoc("1", "value1"));
			col.insertOne(rawDoc("2", "value1"));
			col.insertOne(rawDoc("3", "value2"));
			col.insertOne(rawDoc("4", "value2"));
			return null;
		});
	}

	private static BaseDoc newDoc(String id) {
		BaseDoc doc = new BaseDoc();
		doc.id = id;
		return doc;
	}

	private static BaseDoc newDoc(String id, String value) {
		BaseDoc doc = newDoc(id);
		doc.value = value;
		return doc;
	}

	private static Query queryWhere(String field, String value) {
		return new Query(org.springframework.data.mongodb.core.query.Criteria.where(field).is(value));
	}

	private static Update set(String field, String value) {
		return new Update().set(field, value);
	}

	private static Document rawDoc(String id, String value) {
		return new Document("_id", id).append("value", value);
	}

}
