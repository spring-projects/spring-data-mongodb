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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.data.util.Pair;

import com.mongodb.ClientBulkWriteException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Reactive integration tests for {@link ReactiveMongoOperations#bulkWrite}.
 *
 * @author Christoph Strobl
 */
@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
class ReactiveMongoTemplateBulkTests {

	@Client private static MongoClient mongoClient;

	@Template(initialEntitySet = { BaseDoc.class, SpecialDoc.class }) private static ReactiveMongoTestTemplate operations;

	@BeforeEach
	void setUp() {
		operations.flushDatabase().block();
	}

	@Test // GH-5087
	void bulkWriteMultipleCollections() {

		operations.flushDatabase().block();

		BaseDoc doc1 = new BaseDoc();
		doc1.id = "id-doc1";
		doc1.value = "value-doc1";
		BaseDoc doc2 = new BaseDoc();
		doc2.id = "id-doc2";
		doc2.value = "value-doc2";

		Bulk bulk = Bulk.create(builder -> builder
				.inCollection(BaseDoc.class,
						ops -> ops.insert(doc1).insert(doc2).upsert(where("_id").is("id-doc3"),
								new Update().set("value", "upserted")))
				.inCollection(SpecialDoc.class, it -> it.insert(new SpecialDoc())));

		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(3L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
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
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(result -> result.insertCount() == 3).verifyComplete();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(2L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
	}

	@Test // GH-5087
	void insertOrderedFailsStopsAtDuplicateInCollection() {

		BaseDoc doc1 = newDoc("1");
		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc1))
				.inCollection(SpecialDoc.class, it -> it.insert(new SpecialDoc())).build();

		StepVerifier.create(operations.bulkWrite(bulk, BulkWriteOptions.ordered())).expectErrorMatches(throwable -> {
			if (!(throwable.getCause() instanceof ClientBulkWriteException))
				return false;
			ClientBulkWriteException ex = (ClientBulkWriteException) throwable.getCause();
			assertThat(ex.getPartialResult().get().getInsertedCount()).isOne();
			assertThat(ex.getWriteErrors()).isNotNull();
			assertThat(ex.getWriteErrors().size()).isOne();
			return true;
		}).verify();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(0L)
				.verifyComplete();
	}

	@Test // GH-5087
	void insertUnOrderedAcrossCollections() {

		BaseDoc doc1 = newDoc("1");
		BaseDoc doc2 = newDoc("2");
		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc2))
				.inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		operations.bulkWrite(bulk, BulkWriteOptions.unordered()).as(StepVerifier::create).expectNextMatches(result -> result.insertCount() == 3)
				.verifyComplete();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(2L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
	}

	@Test // GH-5087
	void insertUnOrderedContinuesOnErrorInOneCollection() {

		BaseDoc doc1 = newDoc("1");
		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, ops -> ops.insert(doc1).insert(doc1))
				.inCollection(SpecialDoc.class, it -> it.insert(new SpecialDoc())).build();

		StepVerifier.create(operations.bulkWrite(bulk, BulkWriteOptions.unordered())).expectErrorMatches(throwable -> {
			if (!(throwable.getCause() instanceof ClientBulkWriteException))
				return false;
			ClientBulkWriteException ex = (ClientBulkWriteException) throwable.getCause();
			assertThat(ex.getPartialResult().get().getInsertedCount()).isEqualTo(2);
			assertThat(ex.getWriteErrors()).isNotNull();
			assertThat(ex.getWriteErrors().size()).isOne();
			return true;
		}).verify();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
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
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(result -> result.modifiedCount() == 2).verifyComplete();

		operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value3")))
				.as(StepVerifier::create).expectNext(1L).verifyComplete();
		operations.execute(SpecialDoc.class, col -> col.countDocuments(new Document("value", "value3")))
				.as(StepVerifier::create).expectNext(1L).verifyComplete();
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
				.inCollection(BaseDoc.class, o -> updatesBase.forEach(p -> o.updateMulti(p.getFirst(), p.getSecond())))
				.inCollection(SpecialDoc.class, o -> updatesSpecial.forEach(p -> o.updateMulti(p.getFirst(), p.getSecond())))
				.build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(result -> result.modifiedCount() == 8).verifyComplete();

		operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value3")))
				.as(StepVerifier::create).expectNext(2L).verifyComplete();
		operations.execute(BaseDoc.class, col -> col.countDocuments(new Document("value", "value4")))
				.as(StepVerifier::create).expectNext(2L).verifyComplete();
		operations.execute(SpecialDoc.class, col -> col.countDocuments(new Document("value", "value3")))
				.as(StepVerifier::create).expectNext(2L).verifyComplete();
		operations.execute(SpecialDoc.class, col -> col.countDocuments(new Document("value", "value4")))
				.as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // GH-5087
	void upsertDoesUpdateInEachCollection() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, o -> o.upsert(queryWhere("value", "value1"), set("value", "value2")))
				.inCollection(SpecialDoc.class, o -> o.upsert(queryWhere("value", "value1"), set("value", "value2"))).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(result -> result.matchedCount() == 4 && result.modifiedCount() == 4
						&& result.insertCount() == 0 && result.upsertCount() == 0)
				.verifyComplete();
	}

	@Test // GH-5087
	void upsertDoesInsertInEachCollection() {

		Bulk bulk = Bulk.builder()
				.inCollection(BaseDoc.class, o -> o.upsert(queryWhere("_id", "new-id-1"), set("value", "upserted1")))
				.inCollection(SpecialDoc.class, o -> o.upsert(queryWhere("_id", "new-id-2"), set("value", "upserted2")))
				.build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(
						result -> result.matchedCount() == 0 && result.modifiedCount() == 0 && result.upsertCount() == 2)
				.verifyComplete();

		operations.findOne(queryWhere("_id", "new-id-1"), BaseDoc.class).as(StepVerifier::create)
				.expectNextMatches(doc -> doc != null).verifyComplete();
		operations.findOne(queryWhere("_id", "new-id-2"), SpecialDoc.class).as(StepVerifier::create)
				.expectNextMatches(doc -> doc != null).verifyComplete();
	}

	@Test // GH-5087
	void removeAcrossCollections() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		List<Query> removesBase = Arrays.asList(queryWhere("_id", "1"), queryWhere("value", "value2"));
		List<Query> removesSpecial = Arrays.asList(queryWhere("_id", "1"), queryWhere("value", "value2"));

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, o -> removesBase.forEach(o::remove))
				.inCollection(SpecialDoc.class, o -> removesSpecial.forEach(o::remove)).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(result -> result.deleteCount() == 6).verifyComplete();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
	}

	@Test // GH-5087
	void replaceOneAcrossCollections() {

		insertSomeDocumentsIntoBaseDoc();
		insertSomeDocumentsIntoSpecialDoc();

		Document replacementBase = rawDoc("1", "replaced-base");
		Document replacementSpecial = new Document("_id", "1").append("value", "replaced-special").append("specialValue",
				"special");

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, o -> o.replaceOne(queryWhere("_id", "1"), replacementBase))
				.inCollection(SpecialDoc.class, o -> o.replaceOne(queryWhere("_id", "1"), replacementSpecial)).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(result -> result.matchedCount() == 2 && result.modifiedCount() == 2).verifyComplete();

		operations.execute(BaseDoc.class, col -> Mono.from(col.find(new Document("_id", "1")).first()))
				.as(StepVerifier::create)
				.expectNextMatches(inBase -> inBase != null && "replaced-base".equals(inBase.get("value"))).verifyComplete();
		operations.execute(SpecialDoc.class, col -> Mono.from(col.find(new Document("_id", "1")).first()))
				.as(StepVerifier::create).expectNextMatches(inSpecial -> inSpecial != null
						&& "replaced-special".equals(inSpecial.get("value")) && "special".equals(inSpecial.get("specialValue")))
				.verifyComplete();
	}

	@Test // GH-5087
	void replaceOneWithUpsertInCollection() {

		Document replacement = rawDoc("new-id", "upserted-value");

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, o -> o.replaceOne(queryWhere("_id", "new-id"), replacement))
				.build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(
						result -> result.matchedCount() == 0 && result.modifiedCount() == 0 && result.upsertCount() == 1)
				.verifyComplete();

		operations.findOne(queryWhere("_id", "new-id"), BaseDoc.class).as(StepVerifier::create)
				.expectNextMatches(doc -> doc != null).verifyComplete();
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
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(
						result -> result.insertCount() == 2 && result.modifiedCount() == 1 && result.deleteCount() == 1)
				.verifyComplete();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(0L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
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

		Bulk bulk = Bulk.builder().inCollection(BaseDoc.class, o -> {
			o.insertAll(insertsBase);
			updatesBase.forEach(p -> o.updateMulti(p.getFirst(), p.getSecond()));
			removesBase.forEach(o::remove);
		}).inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create)
				.expectNextMatches(
						result -> result.insertCount() == 4 && result.modifiedCount() == 2 && result.deleteCount() == 1)
				.verifyComplete();

		operations.execute(BaseDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(2L)
				.verifyComplete();
		operations.execute(SpecialDoc.class, MongoCollection::countDocuments).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();
	}

	@Test // GH-5087
	void insertShouldConsiderInheritancePerCollection() {

		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";
		specialDoc.value = "normal-value";
		specialDoc.specialValue = "special-value";

		Bulk bulk = Bulk.builder().inCollection(SpecialDoc.class, it -> it.insert(specialDoc)).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		operations.findOne(queryWhere("_id", specialDoc.id), BaseDoc.class, operations.getCollectionName(SpecialDoc.class))
				.as(StepVerifier::create).expectNextMatches(doc -> doc != null && doc instanceof SpecialDoc).verifyComplete();
	}

	@Test // GH-5087
	void switchingDatabasesBackAndForth() {

		String dbName = operations.getMongoDatabase().map(db -> db.getName()).block();
		Mono.from(mongoClient.getDatabase(dbName).drop()).block();
		Mono.from(mongoClient.getDatabase("bulk-ops-db-2").drop()).block();
		Mono.from(mongoClient.getDatabase("bulk-ops-db-3").drop()).block();

		Bulk bulk = Bulk.builder().inCollection("c1", it -> it.insert(newDoc("c1-id-1", "v1"))).build();
		operations.bulkWrite(bulk, BulkWriteOptions.ordered()).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		Mono.from(mongoClient.getDatabase("bulk-ops-db-2").getCollection("c1").insertOne(rawDoc("c1-id-1", "v1"))).block();
		Mono.from(mongoClient.getDatabase("bulk-ops-db-3").getCollection("c1").insertOne(rawDoc("c1-id-1", "v1"))).block();

		operations.execute("c1", col -> Mono.from(col.find(new Document("_id", "c1-id-1")).first()))
				.as(StepVerifier::create).expectNextMatches(doc -> doc != null).verifyComplete();
		Mono.from(mongoClient.getDatabase("bulk-ops-db-2").getCollection("c1").find(new Document("_id", "c1-id-1")).first())
				.as(StepVerifier::create).expectNextMatches(doc -> doc != null).verifyComplete();
		Mono.from(mongoClient.getDatabase("bulk-ops-db-3").getCollection("c1").find(new Document("_id", "c1-id-1")).first())
				.as(StepVerifier::create).expectNextMatches(doc -> doc != null).verifyComplete();
	}

	private void insertSomeDocumentsIntoBaseDoc() {
		String coll = operations.getCollectionName(BaseDoc.class);
		operations.execute(coll,
				col -> Mono.from(col.insertOne(rawDoc("1", "value1"))).then(Mono.from(col.insertOne(rawDoc("2", "value1"))))
						.then(Mono.from(col.insertOne(rawDoc("3", "value2"))))
						.then(Mono.from(col.insertOne(rawDoc("4", "value2")))))
				.then().block();
	}

	private void insertSomeDocumentsIntoSpecialDoc() {
		String coll = operations.getCollectionName(SpecialDoc.class);
		operations.execute(coll,
				col -> Mono.from(col.insertOne(rawDoc("1", "value1"))).then(Mono.from(col.insertOne(rawDoc("2", "value1"))))
						.then(Mono.from(col.insertOne(rawDoc("3", "value2"))))
						.then(Mono.from(col.insertOne(rawDoc("4", "value2")))))
				.then().block();
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
