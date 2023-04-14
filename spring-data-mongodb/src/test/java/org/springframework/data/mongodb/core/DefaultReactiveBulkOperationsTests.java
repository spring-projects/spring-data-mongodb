/*
 * Copyright 2023 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultReactiveBulkOperations.ReactiveBulkOperationContext;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;

/**
 * Tests for {@link DefaultReactiveBulkOperations}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoTemplateExtension.class)
class DefaultReactiveBulkOperationsTests {

	static final String COLLECTION_NAME = "reactive-bulk-ops";

	@Template(initialEntitySet = BaseDoc.class) static ReactiveMongoTestTemplate template;

	@BeforeEach
	public void setUp() {
		template.flush(COLLECTION_NAME).as(StepVerifier::create).verifyComplete();
	}

	@Test // GH-2821
	void insertOrdered() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("2"));

		createBulkOps(BulkMode.ORDERED).insert(documents) //
				.execute().as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result.getInsertedCount()).isEqualTo(2);
				}).verifyComplete();
	}

	@Test // GH-2821
	void insertOrderedFails() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("1"), newDoc("2"));

		createBulkOps(BulkMode.ORDERED).insert(documents) //
				.execute().as(StepVerifier::create) //
				.verifyErrorSatisfies(error -> {
					assertThat(error).isInstanceOf(DuplicateKeyException.class);
				});
	}

	@Test // GH-2821
	public void insertUnOrdered() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("2"));

		createBulkOps(BulkMode.UNORDERED).insert(documents) //
				.execute().as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result.getInsertedCount()).isEqualTo(2);
				}).verifyComplete();
	}

	@Test // GH-2821
	public void insertUnOrderedContinuesOnError() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("1"), newDoc("2"));

		createBulkOps(BulkMode.UNORDERED).insert(documents) //
				.execute().as(StepVerifier::create) //
				.verifyErrorSatisfies(error -> {

					assertThat(error).isInstanceOf(DuplicateKeyException.class);
					assertThat(error.getCause()).isInstanceOf(MongoBulkWriteException.class);

					MongoBulkWriteException cause = (MongoBulkWriteException) error.getCause();
					assertThat(cause.getWriteResult().getInsertedCount()).isEqualTo(2);
					assertThat(cause.getWriteErrors()).isNotNull();
					assertThat(cause.getWriteErrors().size()).isOne();
				});
	}

	@Test // GH-2821
	void upsertDoesUpdate() {

		insertSomeDocuments();

		createBulkOps(BulkMode.ORDERED).//
				upsert(where("value", "value1"), set("value", "value2")).//
				execute().as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).isNotNull();
					assertThat(result.getMatchedCount()).isEqualTo(2);
					assertThat(result.getModifiedCount()).isEqualTo(2);
					assertThat(result.getInsertedCount()).isZero();
					assertThat(result.getUpserts()).isNotNull();
					assertThat(result.getUpserts().size()).isZero();
				}) //
				.verifyComplete();
	}

	@Test // GH-2821
	public void upsertDoesInsert() {

		createBulkOps(BulkMode.ORDERED).//
				upsert(where("_id", "1"), set("value", "v1")).//
				execute().as(StepVerifier::create) //
				.consumeNextWith(result -> {

					assertThat(result).isNotNull();
					assertThat(result.getMatchedCount()).isZero();
					assertThat(result.getModifiedCount()).isZero();
					assertThat(result.getUpserts()).isNotNull();
					assertThat(result.getUpserts().size()).isOne();
				}) //
				.verifyComplete();
	}

	@ParameterizedTest // GH-2821
	@MethodSource
	public void testUpdates(BulkMode mode, boolean multi, int expectedUpdateCount) {

		insertSomeDocuments();
		ReactiveBulkOperations bulkOps = createBulkOps(mode);

		if (multi) {
			bulkOps.updateMulti(where("value", "value1"), set("value", "value3"));
			bulkOps.updateMulti(where("value", "value2"), set("value", "value4"));
		} else {
			bulkOps.updateOne(where("value", "value1"), set("value", "value3"));
			bulkOps.updateOne(where("value", "value2"), set("value", "value4"));
		}

		bulkOps.execute().map(BulkWriteResult::getModifiedCount) //
				.as(StepVerifier::create) //
				.expectNext(expectedUpdateCount) //
				.verifyComplete();
	}

	private static Stream<Arguments> testUpdates() {
		return Stream.of(Arguments.of(BulkMode.ORDERED, false, 2), Arguments.of(BulkMode.ORDERED, true, 4),
				Arguments.of(BulkMode.UNORDERED, false, 2), Arguments.of(BulkMode.UNORDERED, false, 2));
	}

	@ParameterizedTest // GH-2821
	@EnumSource(BulkMode.class)
	void testRemove(BulkMode mode) {

		insertSomeDocuments();

		List<Query> removes = Arrays.asList(where("_id", "1"), where("value", "value2"));

		createBulkOps(mode).remove(removes).execute().map(BulkWriteResult::getDeletedCount).as(StepVerifier::create)
				.expectNext(3).verifyComplete();
	}

	@ParameterizedTest // GH-2821
	@EnumSource(BulkMode.class)
	void testReplaceOne(BulkMode mode) {

		insertSomeDocuments();

		Query query = where("_id", "1");
		Document document = rawDoc("1", "value2");
		createBulkOps(mode).replaceOne(query, document).execute().map(BulkWriteResult::getModifiedCount)
				.as(StepVerifier::create).expectNext(1).verifyComplete();
	}

	@Test // GH-2821
	public void replaceOneDoesReplace() {

		insertSomeDocuments();

		createBulkOps(BulkMode.ORDERED).//
				replaceOne(where("_id", "1"), rawDoc("1", "value2")).//
				execute().as(StepVerifier::create).consumeNextWith(result -> {

					assertThat(result).isNotNull();
					assertThat(result.getMatchedCount()).isOne();
					assertThat(result.getModifiedCount()).isOne();
					assertThat(result.getInsertedCount()).isZero();
				}).verifyComplete();
	}

	@Test // GH-2821
	public void replaceOneWithUpsert() {

		createBulkOps(BulkMode.ORDERED).//
				replaceOne(where("_id", "1"), rawDoc("1", "value2"), FindAndReplaceOptions.options().upsert()).//
				execute().as(StepVerifier::create).consumeNextWith(result -> {

					assertThat(result).isNotNull();
					assertThat(result.getMatchedCount()).isZero();
					assertThat(result.getInsertedCount()).isZero();
					assertThat(result.getModifiedCount()).isZero();
					assertThat(result.getUpserts().size()).isOne();
				});
	}

	@Test // GH-2821
	public void mixedBulkOrdered() {

		createBulkOps(BulkMode.ORDERED, BaseDoc.class).insert(newDoc("1", "v1")).//
				updateOne(where("_id", "1"), set("value", "v2")).//
				remove(where("value", "v2")).//
				execute().as(StepVerifier::create).consumeNextWith(result -> {

					assertThat(result).isNotNull();
					assertThat(result.getInsertedCount()).isOne();
					assertThat(result.getModifiedCount()).isOne();
					assertThat(result.getDeletedCount()).isOne();
				}).verifyComplete();
	}

	@Test // GH-2821
	public void mixedBulkOrderedWithList() {

		List<BaseDoc> inserts = Arrays.asList(newDoc("1", "v1"), newDoc("2", "v2"), newDoc("3", "v2"));
		List<Query> removes = Arrays.asList(where("_id", "1"));

		createBulkOps(BulkMode.ORDERED, BaseDoc.class).insert(inserts).updateMulti(where("value", "v2"), set("value", "v3"))
				.remove(removes).execute().as(StepVerifier::create).consumeNextWith(result -> {

					assertThat(result).isNotNull();
					assertThat(result.getInsertedCount()).isEqualTo(3);
					assertThat(result.getModifiedCount()).isEqualTo(2);
					assertThat(result.getDeletedCount()).isOne();
				}).verifyComplete();
	}

	@Test // GH-2821
	public void insertShouldConsiderInheritance() {

		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";
		specialDoc.value = "normal-value";
		specialDoc.specialValue = "special-value";

		createBulkOps(BulkMode.ORDERED, SpecialDoc.class).insert(Arrays.asList(specialDoc)).execute().then()
				.as(StepVerifier::create).verifyComplete();

		template.findOne(where("_id", specialDoc.id), BaseDoc.class, COLLECTION_NAME).as(StepVerifier::create)
				.consumeNextWith(doc -> {

					assertThat(doc).isNotNull();
					assertThat(doc).isInstanceOf(SpecialDoc.class);
				}).verifyComplete();
	}

	private void insertSomeDocuments() {

		template.execute(COLLECTION_NAME, collection -> {
			return Flux.from(collection.insertMany(
					List.of(rawDoc("1", "value1"), rawDoc("2", "value1"), rawDoc("3", "value2"), rawDoc("4", "value2"))));
		}).then().as(StepVerifier::create).verifyComplete();

	}

	private DefaultReactiveBulkOperations createBulkOps(BulkMode mode) {
		return createBulkOps(mode, null);
	}

	private DefaultReactiveBulkOperations createBulkOps(BulkMode mode, Class<?> entityType) {

		Optional<? extends MongoPersistentEntity<?>> entity = entityType != null
				? Optional.of(template.getConverter().getMappingContext().getPersistentEntity(entityType))
				: Optional.empty();

		ReactiveBulkOperationContext bulkOperationContext = new ReactiveBulkOperationContext(mode, entity,
				new QueryMapper(template.getConverter()), new UpdateMapper(template.getConverter()), null, null);

		DefaultReactiveBulkOperations bulkOps = new DefaultReactiveBulkOperations(template, COLLECTION_NAME,
				bulkOperationContext);
		bulkOps.setDefaultWriteConcern(WriteConcern.ACKNOWLEDGED);

		return bulkOps;
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

	private static Query where(String field, String value) {
		return new Query().addCriteria(Criteria.where(field).is(value));
	}

	private static Update set(String field, String value) {
		return new Update().set(field, value);
	}

	private static Document rawDoc(String id, String value) {
		return new Document("_id", id).append("value", value);
	}
}
