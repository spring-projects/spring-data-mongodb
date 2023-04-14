/*
 * Copyright 2015-2023 the original author or authors.
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

import java.util.ArrayList;
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
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperations.BulkOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.data.util.Pair;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;

/**
 * Integration tests for {@link DefaultBulkOperations}.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Minsu Kim
 */
@ExtendWith(MongoTemplateExtension.class)
public class DefaultBulkOperationsIntegrationTests {

	static final String COLLECTION_NAME = "bulk_ops";

	@Template(initialEntitySet = BaseDoc.class) //
	static MongoTestTemplate operations;

	@BeforeEach
	public void setUp() {
		operations.flush(COLLECTION_NAME);
	}

	@Test // DATAMONGO-934
	public void rejectsNullMongoOperations() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DefaultBulkOperations(null, COLLECTION_NAME,
				new BulkOperationContext(BulkMode.ORDERED, Optional.empty(), null, null, null, null)));
	}

	@Test // DATAMONGO-934
	public void rejectsNullCollectionName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DefaultBulkOperations(operations, null,
				new BulkOperationContext(BulkMode.ORDERED, Optional.empty(), null, null, null, null)));
	}

	@Test // DATAMONGO-934
	public void rejectsEmptyCollectionName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DefaultBulkOperations(operations, "",
				new BulkOperationContext(BulkMode.ORDERED, Optional.empty(), null, null, null, null)));
	}

	@Test // DATAMONGO-934
	public void insertOrdered() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("2"));

		assertThat(createBulkOps(BulkMode.ORDERED).insert(documents).execute().getInsertedCount()).isEqualTo(2);
	}

	@Test // DATAMONGO-934, DATAMONGO-2285
	public void insertOrderedFails() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("1"), newDoc("2"));

		assertThatThrownBy(() -> createBulkOps(BulkMode.ORDERED).insert(documents).execute()) //
				.isInstanceOf(BulkOperationException.class) //
				.hasCauseInstanceOf(MongoBulkWriteException.class) //
				.extracting(Throwable::getCause) //
				.satisfies(it -> {

					MongoBulkWriteException ex = (MongoBulkWriteException) it;
					assertThat(ex.getWriteResult().getInsertedCount()).isOne();
					assertThat(ex.getWriteErrors()).isNotNull();
					assertThat(ex.getWriteErrors().size()).isOne();
				});
	}

	@Test // DATAMONGO-934
	public void insertUnOrdered() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("2"));

		assertThat(createBulkOps(BulkMode.UNORDERED).insert(documents).execute().getInsertedCount()).isEqualTo(2);
	}

	@Test // DATAMONGO-934, DATAMONGO-2285
	public void insertUnOrderedContinuesOnError() {

		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("1"), newDoc("2"));

		assertThatThrownBy(() -> createBulkOps(BulkMode.UNORDERED).insert(documents).execute()) //
				.isInstanceOf(BulkOperationException.class) //
				.hasCauseInstanceOf(MongoBulkWriteException.class) //
				.extracting(Throwable::getCause) //
				.satisfies(it -> {

					MongoBulkWriteException ex = (MongoBulkWriteException) it;
					assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(2);
					assertThat(ex.getWriteErrors()).isNotNull();
					assertThat(ex.getWriteErrors().size()).isOne();
				});
	}

	@ParameterizedTest // DATAMONGO-934, GH-3872
	@MethodSource("upsertArguments")
	void upsertDoesUpdate(UpdateDefinition update) {

		insertSomeDocuments();

		com.mongodb.bulk.BulkWriteResult result = createBulkOps(BulkMode.ORDERED).//
				upsert(where("value", "value1"), update).//
				execute();

		assertThat(result).isNotNull();
		assertThat(result.getMatchedCount()).isEqualTo(2);
		assertThat(result.getModifiedCount()).isEqualTo(2);
		assertThat(result.getInsertedCount()).isZero();
		assertThat(result.getUpserts()).isNotNull();
		assertThat(result.getUpserts().size()).isZero();
	}

	@ParameterizedTest // DATAMONGO-934, GH-3872
	@MethodSource("upsertArguments")
	void upsertDoesInsert(UpdateDefinition update) {

		com.mongodb.bulk.BulkWriteResult result = createBulkOps(BulkMode.ORDERED).//
				upsert(where("_id", "1"), update).//
				execute();

		assertThat(result).isNotNull();
		assertThat(result.getMatchedCount()).isZero();
		assertThat(result.getModifiedCount()).isZero();
		assertThat(result.getUpserts()).isNotNull();
		assertThat(result.getUpserts().size()).isOne();
	}

	@Test // DATAMONGO-934
	public void updateOneOrdered() {
		testUpdate(BulkMode.ORDERED, false, 2);
	}

	@Test // GH-3872
	public void updateOneWithAggregation() {

		insertSomeDocuments();

		BulkOperations bulkOps = createBulkOps(BulkMode.ORDERED);
		bulkOps.updateOne(where("value", "value1"), AggregationUpdate.update().set("value").toValue("value3"));
		BulkWriteResult result = bulkOps.execute();

		assertThat(result.getModifiedCount()).isEqualTo(1);
		assertThat(operations.<Long>execute(COLLECTION_NAME, collection -> collection.countDocuments(new org.bson.Document("value", "value3")))).isOne();
	}

	@Test // DATAMONGO-934
	public void updateMultiOrdered() {
		testUpdate(BulkMode.ORDERED, true, 4);
	}

	@Test // GH-3872
	public void updateMultiWithAggregation() {

		insertSomeDocuments();

		BulkOperations bulkOps = createBulkOps(BulkMode.ORDERED);
		bulkOps.updateMulti(where("value", "value1"), AggregationUpdate.update().set("value").toValue("value3"));
		BulkWriteResult result = bulkOps.execute();

		assertThat(result.getModifiedCount()).isEqualTo(2);
		assertThat(operations.<Long>execute(COLLECTION_NAME, collection -> collection.countDocuments(new org.bson.Document("value", "value3")))).isEqualTo(2);
	}

	@Test // DATAMONGO-934
	public void updateOneUnOrdered() {
		testUpdate(BulkMode.UNORDERED, false, 2);
	}

	@Test // DATAMONGO-934
	public void updateMultiUnOrdered() {
		testUpdate(BulkMode.UNORDERED, true, 4);
	}

	@Test // DATAMONGO-934
	public void removeOrdered() {
		testRemove(BulkMode.ORDERED);
	}

	@Test // DATAMONGO-934
	public void removeUnordered() {
		testRemove(BulkMode.UNORDERED);
	}

	@Test // DATAMONGO-2218
	public void replaceOneOrdered() {
		testReplaceOne(BulkMode.ORDERED);
	}

	@Test // DATAMONGO-2218
	public void replaceOneUnordered() {
		testReplaceOne(BulkMode.UNORDERED);
	}

	@Test // DATAMONGO-2218
	public void replaceOneDoesReplace() {

		insertSomeDocuments();

		com.mongodb.bulk.BulkWriteResult result = createBulkOps(BulkMode.ORDERED).//
				replaceOne(where("_id", "1"), rawDoc("1", "value2")).//
				execute();

		assertThat(result).isNotNull();
		assertThat(result.getMatchedCount()).isOne();
		assertThat(result.getModifiedCount()).isOne();
		assertThat(result.getInsertedCount()).isZero();
	}

	@Test // DATAMONGO-2218
	public void replaceOneWithUpsert() {

		com.mongodb.bulk.BulkWriteResult result = createBulkOps(BulkMode.ORDERED).//
				replaceOne(where("_id", "1"), rawDoc("1", "value2"), FindAndReplaceOptions.options().upsert()).//
				execute();

		assertThat(result).isNotNull();
		assertThat(result.getMatchedCount()).isZero();
		assertThat(result.getInsertedCount()).isZero();
		assertThat(result.getModifiedCount()).isZero();
		assertThat(result.getUpserts().size()).isOne();
	}

	/**
	 * If working on the same set of documents, only an ordered bulk operation will yield predictable results.
	 */
	@Test // DATAMONGO-934
	public void mixedBulkOrdered() {

		com.mongodb.bulk.BulkWriteResult result = createBulkOps(BulkMode.ORDERED, BaseDoc.class).insert(newDoc("1", "v1")).//
				updateOne(where("_id", "1"), set("value", "v2")).//
				remove(where("value", "v2")).//
				execute();

		assertThat(result).isNotNull();
		assertThat(result.getInsertedCount()).isOne();
		assertThat(result.getModifiedCount()).isOne();
		assertThat(result.getDeletedCount()).isOne();
	}

	/**
	 * If working on the same set of documents, only an ordered bulk operation will yield predictable results.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void mixedBulkOrderedWithList() {

		List<BaseDoc> inserts = Arrays.asList(newDoc("1", "v1"), newDoc("2", "v2"), newDoc("3", "v2"));
		List<Pair<Query, UpdateDefinition>> updates = Arrays.asList(Pair.of(where("value", "v2"), set("value", "v3")));
		List<Query> removes = Arrays.asList(where("_id", "1"));

		com.mongodb.bulk.BulkWriteResult result = createBulkOps(BulkMode.ORDERED, BaseDoc.class).insert(inserts)
				.updateMulti(updates).remove(removes).execute();

		assertThat(result).isNotNull();
		assertThat(result.getInsertedCount()).isEqualTo(3);
		assertThat(result.getModifiedCount()).isEqualTo(2);
		assertThat(result.getDeletedCount()).isOne();
	}

	@Test // DATAMONGO-1534
	public void insertShouldConsiderInheritance() {

		SpecialDoc specialDoc = new SpecialDoc();
		specialDoc.id = "id-special";
		specialDoc.value = "normal-value";
		specialDoc.specialValue = "special-value";

		createBulkOps(BulkMode.ORDERED, SpecialDoc.class).insert(Arrays.asList(specialDoc)).execute();

		BaseDoc doc = operations.findOne(where("_id", specialDoc.id), BaseDoc.class, COLLECTION_NAME);

		assertThat(doc).isNotNull();
		assertThat(doc).isInstanceOf(SpecialDoc.class);
	}

	private void testUpdate(BulkMode mode, boolean multi, int expectedUpdates) {

		BulkOperations bulkOps = createBulkOps(mode);

		insertSomeDocuments();

		List<Pair<Query, UpdateDefinition>> updates = new ArrayList<>();
		updates.add(Pair.of(where("value", "value1"), set("value", "value3")));
		updates.add(Pair.of(where("value", "value2"), set("value", "value4")));

		int modifiedCount = multi ? bulkOps.updateMulti(updates).execute().getModifiedCount()
				: bulkOps.updateOne(updates).execute().getModifiedCount();

		assertThat(modifiedCount).isEqualTo(expectedUpdates);
	}

	private void testRemove(BulkMode mode) {

		insertSomeDocuments();

		List<Query> removes = Arrays.asList(where("_id", "1"), where("value", "value2"));

		assertThat(createBulkOps(mode).remove(removes).execute().getDeletedCount()).isEqualTo(3);
	}

	private void testReplaceOne(BulkMode mode) {

		BulkOperations bulkOps = createBulkOps(mode);

		insertSomeDocuments();

		Query query = where("_id", "1");
		Document document = rawDoc("1", "value2");
		int modifiedCount = bulkOps.replaceOne(query, document).execute().getModifiedCount();

		assertThat(modifiedCount).isOne();
	}

	private BulkOperations createBulkOps(BulkMode mode) {
		return createBulkOps(mode, null);
	}

	private BulkOperations createBulkOps(BulkMode mode, Class<?> entityType) {

		Optional<? extends MongoPersistentEntity<?>> entity = entityType != null
				? Optional.of(operations.getConverter().getMappingContext().getPersistentEntity(entityType))
				: Optional.empty();

		BulkOperationContext bulkOperationContext = new BulkOperationContext(mode, entity,
				new QueryMapper(operations.getConverter()), new UpdateMapper(operations.getConverter()), null, null);

		DefaultBulkOperations bulkOps = new DefaultBulkOperations(operations, COLLECTION_NAME, bulkOperationContext);
		bulkOps.setDefaultWriteConcern(WriteConcern.ACKNOWLEDGED);

		return bulkOps;
	}

	private void insertSomeDocuments() {

		final MongoCollection<Document> coll = operations.getCollection(COLLECTION_NAME);

		coll.insertOne(rawDoc("1", "value1"));
		coll.insertOne(rawDoc("2", "value1"));
		coll.insertOne(rawDoc("3", "value2"));
		coll.insertOne(rawDoc("4", "value2"));
	}

	private static Stream<Arguments> upsertArguments() {
		return Stream.of(Arguments.of(set("value", "value2")), Arguments.of(AggregationUpdate.update().set("value").toValue("value2")));
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
