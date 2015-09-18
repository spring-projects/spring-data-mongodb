/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.Tuple;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * Integration tests for {@link DefaultBulkOperations}.
 * 
 * @author Tobias Trelle
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class DefaultBulkOperationsIntegrationTests {

	private static final String COLLECTION_NAME = "bulk_ops";

	@Autowired
	private MongoTemplate template;

	private DBCollection collection;
	private BulkOperations bulkOps;

	@Before
	public void setUp() {

		this.collection = this.template.getDb().getCollection(COLLECTION_NAME);
		this.collection.remove(new BasicDBObject());
	}

	@Test
	public void insertOrdered() {
		// given
		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("2"));
		bulkOps = createBulkOps(BulkMode.ORDERED);

		// when
		int n = bulkOps.insert(documents).executeBulk().getInsertedCount();

		// then
		assertThat(n, is(2));
	}

	@Test
	public void insertOrderedFails() {
		// given
		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("1"), newDoc("2"));
		bulkOps = createBulkOps(BulkMode.ORDERED);

		// when
		try {
			bulkOps.insert(documents).executeBulk();
			fail();
		} catch (BulkOperationException e) {
			// then
			assertThat(e.getResult().getInsertedCount(), is(1)); // fails after first error
			assertThat(e.getErrors(), notNullValue());
			assertThat(e.getErrors().size(), is(1));
		}
	}

	@Test
	public void insertUnOrdered() {
		// given
		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("2"));
		bulkOps = createBulkOps(BulkMode.UNORDERED);

		// when
		int n = bulkOps.insert(documents).executeBulk().getInsertedCount();

		// then
		assertThat(n, is(2));
	}

	@Test
	public void insertUnOrderedContinuesOnError() {
		// given
		List<BaseDoc> documents = Arrays.asList(newDoc("1"), newDoc("1"), newDoc("2"));
		bulkOps = createBulkOps(BulkMode.UNORDERED);

		// when
		try {
			bulkOps.insert(documents).executeBulk();
			fail();
		} catch (BulkOperationException e) {
			// then
			assertThat(e.getResult().getInsertedCount(), is(2)); // two docs were inserted
			assertThat(e.getErrors(), notNullValue());
			assertThat(e.getErrors().size(), is(1));
		}

	}

	@Test
	public void upsertDoesUpdate() {
		// given
		bulkOps = createBulkOps(BulkMode.ORDERED);
		insertSomeDocuments();

		// when
		BulkWriteResult result = bulkOps.upsert(where("value", "value1"), set("value", "value2")).executeBulk();

		// then
		assertThat(result, notNullValue());
		assertThat(result.getMatchedCount(), is(2));
		assertThat(result.getModifiedCount(), is(2));
		assertThat(result.getInsertedCount(), is(0));
		assertThat(result.getUpserts(), notNullValue());
		assertThat(result.getUpserts().size(), is(0));
	}

	@Test
	public void upsertDoesInsert() {
		// given
		bulkOps = createBulkOps(BulkMode.ORDERED);

		// when
		BulkWriteResult result = bulkOps.upsert(where("_id", "1"), set("value", "v1")).executeBulk();

		// then
		assertThat(result, notNullValue());
		assertThat(result.getMatchedCount(), is(0));
		assertThat(result.getModifiedCount(), is(0));
		assertThat(result.getUpserts(), notNullValue());
		assertThat(result.getUpserts().size(), is(1));
	}

	@Test
	public void updateOneOrdered() {
		testUpdate(BulkMode.ORDERED, false, 2);
	}

	@Test
	public void updateMultiOrdered() {
		testUpdate(BulkMode.ORDERED, true, 4);
	}

	@Test
	public void updateOneUnOrdered() {
		testUpdate(BulkMode.UNORDERED, false, 2);
	}

	@Test
	public void updateMultiUnOrdered() {
		testUpdate(BulkMode.UNORDERED, true, 4);
	}

	@Test
	public void removeOrdered() {
		testRemove(BulkMode.ORDERED);
	}

	@Test
	public void removeUnordered() {
		testRemove(BulkMode.UNORDERED);
	}

	/**
	 * If working on the same set of documents, only an ordered bulk operation will yield predictable results.
	 */
	@Test
	public void mixedBulkOrdered() {
		// given
		bulkOps = createBulkOps(BulkMode.ORDERED);

		// when
		BulkWriteResult result = bulkOps.insert(newDoc("1", "v1")).updateOne(where("_id", "1"), set("value", "v2"))
				.remove(where("value", "v2")).executeBulk();

		// then
		assertThat(result, notNullValue());
		assertThat(result.getInsertedCount(), is(1));
		assertThat(result.getModifiedCount(), is(1));
		assertThat(result.getRemovedCount(), is(1));
	}

	/**
	 * If working on the same set of documents, only an ordered bulk operation will yield predictable results.
	 */
	@Test
	public void mixedBulkOrderedWithList() {
		// given
		bulkOps = createBulkOps(BulkMode.ORDERED);
		List<BaseDoc> inserts = Arrays.asList(newDoc("1", "v1"), newDoc("2", "v2"), newDoc("3", "v2"));
		List<Tuple<Query, Update>> updates = new ArrayList<Tuple<Query, Update>>();
		updates.add(new Tuple<Query, Update>(where("value", "v2"), set("value", "v3")));
		List<Query> removes = Arrays.asList(where("_id", "1"));

		// when
		BulkWriteResult result = bulkOps.insert(inserts).updateMulti(updates).remove(removes).executeBulk();

		// then
		assertThat(result, notNullValue());
		assertThat(result.getInsertedCount(), is(3));
		assertThat(result.getModifiedCount(), is(2));
		assertThat(result.getRemovedCount(), is(1));
	}

	private void testUpdate(BulkMode mode, boolean multi, int expectedUpdates) {
		// given
		bulkOps = createBulkOps(mode);
		insertSomeDocuments();
		List<Tuple<Query, Update>> updates = new ArrayList<Tuple<Query, Update>>();
		updates.add(new Tuple<Query, Update>(where("value", "value1"), set("value", "value3")));
		updates.add(new Tuple<Query, Update>(where("value", "value2"), set("value", "value4")));

		// when
		int n;
		if (multi) {
			n = bulkOps.updateMulti(updates).executeBulk().getModifiedCount();
		} else {
			n = bulkOps.updateOne(updates).executeBulk().getModifiedCount();
		}

		// then
		assertThat(n, is(expectedUpdates));
	}

	private void testRemove(BulkMode mode) {
		// given
		bulkOps = createBulkOps(mode);
		insertSomeDocuments();
		List<Query> removes = Arrays.asList(where("_id", "1"), where("value", "value2"));

		// when
		int n = bulkOps.remove(removes).executeBulk().getRemovedCount();

		// then
		assertThat(n, is(3));
	}

	private BulkOperations createBulkOps(BulkMode mode) {
		return new DefaultBulkOperations(template, mode, COLLECTION_NAME, WriteConcern.ACKNOWLEDGED);
	}

	private void insertSomeDocuments() {
		final DBCollection coll = template.getCollection(COLLECTION_NAME);

		coll.insert(rawDoc("1", "value1"));
		coll.insert(rawDoc("2", "value1"));
		coll.insert(rawDoc("3", "value2"));
		coll.insert(rawDoc("4", "value2"));
	}

	private static BaseDoc newDoc(String id) {
		final BaseDoc doc = new BaseDoc();
		doc.id = id;

		return doc;
	}

	private static BaseDoc newDoc(String id, String value) {
		final BaseDoc doc = newDoc(id);

		doc.value = value;

		return doc;
	}

	private static Query where(String field, String value) {
		return new Query().addCriteria(Criteria.where(field).is(value));
	}

	private static Update set(String field, String value) {
		Update u = new Update();
		u.set(field, value);

		return u;
	}

	private static DBObject rawDoc(String id, String value) {
		final DBObject o = new BasicDBObject();

		o.put("_id", id);
		o.put("value", value);

		return o;
	}

}
