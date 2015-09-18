/*
 * Copyright 2014-2015 the original author or authors.
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

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.springframework.data.mongodb.core.query.Criteria.where;

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
	
	@Autowired MongoTemplate template;
	BulkOperations bulkOps;
	DBCollection collection;

	@Before
	public void setUp() {

		this.collection = this.template.getDb().getCollection(COLLECTION_NAME);
		this.collection.remove(new BasicDBObject());

		this.bulkOps = new DefaultBulkOperations(template, COLLECTION_NAME, WriteConcern.ACKNOWLEDGED);
	}

	@Test
	public void insertOrderedShouldComplete() {
		// given
		List<BaseDoc> documents = Arrays.asList( newDoc("1"), newDoc("2") );
		
		// when
		int n = bulkOps.insert(BulkMode.ORDERED, documents);
		
		// then
		assertThat(n, is(2));
	}

	@Test
	public void insertOrderedShouldFailOnError() {
		// given
		List<BaseDoc> documents = Arrays.asList( newDoc("1"), newDoc("1"), newDoc("2") );
		
		// when
		try {
			bulkOps.insert(BulkMode.ORDERED, documents);
			fail();
		} catch (BulkOperationException e) {
			// then
			assertThat(e.getResult().getInsertedCount(), is(1)); // fails after first error
			assertThat(e.getErrors(), notNullValue());
			assertThat(e.getErrors().size(), is(1));
		}
	}
	
	@Test
	public void insertUnOrderedShouldComplete() {
		// given
		List<BaseDoc> documents = Arrays.asList( newDoc("1"), newDoc("2") );
		
		// when
		int n = bulkOps.insert(BulkMode.UNORDERED, documents);
		
		// then
		assertThat(n, is(2));
	}

	@Test
	public void insertUnOrderedShouldContinueOnError() {
		// given
		List<BaseDoc> documents = Arrays.asList( newDoc("1"), newDoc("1"), newDoc("2") );
		
		// when
		try {
			bulkOps.insert(BulkMode.UNORDERED, documents);
			fail();
		} catch (BulkOperationException e) {
			// then
			assertThat(e.getResult().getInsertedCount(), is(2)); // two docs were inserted
			assertThat(e.getErrors(), notNullValue());
			assertThat(e.getErrors().size(), is(1));
		}
		
	}
	
	@Test
	public void updateOneOrderedShouldComplete() {
		testUpdate(BulkMode.ORDERED, false, 2);
	}	

	@Test
	public void updateMultiOrderedShouldComplete() {
		testUpdate(BulkMode.ORDERED, true, 4);		
	}	

	@Test
	public void updateOneUnOrderedShouldComplete() {
		testUpdate(BulkMode.UNORDERED, false, 2);		
	}	

	@Test
	public void updateMultiUnOrderedShouldComplete() {
		testUpdate(BulkMode.UNORDERED, true, 4);		
	}	

	private void testUpdate(BulkMode mode, boolean multi, int expectedUpdates) {
		// given
		insertSomeDocuments();
		List<Tuple<Query, Update>> updates = new ArrayList<Tuple<Query, Update>>();
		updates.add( new Tuple<Query, Update>( where("value", "value1"), set("value", "value3") ) );
		updates.add( new Tuple<Query, Update>( where("value", "value2"), set("value", "value4") ) );
		
		// when
		int n;
		if (multi) {
			n = bulkOps.updateMulti(mode, updates);
		} else {
			n = bulkOps.updateOne(mode, updates);
		}
		
		// then
		assertThat(n, is(expectedUpdates));
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
		return new Query().addCriteria( Criteria.where(field).is(value));
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
