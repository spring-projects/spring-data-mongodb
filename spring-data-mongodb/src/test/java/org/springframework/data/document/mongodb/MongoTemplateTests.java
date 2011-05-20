/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.document.mongodb.query.Criteria.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.document.InvalidDocumentStoreApiUsageException;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.document.mongodb.mapping.MongoMappingContext;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Index;
import org.springframework.data.document.mongodb.query.Index.Duplicates;
import org.springframework.data.document.mongodb.query.Order;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.document.mongodb.query.Update;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoTemplateTests {

	@Autowired
	MongoTemplate template;

	MongoTemplate mappingTemplate;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Autowired
	@SuppressWarnings("unchecked")
	public void setMongo(Mongo mongo) throws Exception {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(PersonWith_idPropertyOfTypeObjectId.class,
				PersonWith_idPropertyOfTypeString.class, PersonWithIdPropertyOfTypeObjectId.class,
				PersonWithIdPropertyOfTypeString.class)));
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(mappingContext);
		converter.afterPropertiesSet();

		this.mappingTemplate = new MongoTemplate(template.getDbFactory(), converter);
	}

	@Before
	public void setUp() {
		template.dropCollection(template.getCollectionName(Person.class));
		template.dropCollection(template.getCollectionName(PersonWith_idPropertyOfTypeObjectId.class));
		template.dropCollection(template.getCollectionName(PersonWith_idPropertyOfTypeString.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfTypeObjectId.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfTypeString.class));
	}

	@Test
	public void insertsSimpleEntityCorrectly() throws Exception {

		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);

		MongoConverter converter = template.getConverter();

		List<Person> result = template.find(new Query(Criteria.where("_id").is(converter.convertObjectId(person.getId()))),
				Person.class);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(person));
	}

	@Test
	public void updateFailure() throws Exception {

		MongoTemplate mongoTemplate = new MongoTemplate(template.getDbFactory());
		mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person person = new Person("Oliver2");
		person.setAge(25);
		mongoTemplate.insert(person);

		Query q = new Query(Criteria.where("BOGUS").gt(22));
		Update u = new Update().set("firstName", "Sven");
		thrown.expect(DataIntegrityViolationException.class);
		thrown.expectMessage(endsWith("0 documents updated"));
		mongoTemplate.updateFirst(Person.class, q, u);

	}

	@Test
	public void testEnsureIndex() throws Exception {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1);
		Person p2 = new Person("Sven");
		p2.setAge(40);
		template.insert(p2);

		template.ensureIndex(Person.class, new Index().on("age", Order.DESCENDING).unique(Duplicates.DROP));

		DBCollection coll = template.getCollection(template.getCollectionName(Person.class));
		List<DBObject> indexInfo = coll.getIndexInfo();

		assertThat(indexInfo.size(), is(2));
		String indexKey = null;
		boolean unique = false;
		boolean dropDupes = false;
		for (DBObject ix : indexInfo) {
			if ("age_-1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
				unique = (Boolean) ix.get("unique");
				dropDupes = (Boolean) ix.get("dropDups");
			}
		}
		assertThat(indexKey, is("{ \"age\" : -1}"));
		assertThat(unique, is(true));
		assertThat(dropDupes, is(true));
	}

	@Test
	public void testProperHandlingOfDifferentIdTypesWithSimpleMongoConverter() throws Exception {
		testProperHandlingOfDifferentIdTypes(this.template);
	}

	@Test
	public void testProperHandlingOfDifferentIdTypesWithMappingMongoConverter() throws Exception {
		testProperHandlingOfDifferentIdTypes(this.mappingTemplate);
	}

	private void testProperHandlingOfDifferentIdTypes(MongoTemplate mongoTemplate) throws Exception {
		PersonWithIdPropertyOfTypeString p1 = new PersonWithIdPropertyOfTypeString();
		p1.setFirstName("Sven_1");
		p1.setAge(22);
		// insert
		mongoTemplate.insert(p1);
		// also try save
		mongoTemplate.save(p1);
		assertThat(p1.getId(), notNullValue());
		PersonWithIdPropertyOfTypeString p1q = mongoTemplate.findOne(new Query(where("id").is(p1.getId())),
				PersonWithIdPropertyOfTypeString.class);
		assertThat(p1q, notNullValue());
		assertThat(p1q.getId(), is(p1.getId()));

		PersonWithIdPropertyOfTypeString p2 = new PersonWithIdPropertyOfTypeString();
		p2.setFirstName("Sven_2");
		p2.setAge(22);
		p2.setId("TWO");
		// insert
		mongoTemplate.insert(p2);
		// also try save
		mongoTemplate.save(p2);
		assertThat(p2.getId(), notNullValue());
		PersonWithIdPropertyOfTypeString p2q = mongoTemplate.findOne(new Query(where("id").is(p2.getId())),
				PersonWithIdPropertyOfTypeString.class);
		assertThat(p2q, notNullValue());
		assertThat(p2q.getId(), is(p2.getId()));

		PersonWith_idPropertyOfTypeString p3 = new PersonWith_idPropertyOfTypeString();
		p3.setFirstName("Sven_3");
		p3.setAge(22);
		// insert
		mongoTemplate.insert(p3);
		// also try save
		mongoTemplate.save(p3);
		assertThat(p3.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeString p3q = mongoTemplate.findOne(new Query(where("_id").is(p3.get_id())),
				PersonWith_idPropertyOfTypeString.class);
		assertThat(p3q, notNullValue());
		assertThat(p3q.get_id(), is(p3.get_id()));

		PersonWith_idPropertyOfTypeString p4 = new PersonWith_idPropertyOfTypeString();
		p4.setFirstName("Sven_4");
		p4.setAge(22);
		p4.set_id("FOUR");
		// insert
		mongoTemplate.insert(p4);
		// also try save
		mongoTemplate.save(p4);
		assertThat(p4.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeString p4q = mongoTemplate.findOne(new Query(where("_id").is(p4.get_id())),
				PersonWith_idPropertyOfTypeString.class);
		assertThat(p4q, notNullValue());
		assertThat(p4q.get_id(), is(p4.get_id()));

		PersonWithIdPropertyOfTypeObjectId p5 = new PersonWithIdPropertyOfTypeObjectId();
		p5.setFirstName("Sven_5");
		p5.setAge(22);
		// insert
		mongoTemplate.insert(p5);
		// also try save
		mongoTemplate.save(p5);
		assertThat(p5.getId(), notNullValue());
		PersonWithIdPropertyOfTypeObjectId p5q = mongoTemplate.findOne(new Query(where("id").is(p5.getId())),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p5q, notNullValue());
		assertThat(p5q.getId(), is(p5.getId()));

		PersonWithIdPropertyOfTypeObjectId p6 = new PersonWithIdPropertyOfTypeObjectId();
		p6.setFirstName("Sven_6");
		p6.setAge(22);
		p6.setId(new ObjectId());
		// insert
		mongoTemplate.insert(p6);
		// also try save
		mongoTemplate.save(p6);
		assertThat(p6.getId(), notNullValue());
		PersonWithIdPropertyOfTypeObjectId p6q = mongoTemplate.findOne(new Query(where("id").is(p6.getId())),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p6q, notNullValue());
		assertThat(p6q.getId(), is(p6.getId()));

		PersonWith_idPropertyOfTypeObjectId p7 = new PersonWith_idPropertyOfTypeObjectId();
		p7.setFirstName("Sven_7");
		p7.setAge(22);
		// insert
		mongoTemplate.insert(p7);
		// also try save
		mongoTemplate.save(p7);
		assertThat(p7.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeObjectId p7q = mongoTemplate.findOne(new Query(where("_id").is(p7.get_id())),
				PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p7q, notNullValue());
		assertThat(p7q.get_id(), is(p7.get_id()));

		PersonWith_idPropertyOfTypeObjectId p8 = new PersonWith_idPropertyOfTypeObjectId();
		p8.setFirstName("Sven_8");
		p8.setAge(22);
		p8.set_id(new ObjectId());
		// insert
		mongoTemplate.insert(p8);
		// also try save
		mongoTemplate.save(p8);
		assertThat(p8.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeObjectId p8q = mongoTemplate.findOne(new Query(where("_id").is(p8.get_id())),
				PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p8q, notNullValue());
		assertThat(p8q.get_id(), is(p8.get_id()));
	}

	@Test
	public void testFindAndRemove() throws Exception {

		Message m1 = new Message("Hello Spring");
		template.insert(m1);
		Message m2 = new Message("Hello Mongo");
		template.insert(m2);

		Query q = new Query(Criteria.where("text").regex("^Hello.*"));
		Message found1 = template.findAndRemove(q, Message.class);
		Message found2 = template.findAndRemove(q, Message.class);
		// Message notFound = template.findAndRemove(q, Message.class);
		DBObject notFound = template.getCollection("").findAndRemove(q.getQueryObject());
		assertThat(found1, notNullValue());
		assertThat(found2, notNullValue());
		assertThat(notFound, nullValue());
	}

	@Test
	public void testUsingAnInQuery() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		Query q3 = new Query(Criteria.where("id").in(p3.getId()));
		List<PersonWithIdPropertyOfTypeObjectId> results3 = template.find(q3, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(1));
	}

	@Test
	public void testUsingInQueryWithList() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		List<Integer> l1 = new ArrayList<Integer>();
		l1.add(11);
		l1.add(21);
		l1.add(41);
		Query q1 = new Query(Criteria.where("age").in(l1));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("age").in(l1.toArray()));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(3));
		try {
			List<Integer> l2 = new ArrayList<Integer>();
			l2.add(31);
			Query q3 = new Query(Criteria.where("age").in(l1, l2));
			template.find(q3, PersonWithIdPropertyOfTypeObjectId.class);
			Assert.fail("Should have trown an InvalidDocumentStoreApiUsageException");
		} catch (InvalidDocumentStoreApiUsageException e) {
		}
	}

	@Test
	public void testUsingAnOrQuery() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21));
		Query q2 = new Query(Criteria.where("age").is(31));
		Query orQuery = new Query().or(q1, q2);
		List<PersonWithIdPropertyOfTypeObjectId> results = template.find(orQuery, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results.size(), is(3));
		for (PersonWithIdPropertyOfTypeObjectId p : results) {
			assertThat(p.getAge(), isOneOf(11, 21, 31));
		}
	}

	@Test
	public void testUsingUpdateWithMultipleSet() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);

		Update u = new Update().set("firstName", "Bob").set("age", 10);

		WriteResult wr = template.updateMulti(PersonWithIdPropertyOfTypeObjectId.class, new Query(), u);

		assertThat(wr.getN(), is(2));

		Query q1 = new Query(Criteria.where("age").in(11, 21));
		List<PersonWithIdPropertyOfTypeObjectId> r1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(r1.size(), is(0));
		Query q2 = new Query(Criteria.where("age").is(10));
		List<PersonWithIdPropertyOfTypeObjectId> r2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(r2.size(), is(2));
		for (PersonWithIdPropertyOfTypeObjectId p : r2) {
			assertThat(p.getAge(), is(10));
			assertThat(p.getFirstName(), is("Bob"));
		}
	}

	@Test
	public void testRemovingDocument() throws Exception {

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven_to_be_removed");
		p1.setAge(51);
		template.insert(p1);

		Query q1 = new Query(Criteria.where("id").is(p1.getId()));
		PersonWithIdPropertyOfTypeObjectId found1 = template.findOne(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(found1, notNullValue());
		Query _q = new Query(Criteria.where("_id").is(p1.getId()));
		template.remove(_q, PersonWithIdPropertyOfTypeObjectId.class);
		PersonWithIdPropertyOfTypeObjectId notFound1 = template.findOne(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(notFound1, nullValue());

		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Bubba_to_be_removed");
		p2.setAge(51);
		template.insert(p2);

		Query q2 = new Query(Criteria.where("id").is(p2.getId()));
		PersonWithIdPropertyOfTypeObjectId found2 = template.findOne(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(found2, notNullValue());
		template.remove(q2, PersonWithIdPropertyOfTypeObjectId.class);
		PersonWithIdPropertyOfTypeObjectId notFound2 = template.findOne(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(notFound2, nullValue());
	}

	@Test
	public void testAddingToListWithSimpleConverter() throws Exception {
		testAddingToList(this.template);
	}

	@Test
	public void testAddingToListWithMappingConverter() throws Exception {
		testAddingToList(this.mappingTemplate);
	}

	private void testAddingToList(MongoTemplate template) {
		PersonWithAList p = new PersonWithAList();
		p.setFirstName("Sven");
		p.setAge(22);
		template.insert(p);

		Query q1 = new Query(Criteria.where("id").is(p.getId()));
		PersonWithAList p2 = template.findOne(q1, PersonWithAList.class);
		assertThat(p2, notNullValue());
		assertThat(p2.getWishList().size(), is(0));

		p2.addToWishList("please work!");

		template.save(p2);

		PersonWithAList p3 = template.findOne(q1, PersonWithAList.class);
		assertThat(p3, notNullValue());
		assertThat(p3.getWishList().size(), is(1));

		Friend f = new Friend();
		p.setFirstName("Erik");
		p.setAge(21);

		p3.addFriend(f);
		template.save(p3);

		PersonWithAList p4 = template.findOne(q1, PersonWithAList.class);
		assertThat(p4, notNullValue());
		assertThat(p4.getWishList().size(), is(1));
		assertThat(p4.getFriends().size(), is(1));

	}

	@Test
	public void testUsingSlaveOk() throws Exception {
		this.template.execute("slaveOkTest", new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection)
					throws MongoException, DataAccessException {
				assertThat(collection.getOptions(), is(0));
				assertThat(collection.getDB().getOptions(), is(0));
				return null;
			}
		});
		MongoTemplate slaveTemplate = new MongoTemplate(this.template.getDbFactory());
		slaveTemplate.setSlaveOk(true);
		slaveTemplate.execute("slaveOkTest", new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection)
					throws MongoException, DataAccessException {
				assertThat(collection.getOptions(), is(4));
				assertThat(collection.getDB().getOptions(), is(0));
				return null;
			}
		});
	}

}
