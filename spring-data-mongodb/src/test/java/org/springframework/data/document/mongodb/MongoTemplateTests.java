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
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.document.mongodb.convert.SimpleMongoConverter;
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

	MongoTemplate simpleTemplate;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Autowired
	@SuppressWarnings("unchecked")
	public void setMongo(Mongo mongo) throws Exception {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(PersonWith_idPropertyOfTypeObjectId.class,
				PersonWith_idPropertyOfTypeString.class, PersonWithIdPropertyOfTypeObjectId.class,
				PersonWithIdPropertyOfTypeString.class, PersonWithIdPropertyOfTypeInteger.class, 
				PersonWithIdPropertyOfPrimitiveInt.class, PersonWithIdPropertyOfTypeLong.class, 
				PersonWithIdPropertyOfPrimitiveLong.class)));
		mappingContext.afterPropertiesSet();

		MappingMongoConverter mappingConverter = new MappingMongoConverter(template.getDbFactory(), mappingContext);
		mappingConverter.afterPropertiesSet();
		this.mappingTemplate = new MongoTemplate(template.getDbFactory(), mappingConverter);
		
		SimpleMongoConverter simpleConverter = new SimpleMongoConverter();
		simpleConverter.afterPropertiesSet();
		this.simpleTemplate = new MongoTemplate(template.getDbFactory(), simpleConverter);
	}

	@Before
	public void setUp() {
		template.dropCollection(template.getCollectionName(Person.class));
		template.dropCollection(template.getCollectionName(PersonWith_idPropertyOfTypeObjectId.class));
		template.dropCollection(template.getCollectionName(PersonWith_idPropertyOfTypeString.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfTypeObjectId.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfTypeString.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfTypeInteger.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfPrimitiveInt.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfTypeLong.class));
		template.dropCollection(template.getCollectionName(PersonWithIdPropertyOfPrimitiveLong.class));
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
		mongoTemplate.updateFirst(q, u, Person.class);

	}

	@Test
	public void testEnsureIndex() throws Exception {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1);
		Person p2 = new Person("Sven");
		p2.setAge(40);
		template.insert(p2);

		template.ensureIndex(new Index().on("age", Order.DESCENDING).unique(Duplicates.DROP), Person.class);

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
		testProperHandlingOfDifferentIdTypes(this.simpleTemplate);
	}

	@Test
	public void testProperHandlingOfDifferentIdTypesWithMappingMongoConverter() throws Exception {
		testProperHandlingOfDifferentIdTypes(this.mappingTemplate);
	}

	private void testProperHandlingOfDifferentIdTypes(MongoTemplate mongoTemplate) throws Exception {
		
		// String id - generated
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
		checkCollectionContents(PersonWithIdPropertyOfTypeString.class, 1);

		// String id - provided
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
		checkCollectionContents(PersonWithIdPropertyOfTypeString.class, 2);

		// String _id - generated
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
		checkCollectionContents(PersonWith_idPropertyOfTypeString.class, 1);

		// String _id - provided
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
		checkCollectionContents(PersonWith_idPropertyOfTypeString.class, 2);

		// ObjectId id - generated
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
		checkCollectionContents(PersonWithIdPropertyOfTypeObjectId.class, 1);

		// ObjectId id - provided
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
		checkCollectionContents(PersonWithIdPropertyOfTypeObjectId.class, 2);

		// ObjectId _id - generated
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
		checkCollectionContents(PersonWith_idPropertyOfTypeObjectId.class, 1);

		// ObjectId _id - provided
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
		checkCollectionContents(PersonWith_idPropertyOfTypeObjectId.class, 2);

		// Integer id - provided
		PersonWithIdPropertyOfTypeInteger p9 = new PersonWithIdPropertyOfTypeInteger();
		p9.setFirstName("Sven_9");
		p9.setAge(22);
		p9.setId(Integer.valueOf(12345));
		// insert
		mongoTemplate.insert(p9);
		// also try save
		mongoTemplate.save(p9);
		assertThat(p9.getId(), notNullValue());
		PersonWithIdPropertyOfTypeInteger p9q = mongoTemplate.findOne(new Query(where("id").in(p9.getId())),
				PersonWithIdPropertyOfTypeInteger.class);
		assertThat(p9q, notNullValue());
		assertThat(p9q.getId(), is(p9.getId()));
		checkCollectionContents(PersonWithIdPropertyOfTypeInteger.class, 1);

		// int id - provided
		PersonWithIdPropertyOfPrimitiveInt p10 = new PersonWithIdPropertyOfPrimitiveInt();
		p10.setFirstName("Sven_10");
		p10.setAge(22);
		p10.setId(12345);
		// insert
		mongoTemplate.insert(p10);
		// also try save
		mongoTemplate.save(p10);
		assertThat(p10.getId(), notNullValue());
		PersonWithIdPropertyOfPrimitiveInt p10q = mongoTemplate.findOne(new Query(where("id").in(p10.getId())),
				PersonWithIdPropertyOfPrimitiveInt.class);
		assertThat(p10q, notNullValue());
		assertThat(p10q.getId(), is(p10.getId()));
		checkCollectionContents(PersonWithIdPropertyOfPrimitiveInt.class, 1);

		// Long id - provided
		PersonWithIdPropertyOfTypeLong p11 = new PersonWithIdPropertyOfTypeLong();
		p11.setFirstName("Sven_9");
		p11.setAge(22);
		p11.setId(Long.valueOf(12345L));
		// insert
		mongoTemplate.insert(p11);
		// also try save
		mongoTemplate.save(p11);
		assertThat(p11.getId(), notNullValue());
		PersonWithIdPropertyOfTypeLong p11q = mongoTemplate.findOne(new Query(where("id").in(p11.getId())),
				PersonWithIdPropertyOfTypeLong.class);
		assertThat(p11q, notNullValue());
		assertThat(p11q.getId(), is(p11.getId()));
		checkCollectionContents(PersonWithIdPropertyOfTypeLong.class, 1);

		// long id - provided
		PersonWithIdPropertyOfPrimitiveLong p12 = new PersonWithIdPropertyOfPrimitiveLong();
		p12.setFirstName("Sven_10");
		p12.setAge(22);
		p12.setId(12345L);
		// insert
		mongoTemplate.insert(p12);
		// also try save
		mongoTemplate.save(p12);
		assertThat(p12.getId(), notNullValue());
		PersonWithIdPropertyOfPrimitiveLong p12q = mongoTemplate.findOne(new Query(where("id").in(p12.getId())),
				PersonWithIdPropertyOfPrimitiveLong.class);
		assertThat(p12q, notNullValue());
		assertThat(p12q.getId(), is(p12.getId()));
		checkCollectionContents(PersonWithIdPropertyOfPrimitiveLong.class, 1);
	}

	private void checkCollectionContents(Class<?> entityClass, int count) {
		assertThat(template.findAll(entityClass).size(), is(count));
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
	public void testUsingAnInQueryWithObjectId() throws Exception {

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
	public void testUsingAnInQueryWithStringId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeString.class);

		PersonWithIdPropertyOfTypeString p1 = new PersonWithIdPropertyOfTypeString();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeString p2 = new PersonWithIdPropertyOfTypeString();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeString p3 = new PersonWithIdPropertyOfTypeString();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeString p4 = new PersonWithIdPropertyOfTypeString();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeString> results1 = template.find(q1, PersonWithIdPropertyOfTypeString.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeString> results2 = template.find(q2, PersonWithIdPropertyOfTypeString.class);
		Query q3 = new Query(Criteria.where("id").in(p3.getId(), p4.getId()));
		List<PersonWithIdPropertyOfTypeString> results3 = template.find(q3, PersonWithIdPropertyOfTypeString.class);
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
	}

	@Test
	public void testUsingAnInQueryWithLongId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeLong.class);

		PersonWithIdPropertyOfTypeLong p1 = new PersonWithIdPropertyOfTypeLong();
		p1.setFirstName("Sven");
		p1.setAge(11);
		p1.setId(1001L);
		template.insert(p1);
		PersonWithIdPropertyOfTypeLong p2 = new PersonWithIdPropertyOfTypeLong();
		p2.setFirstName("Mary");
		p2.setAge(21);
		p2.setId(1002L);
		template.insert(p2);
		PersonWithIdPropertyOfTypeLong p3 = new PersonWithIdPropertyOfTypeLong();
		p3.setFirstName("Ann");
		p3.setAge(31);
		p3.setId(1003L);
		template.insert(p3);
		PersonWithIdPropertyOfTypeLong p4 = new PersonWithIdPropertyOfTypeLong();
		p4.setFirstName("John");
		p4.setAge(41);
		p4.setId(1004L);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeLong> results1 = template.find(q1, PersonWithIdPropertyOfTypeLong.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeLong> results2 = template.find(q2, PersonWithIdPropertyOfTypeLong.class);
		Query q3 = new Query(Criteria.where("id").in(1001L, 1004L));
		List<PersonWithIdPropertyOfTypeLong> results3 = template.find(q3, PersonWithIdPropertyOfTypeLong.class);
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
	}

	@Test
	public void testUsingAnInQueryWithPrimitiveIntId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfPrimitiveInt.class);

		PersonWithIdPropertyOfPrimitiveInt p1 = new PersonWithIdPropertyOfPrimitiveInt();
		p1.setFirstName("Sven");
		p1.setAge(11);
		p1.setId(1001);
		template.insert(p1);
		PersonWithIdPropertyOfPrimitiveInt p2 = new PersonWithIdPropertyOfPrimitiveInt();
		p2.setFirstName("Mary");
		p2.setAge(21);
		p2.setId(1002);
		template.insert(p2);
		PersonWithIdPropertyOfPrimitiveInt p3 = new PersonWithIdPropertyOfPrimitiveInt();
		p3.setFirstName("Ann");
		p3.setAge(31);
		p3.setId(1003);
		template.insert(p3);
		PersonWithIdPropertyOfPrimitiveInt p4 = new PersonWithIdPropertyOfPrimitiveInt();
		p4.setFirstName("John");
		p4.setAge(41);
		p4.setId(1004);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfPrimitiveInt> results1 = template.find(q1, PersonWithIdPropertyOfPrimitiveInt.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfPrimitiveInt> results2 = template.find(q2, PersonWithIdPropertyOfPrimitiveInt.class);
		Query q3 = new Query(Criteria.where("id").in(1001, 1003));
		List<PersonWithIdPropertyOfPrimitiveInt> results3 = template.find(q3, PersonWithIdPropertyOfPrimitiveInt.class);
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
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
		} catch (InvalidMongoDbApiUsageException e) {
		}
	}

	@Test
	public void testUsingRegexQueryWithOptions() throws Exception {

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
		p4.setFirstName("samantha");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("firstName").regex("S.*"));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("firstName").regex("S.*", "i"));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size(), is(1));
		assertThat(results2.size(), is(2));
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

		WriteResult wr = template.updateMulti(new Query(), u, PersonWithIdPropertyOfTypeObjectId.class);

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
	public void testAddingToList() {
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
