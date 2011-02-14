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

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertThat;

import static org.springframework.data.document.mongodb.query.Criteria.where;

import java.util.List;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Index;
import org.springframework.data.document.mongodb.query.Index.Duplicates;
import org.springframework.data.document.mongodb.query.Order;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.document.mongodb.query.Update;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

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

	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@Before
	public void setUp() {
		template.dropCollection(template.getDefaultCollectionName());
	}

	@Test
	public void insertsSimpleEntityCorrectly() throws Exception {

		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);
		
		MongoConverter converter = template.getConverter();

		List<Person> result = template.find(new Query(Criteria.where("_id").is(converter.convertObjectId(person.getId()))), Person.class);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(person));
	}

	@Test
	public void updateFailure() throws Exception {

		MongoTemplate mongoTemplate = new MongoTemplate(template.getDb().getMongo(), "test", "people", 
				new WriteConcern(), WriteResultChecking.EXCEPTION);
		
		Person person = new Person("Oliver2");
		person.setAge(25);
		mongoTemplate.insert(person);
		
		Query q = new Query(Criteria.where("BOGUS").gt(22));
		Update u = new Update().set("firstName", "Sven");
		thrown.expect(DataIntegrityViolationException.class);
		thrown.expectMessage( endsWith("0 documents updated") );
		mongoTemplate.updateFirst(q, u);
		
	}

	@Test
	public void testEnsureIndex() throws Exception {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1);
		Person p2 = new Person("Sven");
		p2.setAge(40);
		template.insert(p2);
		
		template.ensureIndex(new Index().on("age", Order.DESCENDING).unique(Duplicates.DROP));
		
		DBCollection coll = template.getCollection(template.getDefaultCollectionName());
		List<DBObject> indexInfo = coll.getIndexInfo();
		
		assertThat(indexInfo.size(), is(2));
		String indexKey = null;
		boolean unique = false;
		boolean dropDupes = false;
		for (DBObject ix : indexInfo) {
			if ("age_-1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
				unique = (Boolean) ix.get("unique");
				dropDupes = (Boolean) ix.get("drop_dups");
			}
		}
		assertThat(indexKey, is("{ \"age\" : -1}"));
		assertThat(unique, is(true));
		assertThat(dropDupes, is(true));
	}

	@Test
	public void testProperHandlingOfDifferentIdTypes() throws Exception {
		PersonWithIdPropertyOfTypeString p1 = new PersonWithIdPropertyOfTypeString();
		p1.setFirstName("Sven_1");
		p1.setAge(22);
		template.insert(p1);
		assertThat(p1.getId(), notNullValue());
		PersonWithIdPropertyOfTypeString p1q = template.findOne(new Query(where("id").is(p1.getId())), PersonWithIdPropertyOfTypeString.class);
		assertThat(p1q, notNullValue());
		assertThat(p1q.getId(), is(p1.getId()));

		PersonWithIdPropertyOfTypeString p2 = new PersonWithIdPropertyOfTypeString();
		p2.setFirstName("Sven_2");
		p2.setAge(22);
		p2.setId("TWO");
		template.insert(p2);
		assertThat(p2.getId(), notNullValue());
		PersonWithIdPropertyOfTypeString p2q = template.findOne(new Query(where("id").is(p2.getId())), PersonWithIdPropertyOfTypeString.class);
		assertThat(p2q, notNullValue());
		assertThat(p2q.getId(), is(p2.getId()));
		
		PersonWith_idPropertyOfTypeString p3 = new PersonWith_idPropertyOfTypeString();
		p3.setFirstName("Sven_3");
		p3.setAge(22);
		template.insert(p3);
		assertThat(p3.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeString p3q = template.findOne(new Query(where("_id").is(p3.get_id())), PersonWith_idPropertyOfTypeString.class);
		assertThat(p3q, notNullValue());
		assertThat(p3q.get_id(), is(p3.get_id()));

		PersonWith_idPropertyOfTypeString p4 = new PersonWith_idPropertyOfTypeString();
		p4.setFirstName("Sven_4");
		p4.setAge(22);
		p4.set_id("FOUR");
		template.insert(p4);
		assertThat(p4.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeString p4q = template.findOne(new Query(where("_id").is(p4.get_id())), PersonWith_idPropertyOfTypeString.class);
		assertThat(p4q, notNullValue());
		assertThat(p4q.get_id(), is(p4.get_id()));

		PersonWithIdPropertyOfTypeObjectId p5 = new PersonWithIdPropertyOfTypeObjectId();
		p5.setFirstName("Sven_5");
		p5.setAge(22);
		template.insert(p5);
		assertThat(p5.getId(), notNullValue());
		PersonWithIdPropertyOfTypeObjectId p5q = template.findOne(new Query(where("id").is(p5.getId())), PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p5q, notNullValue());
		assertThat(p5q.getId(), is(p5.getId()));

		PersonWithIdPropertyOfTypeObjectId p6 = new PersonWithIdPropertyOfTypeObjectId();
		p6.setFirstName("Sven_6");
		p6.setAge(22);
		p6.setId(new ObjectId());
		template.insert(p6);
		assertThat(p6.getId(), notNullValue());
		PersonWithIdPropertyOfTypeObjectId p6q = template.findOne(new Query(where("id").is(p6.getId())), PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p6q, notNullValue());
		assertThat(p6q.getId(), is(p6.getId()));
		
		PersonWith_idPropertyOfTypeObjectId p7 = new PersonWith_idPropertyOfTypeObjectId();
		p7.setFirstName("Sven_7");
		p7.setAge(22);
		template.insert(p7);
		assertThat(p7.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeObjectId p7q = template.findOne(new Query(where("_id").is(p7.get_id())), PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p7q, notNullValue());
		assertThat(p7q.get_id(), is(p7.get_id()));

		PersonWith_idPropertyOfTypeObjectId p8 = new PersonWith_idPropertyOfTypeObjectId();
		p8.setFirstName("Sven_8");
		p8.setAge(22);
		p8.set_id(new ObjectId());
		template.insert(p8);
		assertThat(p8.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeObjectId p8q = template.findOne(new Query(where("_id").is(p8.get_id())), PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p8q, notNullValue());
		assertThat(p8q.get_id(), is(p8.get_id()));
	}

}
