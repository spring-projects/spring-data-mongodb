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
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertThat;

import static org.springframework.data.document.mongodb.query.Criteria.where;

import java.util.List;

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
			System.out.println(ix);
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
	public void simpleQuery() throws Exception {
		new Query(where("name").is("Mary")).and(where("age").lt(33).gt(22)).skip(22).limit(20);
		// TODO: more tests
	}
}
