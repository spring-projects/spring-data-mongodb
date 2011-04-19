/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.mapping;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoDbUtils;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Query;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MappingTests {

	private static final Log LOGGER = LogFactory.getLog(MongoDbUtils.class);
	private final String[] collectionsToDrop = new String[]{
			"foobar",
			"person",
			"personmapproperty",
			"personpojo",
			"personcustomidname",
			"personmultidimarrays",
			"personmulticollection",
			"person1",
			"person2",
			"account"
	};

	ApplicationContext applicationContext;
	MongoTemplate template;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() throws Exception {
		Mongo mongo = new Mongo();
		DB db = mongo.getDB("database");
		for (String coll : collectionsToDrop) {
			db.getCollection(coll).drop();
		}
		applicationContext = new ClassPathXmlApplicationContext("/mapping.xml");
		template = applicationContext.getBean(MongoTemplate.class);
		mappingContext = applicationContext.getBean(MongoMappingContext.class);
	}

	@Test
	public void testGeneratedId() {
		GeneratedId genId = new GeneratedId("test");
		template.insert(genId);

		assertNotNull(genId.getId());
	}

	@Test
	public void testPersonPojo() throws Exception {
		// POJOs aren't auto-detected, have to add manually
		mappingContext.addPersistentEntity(PersonPojo.class);

		LOGGER.info("about to create new personpojo");
		PersonPojo p = new PersonPojo(12345, "Person", "Pojo");
		LOGGER.info("about to insert");
		template.insert(p);
		LOGGER.info("done inserting");
		assertNotNull(p.getId());

		List<PersonPojo> result = template.find(new Query(Criteria.where("ssn").is(12345)), PersonPojo.class);
		assertThat(result.size(), is(1));
		assertThat(result.get(0).getSsn(), is(12345));
	}

	@Test
	public void testPersonWithCustomIdName() {
		// POJOs aren't auto-detected, have to add manually
		mappingContext.addPersistentEntity(PersonCustomIdName.class);

		PersonCustomIdName p = new PersonCustomIdName(123456, "Custom Id", null);
		template.insert(p);

		List<PersonCustomIdName> result = template.find(new Query(Criteria.where("ssn").is(123456)), PersonCustomIdName.class);
		assertThat(result.size(), is(1));
		assertNotNull(result.get(0).getLastName());

		PersonCustomIdName p2 = new PersonCustomIdName(654321, "Custom Id", "LastName");
		template.insert(p2);

		List<PersonCustomIdName> result2 = template.find(new Query(Criteria.where("ssn").is(654321)), PersonCustomIdName.class);
		assertThat(result2.size(), is(1));
		assertNotNull(result2.get(0).getLastName());
		assertThat(result2.get(0).getLastName(), is("LastName"));

	}

	@Test
	public void testPersonMapProperty() {
		PersonMapProperty p = new PersonMapProperty(1234567, "Map", "Property");
		Map<String, AccountPojo> accounts = new HashMap<String, AccountPojo>();

		AccountPojo checking = new AccountPojo("checking", 1000.0f);
		AccountPojo savings = new AccountPojo("savings", 10000.0f);

		accounts.put("checking", checking);
		accounts.put("savings", savings);
		p.setAccounts(accounts);

		template.insert(p);
		assertNotNull(p.getId());

		List<PersonMapProperty> result = template.find(new Query(Criteria.where("ssn").is(1234567)), PersonMapProperty.class);
		assertThat(result.size(), is(1));
		assertThat(result.get(0).getAccounts().size(), is(2));
		assertThat(result.get(0).getAccounts().get("checking").getBalance(),
				is(1000.0f));
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testWriteEntity() {

		Address addr = new Address();
		addr.setLines(new String[]{"1234 W. 1st Street", "Apt. 12"});
		addr.setCity("Anytown");
		addr.setPostalCode(12345);
		addr.setCountry("USA");

		Account acct = new Account();
		acct.setBalance(1000.00f);
		template.insert("account", acct);

		List<Account> accounts = new ArrayList<Account>();
		accounts.add(acct);

		Person p = new Person(123456789, "John", "Doe", 37, addr);
		p.setAccounts(accounts);
		template.insert("person", p);

		Account newAcct = new Account();
		newAcct.setBalance(10000.00f);
		template.insert("account", newAcct);

		accounts.add(newAcct);
		template.save("person", p);

		assertNotNull(p.getId());

		List<Person> result = template.find(new Query(Criteria.where("ssn").is(123456789)), Person.class);
		assertThat(result.size(), is(1));
		assertThat(result.get(0).getAddress().getCountry(), is("USA"));
		assertThat(result.get(0).getAccounts(), notNullValue());
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testUniqueIndex() {
		Address addr = new Address();
		addr.setLines(new String[]{"1234 W. 1st Street", "Apt. 12"});
		addr.setCity("Anytown");
		addr.setPostalCode(12345);
		addr.setCountry("USA");

		Person p1 = new Person(1234567890, "John", "Doe", 37, addr);
		Person p2 = new Person(1234567890, "Jane", "Doe", 38, addr);

		template.insert(p2);
		template.insert(p1);

		List<Person> result = template.find(new Query(Criteria.where("ssn").is(1234567890)), Person.class);
		assertThat(result.size(), is(1));
	}

	@Test
	public void testCustomCollectionInList() {
		List<BasePerson> persons = new ArrayList<BasePerson>();
		persons.add(new PersonCustomCollection1(55555, "Person", "One"));
		persons.add(new PersonCustomCollection2(66666, "Person", "Two"));
		template.insertList(persons);

		List<PersonCustomCollection1> p1Results = template.find("person1",
				new Query(Criteria.where("ssn").is(55555)),
				PersonCustomCollection1.class);
		List<PersonCustomCollection2> p2Results = template.find("person2",
				new Query(Criteria.where("ssn").is(66666)),
				PersonCustomCollection2.class);
		assertThat(p1Results.size(), is(1));
		assertThat(p2Results.size(), is(1));
	}

	@Test
	public void testPrimitivesAndCustomCollectionName() {
		Location loc = new Location(
				new double[]{1.0, 2.0},
				new int[]{1, 2, 3, 4},
				new float[]{1.0f, 2.0f}
		);
		template.insert(loc);

		List<Location> result = template.find("places", new Query(Criteria.where("_id").is(loc.getId())), Location.class);
		assertThat(result.size(), is(1));
	}

	@Test
	public void testIndexesCreatedInRightCollection() {
		CustomCollectionWithIndex ccwi = new CustomCollectionWithIndex("test");
		template.insert(ccwi);

		assertTrue(template.execute("foobar", new CollectionCallback<Boolean>() {
			public Boolean doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				List<DBObject> indexes = collection.getIndexInfo();
				for (DBObject dbo : indexes) {
					if ("name_1".equals(dbo.get("name"))) {
						return true;
					}
				}
				return false;
			}
		}));

		DetectedCollectionWithIndex dcwi = new DetectedCollectionWithIndex("test");
		template.insert(dcwi);

		assertTrue(template.execute(DetectedCollectionWithIndex.class.getSimpleName().toLowerCase(), new CollectionCallback<Boolean>() {
			public Boolean doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				List<DBObject> indexes = collection.getIndexInfo();
				for (DBObject dbo : indexes) {
					if ("name_1".equals(dbo.get("name"))) {
						return true;
					}
				}
				return false;
			}
		}));
	}

	@Test
	public void testMultiDimensionalArrayProperties() {
		String[][] grid = new String[][]{
				new String[]{"1", "2", "3", "4"},
				new String[]{"5", "6", "7", "8"},
				new String[]{"9", "10", "11", "12"}
		};
		PersonMultiDimArrays p = new PersonMultiDimArrays(123, "Multi", "Dimensional", grid);

		template.insert(p);
		List<PersonMultiDimArrays> result = template.find(new Query(Criteria.where("ssn").is(123)), PersonMultiDimArrays.class);
		assertThat(result.size(), is(1));

		assertThat(result.get(0).getGrid().length, is(3));
	}

	@Test
	public void testMultiDimensionalCollectionProperties() {
		List<List<String>> grid = new ArrayList<List<String>>();
		ArrayList<String> inner = new ArrayList<String>();
		inner.add("1");
		inner.add("2");
		inner.add("3");
		inner.add("4");
		grid.add(inner);

		PersonMultiCollection p = new PersonMultiCollection(321, "Multi Dim", "Collections", grid);
		template.insert(p);

		List<PersonMultiCollection> result = template.find(new Query(Criteria.where("ssn").is(321)), PersonMultiCollection.class);
		assertThat(result.size(), is(1));

		assertThat(result.get(0).getGrid().size(), is(1));
	}

}
