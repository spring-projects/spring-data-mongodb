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
import com.mongodb.Mongo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.document.mongodb.MongoDbUtils;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.mapping.model.MappingException;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MappingTests {

	private static final Log LOGGER = LogFactory.getLog(MongoDbUtils.class);
	private final String[] collectionsToDrop = new String[]{"person", "personmapproperty", "personpojo", "personcustomidname", "account"};

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

		PersonCustomIdName p = new PersonCustomIdName(123456, "Custom Id");
		template.insert(p);

		List<PersonCustomIdName> result = template.find(new Query(Criteria.where("ssn").is(123456)), PersonCustomIdName.class);
		assertThat(result.size(), is(1));
		assertNotNull(result.get(0).getLastName());
	}

	@Test(expected = MappingException.class)
	public void testPersonWithInvalidCustomIdName() {
		// POJOs aren't auto-detected, have to add manually
		mappingContext.addPersistentEntity(PersonInvalidId.class);

		PersonInvalidId p = new PersonInvalidId();
		template.insert(p);
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
		Person p2 = new Person(1234567890, "John", "Doe", 37, addr);

		template.insert("person", p1);
		template.insert("person", p2);

		List<Person> result = template.find(new Query(Criteria.where("ssn").is(1234567890)), Person.class);
		assertThat(result.size(), is(1));
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

}
