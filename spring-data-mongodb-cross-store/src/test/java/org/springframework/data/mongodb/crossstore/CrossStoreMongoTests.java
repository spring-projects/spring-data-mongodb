/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.crossstore;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.crossstore.test.Address;
import org.springframework.data.mongodb.crossstore.test.Person;
import org.springframework.data.mongodb.crossstore.test.Resume;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Integration tests for MongoDB cross-store persistence (mainly {@link MongoChangeSetPersister}).
 *
 * @author Thomas Risberg
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/META-INF/spring/applicationContext.xml")
public class CrossStoreMongoTests {

	@Autowired MongoTemplate mongoTemplate;

	@PersistenceContext EntityManager entityManager;

	@Autowired PlatformTransactionManager transactionManager;
	TransactionTemplate txTemplate;

	@Before
	public void setUp() {

		txTemplate = new TransactionTemplate(transactionManager);

		clearData(Person.class);

		Address address = new Address(12, "MAin St.", "Boston", "MA", "02101");

		Resume resume = new Resume();
		resume.addEducation("Skanstulls High School, 1975");
		resume.addEducation("Univ. of Stockholm, 1980");
		resume.addJob("DiMark, DBA, 1990-2000");
		resume.addJob("VMware, Developer, 2007-");

		final Person person = new Person("Thomas", 20);
		person.setAddress(address);
		person.setResume(resume);
		person.setId(1L);

		txTemplate.execute(new TransactionCallback<Void>() {
			public Void doInTransaction(TransactionStatus status) {
				entityManager.persist(person);
				return null;
			}
		});
	}

	@After
	public void tearDown() {
		txTemplate.execute(new TransactionCallback<Void>() {
			public Void doInTransaction(TransactionStatus status) {
				entityManager.remove(entityManager.find(Person.class, 1L));
				return null;
			}
		});
	}

	private void clearData(Class<?> domainType) {

		String collectionName = mongoTemplate.getCollectionName(domainType);
		mongoTemplate.dropCollection(collectionName);
	}

	@Test
	@Transactional
	public void testReadJpaToMongoEntityRelationship() {

		Person found = entityManager.find(Person.class, 1L);
		Assert.assertNotNull(found);
		Assert.assertEquals(Long.valueOf(1), found.getId());
		Assert.assertNotNull(found);
		Assert.assertEquals(Long.valueOf(1), found.getId());
		Assert.assertNotNull(found.getResume());
		Assert.assertEquals("DiMark, DBA, 1990-2000" + "; " + "VMware, Developer, 2007-", found.getResume().getJobs());
	}

	@Test
	@Transactional
	public void testUpdatedJpaToMongoEntityRelationship() {

		Person found = entityManager.find(Person.class, 1L);
		found.setAge(44);
		found.getResume().addJob("SpringDeveloper.com, Consultant, 2005-2006");

		entityManager.merge(found);

		Assert.assertNotNull(found);
		Assert.assertEquals(Long.valueOf(1), found.getId());
		Assert.assertNotNull(found);
		Assert.assertEquals(Long.valueOf(1), found.getId());
		Assert.assertNotNull(found.getResume());
		Assert.assertEquals("DiMark, DBA, 1990-2000" + "; " + "VMware, Developer, 2007-" + "; "
				+ "SpringDeveloper.com, Consultant, 2005-2006", found.getResume().getJobs());
	}

	@Test
	public void testMergeJpaEntityWithMongoDocument() {

		final Person detached = entityManager.find(Person.class, 1L);
		entityManager.detach(detached);
		detached.getResume().addJob("TargetRx, Developer, 2000-2005");

		Person merged = txTemplate.execute(new TransactionCallback<Person>() {
			public Person doInTransaction(TransactionStatus status) {
				Person result = entityManager.merge(detached);
				entityManager.flush();
				return result;
			}
		});

		Assert.assertTrue(detached.getResume().getJobs().contains("TargetRx, Developer, 2000-2005"));
		Assert.assertTrue(merged.getResume().getJobs().contains("TargetRx, Developer, 2000-2005"));
		final Person updated = entityManager.find(Person.class, 1L);
		Assert.assertTrue(updated.getResume().getJobs().contains("TargetRx, Developer, 2000-2005"));
	}

	@Test
	public void testRemoveJpaEntityWithMongoDocument() {

		txTemplate.execute(new TransactionCallback<Person>() {
			public Person doInTransaction(TransactionStatus status) {
				Person p2 = new Person("Thomas", 20);
				Resume r2 = new Resume();
				r2.addEducation("Skanstulls High School, 1975");
				r2.addJob("DiMark, DBA, 1990-2000");
				p2.setResume(r2);
				p2.setId(2L);
				entityManager.persist(p2);
				Person p3 = new Person("Thomas", 20);
				Resume r3 = new Resume();
				r3.addEducation("Univ. of Stockholm, 1980");
				r3.addJob("VMware, Developer, 2007-");
				p3.setResume(r3);
				p3.setId(3L);
				entityManager.persist(p3);
				return null;
			}
		});
		txTemplate.execute(new TransactionCallback<Person>() {
			public Person doInTransaction(TransactionStatus status) {
				final Person found2 = entityManager.find(Person.class, 2L);
				entityManager.remove(found2);
				return null;
			}
		});

		boolean weFound3 = false;

		for (Document dbo : this.mongoTemplate.getCollection(mongoTemplate.getCollectionName(Person.class)).find()) {
			Assert.assertTrue(!dbo.get("_entity_id").equals(2L));
			if (dbo.get("_entity_id").equals(3L)) {
				weFound3 = true;
			}
		}
		Assert.assertTrue(weFound3);
	}

}
