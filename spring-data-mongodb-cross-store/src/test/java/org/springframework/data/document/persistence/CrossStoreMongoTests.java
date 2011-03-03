package org.springframework.data.document.persistence;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.persistence.document.test.MongoPerson;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/META-INF/spring/applicationContext.xml")
public class CrossStoreMongoTests {

	@Autowired
	private Mongo mongo;

	@Autowired
	private MongoTemplate mongoTemplate;

	@BeforeTransaction
	public void setUp() {
		DBCollection col = this.mongoTemplate.getCollection(MongoPerson.class.getSimpleName().toLowerCase());
		if (col != null) {
			this.mongoTemplate.dropCollection(MongoPerson.class.getName());
		}
	}
	
	@Test
	@Transactional
	@Rollback(false)
	public void testUserConstructor() {
		int age = 33;
		MongoPerson p = new MongoPerson("Thomas", age);
		Assert.assertEquals(age, p.getAge());
		p.birthday();
		Assert.assertEquals(1 + age, p.getAge());
	}

	@Test
	@Transactional
	public void testInstantiatedFinder() throws MongoException {
		String key = MongoPerson.class.getSimpleName().toLowerCase();
		DBCollection col = this.mongoTemplate.getCollection(key);
		DBObject dbo = col.findOne();
		Object id1 = dbo.get("_id");
		MongoPerson found = MongoPerson.findPerson(id1);
		Assert.assertNotNull(found);
		Assert.assertEquals(id1, found.getId());
		System.out.println("Loaded MongoPerson data: " + found);
	}

}
