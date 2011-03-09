package org.springframework.data.document.persistence;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.persistence.document.test.Account;
import org.springframework.persistence.document.test.MongoPerson;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
	
	private EntityManager entityManager;
	
	private String colName = MongoPerson.class.getSimpleName().toLowerCase();

	
    @PersistenceContext
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

	private void clearData() {
		DBCollection col = this.mongoTemplate.getCollection(colName);
		if (col != null) {
			this.mongoTemplate.dropCollection(colName);
		}
	}
	
	@Test
	@Transactional
	@Rollback(false)
	public void testUserConstructor() {
		clearData();
		int age = 33;
		MongoPerson p = new MongoPerson("Thomas", age);
		Assert.assertEquals(age, p.getAge());
		p.birthday();
		Assert.assertEquals(1 + age, p.getAge());
	}

	@Test
	@Transactional
	public void testInstantiatedFinder() throws MongoException {
		DBCollection col = this.mongoTemplate.getCollection(colName);
		DBObject dbo = col.findOne();
		Object _id = dbo.get("_id");
		MongoPerson found = MongoPerson.findPerson(_id);
		Assert.assertNotNull(found);
		Assert.assertEquals(_id, found.getId());
		System.out.println("Loaded MongoPerson data: " + found);
	}

	@Test
	@Transactional
	@Rollback(false)
	public void testCreateJpaEntity() {
		clearData();
		Account a = new Account();
		a.setName("My Account");
		a.setFriendlyName("My Test Acct.");
		a.setBalance(123.45F);
		a.setId(2L);
		MongoPerson p = new MongoPerson("Jack", 22);
		entityManager.persist(a);
		p.setAccount(a);
	}

	@Test
	@Transactional
	public void testReadJpaEntity() {
		DBCollection col = this.mongoTemplate.getCollection(colName);
		DBCursor dbc = col.find();
		Object _id = null;
		for (DBObject dbo : dbc) {
			System.out.println(dbo);
			if ("Jack".equals(dbo.get("name"))) {
				_id = dbo.get("_id");
				break;
			}
		}
		System.out.println(_id);
		MongoPerson found = MongoPerson.findPerson(_id);
		System.out.println(found);
		if (found != null)
			System.out.println(found.getAccount());
	}

}
