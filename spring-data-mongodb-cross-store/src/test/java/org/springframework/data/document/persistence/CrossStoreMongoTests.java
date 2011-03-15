package org.springframework.data.document.persistence;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.mongodb.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.persistence.document.test.Account;
import org.springframework.persistence.document.test.MongoPerson;
import org.springframework.persistence.document.test.Person;
import org.springframework.persistence.document.test.Resume;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

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

  private void clearData(String collectionName) {
    DBCollection col = this.mongoTemplate.getCollection(collectionName);
    if (col != null) {
      this.mongoTemplate.dropCollection(collectionName);
    }
  }

  @Test
  @Transactional
  @Rollback(false)
  public void testUserConstructor() {
    clearData(colName);
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
  public void testCreateMongoToJpaEntityRelationship() {
    clearData(colName);
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
  public void testReadMongoToJpaEntityRelationship() {
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

  @Test
  @Transactional
  @Rollback(false)
  public void testCreateJpaToMongoEntityRelationship() {
    clearData("resume");
    Person p = new Person("Thomas", 20);
    Resume r = new Resume();
    r.addEducation("Skanstulls High School, 1975");
    r.addEducation("Univ. of Stockholm, 1980");
    r.addJob("DiMark, DBA, 1990-2000");
    r.addJob("VMware, Developer, 2007-");
    p.setResume(r);
    p.setId(1L);
    entityManager.persist(p);
  }

  @Test
  @Transactional
  public void testReadJpaToMongoEntityRelationship() {
    Person found = entityManager.find(Person.class, 1L);
    System.out.println(found);
//		TODO: This part isn't working yet - there is no reference to the Momgo _id stored in the db
//		if (found != null)
//			System.out.println(found.getResume());
  }

}
