package org.springframework.data.document.persistence;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.persistence.document.test.Person;
import org.springframework.persistence.document.test.Resume;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.mongodb.DBCollection;
import com.mongodb.Mongo;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/META-INF/spring/applicationContext.xml")
public class CrossStoreMongoTests {

  @Autowired
  private Mongo mongo;

  @Autowired
  private MongoTemplate mongoTemplate;

  private EntityManager entityManager;

  @Autowired
  private PlatformTransactionManager transactionManager;
  
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
  public void testCreateJpaToMongoEntityRelationship() {
    clearData(Person.class.getName());
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
  @Rollback(false)
  public void testReadJpaToMongoEntityRelationship() {
    Person found = entityManager.find(Person.class, 1L);
    Assert.assertNotNull(found);
    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found);
    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found.getResume());
    Assert.assertEquals("DiMark, DBA, 1990-2000" + "; "
        + "VMware, Developer, 2007-", found.getResume().getJobs());
    found.getResume().addJob("SpringDeveloper.com, Consultant, 2005-2006");
    found.setAge(44);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void testUpdatedJpaToMongoEntityRelationship() {
    Person found = entityManager.find(Person.class, 1L);
    Assert.assertNotNull(found);
    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found);
    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found.getResume());
    Assert.assertEquals("DiMark, DBA, 1990-2000" + "; "
        + "VMware, Developer, 2007-" + "; "
        + "SpringDeveloper.com, Consultant, 2005-2006", found.getResume().getJobs());
  }
  
  @Test
  public void testMergeJpaEntityWithMongoDocument() {
    TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
    final Person found = entityManager.find(Person.class, 1L);
    found.setAge(77);
    found.getResume().addJob("TargetRx, Developer, 2000-2005");
    txTemplate.execute(new TransactionCallback<Object>() {
      public Object doInTransaction(TransactionStatus status) {
        entityManager.merge(found);
        return null;
      }
    });
    final Person updated = entityManager.find(Person.class, 1L);
    // assert that the new values are in respective DBs
    // TODO: during merge we lose the changeset since JPA creates a new persistent instance - 
    // we need to move the existing changeset over somehow
  }
}
