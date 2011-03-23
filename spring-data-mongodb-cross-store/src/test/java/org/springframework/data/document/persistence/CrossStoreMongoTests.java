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
import org.springframework.transaction.annotation.Transactional;

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

    // TODO: This part isn't quite working yet - need to intercept the id
    // population from JPA EM
    found.setId(found.getId());

    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found);
    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found.getResume());
    Assert.assertEquals("DiMark, DBA, 1990-2000" + "; "
        + "VMware, Developer, 2007-", found.getResume().getJobs());
    found.getResume().addJob("SpringDeveloper.com, Consultant, 2005-2006");
  }

  @Test
  @Transactional
  @Rollback(false)
  public void testUpdatedJpaToMongoEntityRelationship() {
    Person found = entityManager.find(Person.class, 1L);
    Assert.assertNotNull(found);

    // TODO: This part isn't quite working yet - need to intercept the id
    // population from JPA EM
    found.setId(found.getId());

    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found);
    Assert.assertEquals(Long.valueOf(1), found.getId());
    Assert.assertNotNull(found.getResume());
    Assert.assertEquals("DiMark, DBA, 1990-2000" + "; "
        + "VMware, Developer, 2007-" + "; "
        + "SpringDeveloper.com, Consultant, 2005-2006", found.getResume().getJobs());
  }
}
