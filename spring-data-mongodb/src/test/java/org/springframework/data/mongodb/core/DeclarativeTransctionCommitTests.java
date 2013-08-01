package org.springframework.data.mongodb.core;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

@ContextConfiguration("classpath:mongotx-test.xml")
@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
@TransactionConfiguration(defaultRollback = false)
public class DeclarativeTransctionCommitTests {

  static final String COLLECTION = "declarative_transction_commit";

  @Autowired
  private MongoTemplate mongoTemplate;

  @BeforeTransaction
  public void ensureCollectionIsEmpty() {
    DB db = mongoTemplate.getDb();
    db.getCollection(COLLECTION).remove(new BasicDBObject());
  }

  @Test
  public void testSaveDoc() {
    DB db = mongoTemplate.getDb();
    db.getCollection(COLLECTION).save(new BasicDBObject("_id", 1));
    db.getCollection(COLLECTION).save(new BasicDBObject("_id", 2));
  }

  @AfterTransaction
  public void assertCollectionIsEmpty() {
    DB db = mongoTemplate.getDb();
    int count = db.getCollection(COLLECTION).find().itcount();
    org.junit.Assert.assertEquals("two saved documents not commited", 2, count);
  }

//  @Transactional(isolation = Isolation.READ_COMMITTED)
//  @Test(expected = InvalidIsolationLevelException.class)
//  public void testIsolation() {
//    DB db = mongoTemplate.getDb();
//    db.getCollection("ttt").save(new BasicDBObject("_id", 1));
//  }

}
