package org.springframework.data.mongodb.core;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:mongotx-test.xml")
public class ProgrammaticTransactionTests {
  static final String COLLECTION = "programmatic_transaction_rollback";

  @Autowired
  PlatformTransactionManager transactionManager;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Test
  public void testTxTemplate() {
    TransactionTemplate txt = new TransactionTemplate(transactionManager);
    String version = txt.execute(new TransactionCallback<String>() {
      @Override
      public String doInTransaction(TransactionStatus status) {
        DB db = mongoTemplate.getDb();
        db.getCollection(COLLECTION).save(new BasicDBObject("_id", 1));
        db.getCollection(COLLECTION).save(new BasicDBObject("_id", 2));
        status.setRollbackOnly();
        return "";
      }
    });
    int ndocs = mongoTemplate.getDb().getCollection(COLLECTION).find().itcount();
    org.junit.Assert.assertEquals("should count 0 saved documents", 0, ndocs);
  }

  @Test
  public void testProgramaticTx() {
    DefaultTransactionDefinition def = new DefaultTransactionDefinition();
    def.setName("testProgramaticTx");
    def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    TransactionStatus status = transactionManager.getTransaction(def);
    DB db = mongoTemplate.getDb();
    db.getCollection(COLLECTION).save(new BasicDBObject("_id", 1));
    db.getCollection(COLLECTION).save(new BasicDBObject("_id", 2));
    int ndocs = db.getCollection(COLLECTION).find().itcount();
    org.junit.Assert.assertEquals("should count 2 saved documents", 2, ndocs);
    transactionManager.rollback(status);
    ndocs = db.getCollection(COLLECTION).find().itcount();
    org.junit.Assert.assertEquals("should count 0 saved documents", 0, ndocs);
  }

}
