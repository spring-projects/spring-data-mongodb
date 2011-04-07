package org.springframework.data.document.mongodb;

import org.springframework.beans.factory.annotation.Autowired;
import static org.springframework.data.document.mongodb.query.Criteria.where;
import org.springframework.data.document.mongodb.query.Query;
import static org.springframework.data.document.mongodb.query.Query.query;
import org.springframework.data.document.mongodb.query.Update;
import static org.springframework.data.document.mongodb.query.Update.update;

public class PersonExample {

  @Autowired
  private MongoOperations mongoOps;
  
  public void doWork() {
    Person p = new Person();
    p.setFirstName("Sven");
    p.setAge(22);
    
    mongoOps.save(p);
    
    System.out.println(p.getId());
    
    mongoOps.updateFirst(new Query(where("firstName").is("Sven")), new Update().set("age", 24));
    
    mongoOps.updateFirst(new Query(where("firstName").is("Sven")), update("age", 24));
    
    mongoOps.updateFirst(query(where("firstName").is("Sven")), update("age", 24));
  }
  
  
}
