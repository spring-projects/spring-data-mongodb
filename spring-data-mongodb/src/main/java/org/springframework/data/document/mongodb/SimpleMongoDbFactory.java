package org.springframework.data.document.mongodb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class SimpleMongoDbFactory implements MongoDbFactory {

  /**
   * Logger, available to subclasses.
   */
  protected final Log logger = LogFactory.getLog(getClass());

  private Mongo mongo;
  private String databaseName;
  private String username;
  private String password;

  /**
   * Create an instance of SimpleMongoDbFactory given the Mongo instance and database name
   * @param mongo Mongo instance, not null
   * @param databaseName Database name, not null
   */
  public SimpleMongoDbFactory(Mongo mongo, String databaseName) {
    Assert.notNull(mongo, "Mongo must not be null");
    Assert.hasText(databaseName, "Database name must not be empty");
    this.mongo = mongo;
    this.databaseName = databaseName;
  }
  
  /**
   * Create an instance of SimpleMongoDbFactory given the Mongo instance, database name, and username/password
   * @param mongo Mongo instance, not null
   * @param databaseName Database name, not null
   * @param userCredentials username and password
   */
  public SimpleMongoDbFactory(Mongo mongo, String databaseName, UserCredentials userCredentials) {
    this(mongo, databaseName);
    this.username = userCredentials.getUsername();
    this.password = userCredentials.getPassword();
  }

  public DB getDb() throws DataAccessException {
    Assert.notNull(mongo, "Mongo must not be null");
    Assert.hasText(databaseName, "Database name must not be empty");
    return MongoDbUtils.getDB(mongo, databaseName, username, password == null ? null : password.toCharArray());
  }
  
  public DB getDb(String dbName) throws DataAccessException {
    Assert.notNull(mongo, "Mongo must not be null");
    Assert.hasText(dbName, "Database name must not be empty");
    return MongoDbUtils.getDB(mongo, dbName, username, password == null ? null : password.toCharArray());
  }
  
  

  public Mongo getMongo() {
    return this.mongo;
  }

}
