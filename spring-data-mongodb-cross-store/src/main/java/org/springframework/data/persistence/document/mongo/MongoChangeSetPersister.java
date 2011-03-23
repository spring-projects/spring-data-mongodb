package org.springframework.data.persistence.document.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.persistence.ChangeSet;
import org.springframework.data.persistence.ChangeSetBacked;
import org.springframework.data.persistence.ChangeSetPersister;
import org.springframework.util.ClassUtils;

public class MongoChangeSetPersister implements ChangeSetPersister<Object> {

  private static final String ENTITY_CLASS = "_entity_class";

  private static final String ENTITY_ID = "_entity_id";

  private static final String ENTITY_FIELD_NAME = "_entity_field_name";

  private static final String ENTITY_FIELD_CLASS = "_entity_field_class";

  protected final Log log = LogFactory.getLog(getClass());

  private MongoTemplate mongoTemplate;

  public void setMongoTemplate(MongoTemplate mongoTemplate) {
    this.mongoTemplate = mongoTemplate;
  }

  @Override
  public void getPersistentState(Class<? extends ChangeSetBacked> entityClass,
      Object id, final ChangeSet changeSet) throws DataAccessException,
      NotFoundException {
    String collName = getCollectionNameForEntity(entityClass);

    final DBObject dbk = new BasicDBObject();
    dbk.put(ENTITY_ID, id);
    dbk.put(ENTITY_CLASS, entityClass.getName());
    mongoTemplate.execute(collName, new CollectionCallback<Object>() {
      @Override
      public Object doInCollection(DBCollection collection)
          throws MongoException, DataAccessException {
        for (DBObject dbo : collection.find(dbk)) {
          String key = (String) dbo.get(ENTITY_FIELD_NAME);
          String className = (String) dbo.get(ENTITY_FIELD_CLASS);
          if (className == null) {
            throw new DataIntegrityViolationException(
                "Unble to convert property " + key
                    + ": Invalid metadata, " + ENTITY_FIELD_CLASS + " not available");
          }
          Class<?> clazz = null;
          try {
            clazz = Class.forName(className);
          } catch (ClassNotFoundException e) {
            throw new DataIntegrityViolationException(
                "Unble to convert property " + key + " of type " + className, e);
          }
          Object value = mongoTemplate.getConverter().read(clazz, dbo);
          changeSet.set(key, value);
        }
        return null;
      }
    });
  }

  @Override
  public Object getPersistentId(Class<? extends ChangeSetBacked> entityClass,
      ChangeSet cs) throws DataAccessException {
    log.debug("getPersistentId called on " + entityClass);
    if (cs == null) {
      return null;
    }
    if (cs.getValues().get(ChangeSetPersister.ID_KEY) == null) {
      // Not yet persistent
      return null;
    }
    Object o = cs.getValues().get(ChangeSetPersister.ID_KEY);
    return o;
  }

  @Override
  public Object persistState(Class<? extends ChangeSetBacked> entityClass,
      ChangeSet cs) throws DataAccessException {
    log.debug("Flush: changeset: " + cs.getValues().keySet());

    String collName = getCollectionNameForEntity(entityClass);
    DBCollection dbc = mongoTemplate.getCollection(collName);
    if (dbc == null) {
      dbc = mongoTemplate.createCollection(collName);
    }
    for (String key : cs.getValues().keySet()) {
      if (key != null && !key.startsWith("_") && !key.equals(ChangeSetPersister.ID_KEY)) {
        Object value = cs.getValues().get(key);
        final DBObject dbQuery = new BasicDBObject();
        dbQuery.put(ENTITY_ID, cs.getValues().get(ChangeSetPersister.ID_KEY));
        dbQuery.put(ENTITY_CLASS, entityClass.getName());
        dbQuery.put(ENTITY_FIELD_NAME, key);
        dbQuery.put(ENTITY_FIELD_CLASS, value.getClass().getName());
        DBObject dbId = mongoTemplate.execute(collName,
            new CollectionCallback<DBObject>() {
              @Override
              public DBObject doInCollection(DBCollection collection)
                  throws MongoException, DataAccessException {
                return collection.findOne(dbQuery);
              }
            });
        final DBObject dbDoc = new BasicDBObject();
        mongoTemplate.getConverter().write(value, dbDoc);
        dbDoc.putAll(dbQuery);
        if (dbId != null) {
          dbDoc.put("_id", dbId.get("_id"));
        }
        mongoTemplate.execute(collName, new CollectionCallback<Object>() {
          @Override
          public Object doInCollection(DBCollection collection)
              throws MongoException, DataAccessException {
            collection.save(dbDoc);
            return null;
          }
        });
      }
    }
    return 0L;
  }

  private String getCollectionNameForEntity(
      Class<? extends ChangeSetBacked> entityClass) {
    return ClassUtils.getQualifiedName(entityClass);
  }

}
