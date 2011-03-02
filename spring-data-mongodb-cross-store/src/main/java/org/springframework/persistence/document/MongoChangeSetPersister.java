package org.springframework.persistence.document;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.persistence.support.ChangeSet;
import org.springframework.persistence.support.ChangeSetBacked;
import org.springframework.persistence.support.ChangeSetPersister;
import org.springframework.util.ClassUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

//import edu.emory.mathcs.backport.java.util.Arrays;

public class MongoChangeSetPersister implements ChangeSetPersister<Object> {

	protected final Log log = LogFactory.getLog(getClass());

	@Autowired
	private MongoTemplate mongoTemplate;
	
	@Autowired
	private ConversionService conversionService;
	
	@Override
	public void getPersistentState(Class<? extends ChangeSetBacked> entityClass, Object id, ChangeSet changeSet)
			throws DataAccessException, NotFoundException {
		String collection = ClassUtils.getQualifiedName(entityClass);
		DBObject q = new BasicDBObject();
		q.put("_id", id);
		try {
			DBObject dbo = mongoTemplate.getCollection(collection).findOne(q);
			if (dbo == null) {
				throw new NotFoundException();
			}
			String classShortName = ClassUtils.getShortName(entityClass);
			for (Object property : dbo.toMap().keySet()) {
				String propertyKey = (String) property;
				String propertyName = propertyKey.startsWith(classShortName) ? propertyKey.substring(propertyKey.indexOf(classShortName)
						+ classShortName.length() + 1) : propertyKey;
				// System.err.println("Mongo persisted property [" + propertyName + "] :: " + propertyKey + " = " + dbo.get(propertyKey));
				if (propertyKey.startsWith("_")) {
					// Id or class
					changeSet.set(propertyName, dbo.get(propertyKey));
				} else {
					//throw new IllegalStateException("Unknown property [" + propertyName + "] found in MongoDB store");
					changeSet.set(propertyName, dbo.get(propertyKey));
				}
			}
		} catch (MongoException ex) {
			throw new DataAccessResourceFailureException("Can't read from Mongo", ex);
		}
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
		Object o =  cs.getValues().get(ChangeSetPersister.ID_KEY);
		return o;
	}
	
	@Override
	public Object persistState(Class<? extends ChangeSetBacked> entityClass, ChangeSet cs) throws DataAccessException {
		log.info("PERSIST::"+cs);
		cs.set(CLASS_KEY, entityClass.getName());	
		String idstr = cs.get(ID_KEY, String.class, this.conversionService);
		Object id = null;
		if (idstr != null) {
			id = idstr;
		}
		if (id == null) {
			log.info("Flush: entity make persistent; data store will assign id");
			cs.set("_class", entityClass.getName());
			String collection = entityClass.getName();
			DBCollection dbc = mongoTemplate.getCollection(collection);
			DBObject dbo = mapChangeSetToDbObject(cs);
			if (dbc == null) {
				dbc = mongoTemplate.createCollection(collection);
			}
			dbc.save(dbo);
			id = dbo.get(ID_KEY);
			log.info("Data store assigned id: " + id);
		} else {
			log.info("Flush: entity already persistent with id=" + id);
			String collection = entityClass.getName();
			DBCollection dbc = mongoTemplate.getCollection(collection);
			DBObject dbo = mapChangeSetToDbObject(cs);
			if (dbc == null) {
				throw new DataAccessResourceFailureException("Expected to find a collection named '" + collection +"'. It was not found, so ChangeSet can't be persisted.");
			}
			dbc.save(dbo);
		}

		return 0L;
	}

	private DBObject mapChangeSetToDbObject(ChangeSet cs) {
		BasicDBObject dbo = new BasicDBObject();
		for (String property : cs.getValues().keySet()) {
			dbo.put(property, cs.getValues().get(property));
		}
		return dbo;
	}
}
