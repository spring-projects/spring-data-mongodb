/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.crossstore;

import javax.persistence.EntityManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.persistence.ChangeSet;
import org.springframework.data.persistence.ChangeSetBacked;
import org.springframework.data.persistence.ChangeSetPersister;
import org.springframework.util.ClassUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class MongoChangeSetPersister implements ChangeSetPersister<Object> {

	private static final String ENTITY_CLASS = "_entity_class";

	private static final String ENTITY_ID = "_entity_id";

	private static final String ENTITY_FIELD_NAME = "_entity_field_name";

	private static final String ENTITY_FIELD_CLASS = "_entity_field_class";

	protected final Log log = LogFactory.getLog(getClass());

	private MongoTemplate mongoTemplate;

	private EntityManagerFactory entityManagerFactory;

	public void setMongoTemplate(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	public void setEntityManagerFactory(EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}


	public void getPersistentState(Class<? extends ChangeSetBacked> entityClass,
			Object id, final ChangeSet changeSet)
					throws DataAccessException, NotFoundException {

		if (id == null) {
			log.debug("Unable to load MongoDB data for null id");
			return;
		}

		String collName = getCollectionNameForEntity(entityClass);

		final DBObject dbk = new BasicDBObject();
		dbk.put(ENTITY_ID, id);
		dbk.put(ENTITY_CLASS, entityClass.getName());
		if (log.isDebugEnabled()) {
			log.debug("Loading MongoDB data for " + dbk);
		}
		mongoTemplate.execute(collName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection)
					throws MongoException, DataAccessException {
				for (DBObject dbo : collection.find(dbk)) {
					String key = (String) dbo.get(ENTITY_FIELD_NAME);
					if (log.isDebugEnabled()) {
						log.debug("Processing key: " + key);
					}
					if (!changeSet.getValues().containsKey(key)) {
						String className = (String) dbo.get(ENTITY_FIELD_CLASS);
						if (className == null) {
							throw new DataIntegrityViolationException(
									"Unble to convert property " + key
									+ ": Invalid metadata, " + ENTITY_FIELD_CLASS + " not available");
						}
						Class<?> clazz = ClassUtils.resolveClassName(className, ClassUtils.getDefaultClassLoader());
						Object value = mongoTemplate.getConverter().read(clazz, dbo);
						if (log.isDebugEnabled()) {
							log.debug("Adding to ChangeSet: " + key);
						}
						changeSet.set(key, value);
					}
				}
				return null;
			}
		});
	}

	public Object getPersistentId(ChangeSetBacked entity, ChangeSet cs) throws DataAccessException {
		log.debug("getPersistentId called on " + entity);
		if (entityManagerFactory == null) {
			throw new DataAccessResourceFailureException("EntityManagerFactory cannot be null");
		}
		Object o = entityManagerFactory.getPersistenceUnitUtil().getIdentifier(entity);
		return o;
	}

	public Object persistState(ChangeSetBacked entity, ChangeSet cs) throws DataAccessException {
		if (cs == null) {
			log.debug("Flush: changeset was null, nothing to flush.");
			return 0L;
		}

		if (log.isDebugEnabled()) {
			log.debug("Flush: changeset: " + cs.getValues());
		}

		String collName = getCollectionNameForEntity(entity.getClass());
		if (mongoTemplate.getCollection(collName) == null) {
			mongoTemplate.createCollection(collName);
		}

		for (String key : cs.getValues().keySet()) {
			if (key != null && !key.startsWith("_") && !key.equals(ChangeSetPersister.ID_KEY)) {
				Object value = cs.getValues().get(key);
				final DBObject dbQuery = new BasicDBObject();
				dbQuery.put(ENTITY_ID, getPersistentId(entity, cs));
				dbQuery.put(ENTITY_CLASS, entity.getClass().getName());
				dbQuery.put(ENTITY_FIELD_NAME, key);
				DBObject dbId = mongoTemplate.execute(collName,
						new CollectionCallback<DBObject>() {
					public DBObject doInCollection(DBCollection collection)
							throws MongoException, DataAccessException {
						return collection.findOne(dbQuery);
					}
				});
				if (value == null) {
					if (log.isDebugEnabled()) {
						log.debug("Flush: removing: " + dbQuery);
					}
					mongoTemplate.execute(collName, new CollectionCallback<Object>() {
						public Object doInCollection(DBCollection collection)
								throws MongoException, DataAccessException {
							collection.remove(dbQuery);
							return null;
						}
					});
				}
				else {
					final DBObject dbDoc = new BasicDBObject();
					dbDoc.putAll(dbQuery);
					if (log.isDebugEnabled()) {
						log.debug("Flush: saving: " + dbQuery);
					}
					mongoTemplate.getConverter().write(value, dbDoc);
					dbDoc.put(ENTITY_FIELD_CLASS, value.getClass().getName());
					if (dbId != null) {
						dbDoc.put("_id", dbId.get("_id"));
					}
					mongoTemplate.execute(collName, new CollectionCallback<Object>() {
						public Object doInCollection(DBCollection collection)
								throws MongoException, DataAccessException {
							collection.save(dbDoc);
							return null;
						}
					});
				}
			}
		}
		return 0L;
	}

	private String getCollectionNameForEntity(Class<? extends ChangeSetBacked> entityClass) {
		return ClassUtils.getQualifiedName(entityClass);
	}

}
