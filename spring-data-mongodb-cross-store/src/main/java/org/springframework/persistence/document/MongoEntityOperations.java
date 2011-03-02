package org.springframework.persistence.document;

import java.lang.reflect.Field;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.persistence.OrderedEntityOperations;
import org.springframework.persistence.RelatedEntity;
import org.springframework.persistence.support.ChangeSetBacked;

import com.mongodb.DB;

public class MongoEntityOperations extends OrderedEntityOperations<Object, ChangeSetBacked> {
	
	@Autowired
	private DB mongoDb;
	
	@Autowired
	private MongoChangeSetPersister changeSetPersister;

	@Override
	public boolean cacheInEntity() {
		return true;
	}

	@Override
	public ChangeSetBacked findEntity(Class<ChangeSetBacked> entityClass, Object pk) throws DataAccessException {
		throw new UnsupportedOperationException(); 
	}

	@Override
	public Object findUniqueKey(ChangeSetBacked entity) throws DataAccessException {
		return entity.getId();
	}

	@Override
	public boolean isTransactional() {
		// TODO
		return false;
	}

	@Override
	public boolean isTransient(ChangeSetBacked entity) throws DataAccessException {
		return entity.getId() == null;
	}

	@Override
	public Object makePersistent(Object owner, ChangeSetBacked entity, Field f, RelatedEntity fs) throws DataAccessException {
		changeSetPersister.persistState(entity.getClass(), entity.getChangeSet());
		return entity.getId();
	}

	@Override
	public boolean supports(Class<?> entityClass, RelatedEntity fs) {
		return entityClass.isAnnotationPresent(DocumentEntity.class);
	}

}
