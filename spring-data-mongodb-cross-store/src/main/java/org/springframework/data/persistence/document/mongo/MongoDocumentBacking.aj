package org.springframework.data.persistence.document.mongo;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Transient;
import javax.persistence.Entity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.FieldSignature;

import org.springframework.dao.DataAccessException;
import org.springframework.data.document.annotation.RelatedDocument;

import org.springframework.data.persistence.document.DocumentBacked;
import org.springframework.data.persistence.document.DocumentBackedTransactionSynchronization;
import org.springframework.data.persistence.ChangeSet;
import org.springframework.data.persistence.ChangeSetBacked;
import org.springframework.data.persistence.ChangeSetPersister;
import org.springframework.data.persistence.ChangeSetPersister.NotFoundException;
import org.springframework.data.persistence.HashMapChangeSet;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Aspect to turn an object annotated with @Document into a persistent document
 * using Mongo.
 * 
 * @author Thomas Risberg
 */
public aspect MongoDocumentBacking {

  private static final Log LOGGER = LogFactory
      .getLog(MongoDocumentBacking.class);

  // Aspect shared config
  private ChangeSetPersister<Object> changeSetPersister;

  public void setChangeSetPersister(
      ChangeSetPersister<Object> changeSetPersister) {
    this.changeSetPersister = changeSetPersister;
  }

  // ITD to introduce N state to Annotated objects
  declare parents : (@Entity *) implements DocumentBacked;

  // The annotated fields that will be persisted in MongoDB rather than with JPA
  declare @field: @RelatedDocument * (@Entity+ *).*:@Transient;

  // -------------------------------------------------------------------------
  // Advise user-defined constructors of ChangeSetBacked objects to create a new
  // backing ChangeSet
  // -------------------------------------------------------------------------
  pointcut arbitraryUserConstructorOfChangeSetBackedObject(DocumentBacked entity) : 
		execution((DocumentBacked+).new(..)) &&
		!execution((DocumentBacked+).new(ChangeSet)) &&
		this(entity);

  pointcut finderConstructorOfChangeSetBackedObject(DocumentBacked entity,
      ChangeSet cs) : 
		execution((DocumentBacked+).new(ChangeSet)) &&
		this(entity) && 
		args(cs);

  protected pointcut entityFieldGet(DocumentBacked entity) :
        get(@RelatedDocument * DocumentBacked+.*) &&
        this(entity) &&
        !get(* DocumentBacked.*);

  protected pointcut entityFieldSet(DocumentBacked entity, Object newVal) :
        set(@RelatedDocument * DocumentBacked+.*) &&
        this(entity) &&
        args(newVal) &&
        !set(* DocumentBacked.*);

  // intercept EntityManager.merge calls
  public pointcut entityManagerMerge(EntityManager em, Object entity) : 
    call(* EntityManager.merge(Object)) &&
    target(em) &&
    args(entity);
  
  // intercept EntityManager.remove calls
//  public pointcut entityManagerRemove(EntityManager em, Object entity) : 
//    call(* EntityManager.remove(Object)) &&
//    target(em) &&
//    args(entity);

  // move changeSet from detached entity to the newly merged persistent object
  Object around(EntityManager em, Object entity) : entityManagerMerge(em, entity) {
    Object mergedEntity = proceed(em, entity);
    if (entity instanceof DocumentBacked && mergedEntity instanceof DocumentBacked) {
      ((DocumentBacked)mergedEntity).changeSet = ((DocumentBacked)entity).getChangeSet();
    }
    return mergedEntity;
  }

  // clear changeSet from removed entity
//  Object around(EntityManager em, Object entity) : entityManagerRemove(em, entity) {
//    if (entity instanceof DocumentBacked) {
//      removeChangeSetValues((DocumentBacked)entity);
//    }
//    return proceed(em, entity);
//  }

  private static void removeChangeSetValues(DocumentBacked entity) {
    LOGGER.debug("Removing all change-set values for " + entity);	
	ChangeSet nulledCs = new HashMapChangeSet();
	DocumentBacked documentEntity = (DocumentBacked) entity;
	@SuppressWarnings("unchecked")
	ChangeSetPersister<Object> changeSetPersister = (ChangeSetPersister<Object>)documentEntity.itdChangeSetPersister;
	try {
        changeSetPersister.getPersistentState(
            documentEntity.getClass(),
            documentEntity.get_persistent_id(), 
            documentEntity.getChangeSet());
	} 
	catch (DataAccessException e) {} 
	catch (NotFoundException e) {}
	for (String key :entity.getChangeSet().getValues().keySet()) {
        nulledCs.set(key, null);
	}
	entity.setChangeSet(nulledCs);
  }

  before(DocumentBacked entity) : arbitraryUserConstructorOfChangeSetBackedObject(entity) {
    LOGGER
        .debug("User-defined constructor called on DocumentBacked object of class "
            + entity.getClass());
    // Populate all ITD fields
    entity.setChangeSet(new HashMapChangeSet());
    entity.itdChangeSetPersister = changeSetPersister;
    entity.itdTransactionSynchronization = 
        new DocumentBackedTransactionSynchronization(changeSetPersister, entity);
    //registerTransactionSynchronization(entity);
  }

  private static void registerTransactionSynchronization(DocumentBacked entity) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      if (!TransactionSynchronizationManager.getSynchronizations().contains(entity.itdTransactionSynchronization)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Adding transaction synchronization for " + entity);
        }
        TransactionSynchronizationManager.registerSynchronization(entity.itdTransactionSynchronization);
      }
      else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Transaction synchronization already active for " + entity);
        }
      }
    }
    else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Transaction synchronization is not active for " + entity);
      }
    }
  }

  // -------------------------------------------------------------------------
  // ChangeSet-related mixins
  // -------------------------------------------------------------------------
  // Introduced field
  @Transient private ChangeSet DocumentBacked.changeSet;

  @Transient private ChangeSetPersister<?> DocumentBacked.itdChangeSetPersister;

  @Transient private DocumentBackedTransactionSynchronization DocumentBacked.itdTransactionSynchronization;

  public void DocumentBacked.setChangeSet(ChangeSet cs) {
    this.changeSet = cs;
  }

  public ChangeSet DocumentBacked.getChangeSet() {
    return changeSet;
  }

  // Flush the entity state to the persistent store
  public void DocumentBacked.flush() {
    Object id = itdChangeSetPersister.getPersistentId(this, this.changeSet);
    itdChangeSetPersister.persistState(this, this.changeSet);
  }

  public Object DocumentBacked.get_persistent_id() {
    return itdChangeSetPersister.getPersistentId(this, this.changeSet);
  }

  // lifecycle methods
  @javax.persistence.PostPersist public void DocumentBacked.itdPostPersist() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("JPA lifecycle event PrePersist: " + this.getClass().getName());
    }
    registerTransactionSynchronization(this);
  }
  @javax.persistence.PreUpdate public void DocumentBacked.itdPreUpdate() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("JPA lifecycle event PreUpdate: " + this.getClass().getName() + " :: " + this);
    }
    registerTransactionSynchronization(this);
  }
  @javax.persistence.PostUpdate public void DocumentBacked.itdPostUpdate() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("JPA lifecycle event PostUpdate: " + this.getClass().getName() + " :: " + this);
    }
    registerTransactionSynchronization(this);
  }
  @javax.persistence.PostRemove public void DocumentBacked.itdPostRemove() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("JPA lifecycle event PostRemove: " + this.getClass().getName() + " :: " + this);
    }
    registerTransactionSynchronization(this);
    removeChangeSetValues(this);
  }
  @javax.persistence.PostLoad public void DocumentBacked.itdPostLoad() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("JPA lifecycle event PostLoad: " + this.getClass().getName() + " :: " + this);
    }
    registerTransactionSynchronization(this);
  }
  
  /**
   * delegates field reads to the state accessors instance
   */
  Object around(DocumentBacked entity): entityFieldGet(entity) {
    Field f = field(thisJoinPoint);
    String propName = f.getName();
    LOGGER.trace("GET " + f + " -> ChangeSet value property [" + propName
        + "] using: " + entity.getChangeSet());
    if (entity.getChangeSet().getValues().get(propName) == null) {
      try {
        this.changeSetPersister.getPersistentState(entity.getClass(),
            entity.get_persistent_id(), entity.getChangeSet());
      } catch (NotFoundException e) {
      }
    }
    Object fValue = entity.getChangeSet().getValues().get(propName);
    if (fValue != null) {
      return fValue;
    }
    return proceed(entity);
  }

  /**
   * delegates field writes to the state accessors instance
   */
  Object around(DocumentBacked entity, Object newVal) : entityFieldSet(entity, newVal) {
    Field f = field(thisJoinPoint);
    String propName = f.getName();
    LOGGER.trace("SET " + f + " -> ChangeSet number value property [" + propName
        + "] with value=[" + newVal + "]");
    entity.getChangeSet().set(propName, newVal);
    return proceed(entity, newVal);
  }

  Field field(JoinPoint joinPoint) {
    FieldSignature fieldSignature = (FieldSignature) joinPoint.getSignature();
    return fieldSignature.getField();
  }
}
