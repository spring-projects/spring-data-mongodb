package org.springframework.data.persistence.document.mongo;

import java.lang.reflect.Field;

import javax.persistence.Transient;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.FieldSignature;

import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.document.mongodb.mapping.Document;

import org.springframework.data.persistence.document.DocumentBacked;
import org.springframework.data.persistence.document.DocumentBackedTransactionSynchronization;
import org.springframework.data.persistence.ChangeSet;
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

//  declare @type: DocumentBacked+: @Configurable;

  declare @field: @Document * (@Entity+ *).*:@Transient;
  declare @field: ChangeSet (DocumentBacked+).*:@Transient;
  declare @field: ChangeSetPersister (DocumentBacked+).*:@Transient;

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
        get(@Document * DocumentBacked+.*) &&
        this(entity) &&
        !get(* DocumentBacked.*);

  protected pointcut entityFieldSet(DocumentBacked entity, Object newVal) :
        set(@Document * DocumentBacked+.*) &&
        this(entity) &&
        args(newVal) &&
        !set(* DocumentBacked.*);

//  protected pointcut entityIdSet(DocumentBacked entity, Object newVal) :
//        set(@Id * DocumentBacked+.*) &&
//        this(entity) &&
//        args(newVal) &&
//        !set(* DocumentBacked.*);

  before(DocumentBacked entity) : arbitraryUserConstructorOfChangeSetBackedObject(entity) {
    LOGGER
        .debug("User-defined constructor called on DocumentBacked object of class "
            + entity.getClass());
    entity.itdChangeSetPersister = changeSetPersister;
    // Populate all properties
    ChangeSet changeSet = new HashMapChangeSet();
    // changeSetManager.populateChangeSet(changeSet, entity);
    entity.setChangeSet(changeSet);
    if (!TransactionSynchronizationManager.isSynchronizationActive()) {
      throw new InvalidDataAccessResourceUsageException(
          "No transaction synchronization is active");
    }
    TransactionSynchronizationManager
        .registerSynchronization(new DocumentBackedTransactionSynchronization(
            changeSetPersister, entity));
  }

  // -------------------------------------------------------------------------
  // ChangeSet-related mixins
  // -------------------------------------------------------------------------
  // Introduced field
  private ChangeSet DocumentBacked.changeSet;

  private ChangeSetPersister<?> DocumentBacked.itdChangeSetPersister;

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

//  /**
//   * delegates field writes to the state accessors instance
//   */
//  Object around(DocumentBacked entity, Object newVal) : entityIdSet(entity, newVal) {
//    Field f = field(thisJoinPoint);
//    String propName = f.getName();
//    LOGGER.trace("SET @Id -> ChangeSet @Id property [" + propName
//        + "] with value=[" + newVal + "]");
//    entity.getChangeSet().set("_id", newVal);
//    return proceed(entity, newVal);
//  }

  Field field(JoinPoint joinPoint) {
    FieldSignature fieldSignature = (FieldSignature) joinPoint.getSignature();
    return fieldSignature.getField();
  }
}
