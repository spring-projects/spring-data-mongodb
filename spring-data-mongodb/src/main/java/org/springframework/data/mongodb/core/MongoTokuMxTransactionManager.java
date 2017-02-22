package org.springframework.data.mongodb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.transaction.*;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.util.Assert;

import com.mongodb.*;

/**
 * Implementation of {@link PlatformTransactionManager} for TokuMX, a MongoDB Engine from Tokutek.
 * 
 * @author Lars Uffmann
 */
public class MongoTokuMxTransactionManager extends AbstractPlatformTransactionManager implements InitializingBean {

	private Logger logger = LoggerFactory.getLogger(MongoTokuMxTransactionManager.class);
	private MongoDbFactory mongoDbFactory;
	private boolean requestEnsureConnection;

	public MongoTokuMxTransactionManager() {}

	/**
	 * Constructor accepting the {@link MongoDbFactory} to use.
	 * 
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public MongoTokuMxTransactionManager(MongoDbFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	public MongoDbFactory getMongoDbFactory() {
		return mongoDbFactory;
	}

	/**
	 * Set the {@link MongoDbFactory} to use.
	 * 
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public void setMongoDbFactory(MongoDbFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	public boolean isRequestEnsureConnection() {
		return requestEnsureConnection;
	}

	/**
	 * Whether to call db.requestEnsureConnection() on transaction begin.
	 * 
	 * @see http://docs.mongodb.org/ecosystem/drivers/java-concurrency/
	 * @param requestEnsureConnection
	 */
	public void setRequestEnsureConnection(boolean requestEnsureConnection) {
		this.requestEnsureConnection = requestEnsureConnection;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongoDbFactory, "mongoDbFactory must not be null");
		if (!(mongoDbFactory instanceof SimpleMongoDbFactory)) {
			// SimpleMongoDbFactory handles transaction resource synchronisation
			throw new IllegalArgumentException("SimpleMongoDbFactory required for transaction synchronization");
		}
		DBObject command = new BasicDBObject("serverStatus", Boolean.TRUE);
		CommandResult result = null;
		try {
			result = mongoDbFactory.getDb().command(command);
		} catch (RuntimeException ex) {
			// use MongoExceptionTranslator?
			throw new TransactionSystemException("error getting serverStatus: " + ex.getMessage(), ex);
		}
		String version = result.getString("tokumxVersion");
		Assert.notNull(version, "Server does not run tokumx");
		logger.info("tokuMx mongodb transaction manager initialized. TokuMx version {} detected.", version);
	}

	@Override
	protected Object doGetTransaction() throws TransactionException {
		MongoDBTransactionObject txo = new MongoDBTransactionObject();
		// SimpleMongoDbFactory handles transaction resource synchronisation
		DbHolder holder = new DbHolder(mongoDbFactory.getDb());
		txo.setHolder(holder);
		logger.trace("tokuMx.doGetTransaction: " + holder.getDB().getName());
		return txo;
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder().add("beginTransaction", Boolean.TRUE);
		switch (definition.getIsolationLevel()) {
			case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
				builder.append("isolation", "readUncommitted");
				break;
			case TransactionDefinition.ISOLATION_SERIALIZABLE:
				builder.append("isolation", "serializable");
				break;
			case TransactionDefinition.ISOLATION_DEFAULT:
				builder.append("isolation", "mvcc");
				break;
			default:
				throw new InvalidIsolationLevelException("the requested isolation level " + definition.getIsolationLevel()
						+ " is not supported by tokuMx.");
		}
		MongoDBTransactionObject mtx = (MongoDBTransactionObject) transaction;
		DBObject command = builder.get();
		CommandResult result = null;
		DB mongoDB = null;
		try {
			mongoDB = mtx.getHolder().getDB();
			mongoDB.requestStart();
			if (isRequestEnsureConnection()) {
				mongoDB.requestEnsureConnection();
			}
			result = mongoDB.command(command);
		} catch (RuntimeException ex) {
			MongoDbUtils.closeDB(mongoDB);
			// use MongoExceptionTranslator?
			throw new TransactionSystemException("tokuMx.doBegin: unexpected system exception: " + ex.getMessage(), ex);
		}
		String error = result.getErrorMessage();
		if (error != null) {
			MongoDbUtils.closeDB(mongoDB);
			throw new CannotCreateTransactionException("execution of " + command.toString() + " failed: " + error);
		}
		logger.trace("tokuMx.doBegin: {}", command);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		MongoDBTransactionObject mtx = (MongoDBTransactionObject) status.getTransaction();
		DBObject command = new BasicDBObject("commitTransaction", Boolean.TRUE);
		CommandResult result = null;
		DB mongoDB = null;
		try {
			mongoDB = mtx.getHolder().getDB();
			result = mongoDB.command(command);
		} catch (RuntimeException ex) {
			// use MongoExceptionTranslator?
			throw new TransactionSystemException("tokuMx.doCommit: unexpected system exception: " + ex.getMessage(), ex);
		} finally {
			MongoDbUtils.closeDB(mongoDB);
		}
		String error = result.getErrorMessage();
		if (error != null) {
			throw new TransactionSystemException("tokuMx.doCommit: execution of " + command.toString() + " failed: " + error);
		}
		logger.trace("tokuMx.doCommit: {}", command);
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		MongoDBTransactionObject mtx = (MongoDBTransactionObject) status.getTransaction();
		DBObject command = new BasicDBObject("rollbackTransaction", Boolean.TRUE);
		CommandResult result = null;
		DB mongoDB = null;
		try {
			mongoDB = mtx.getHolder().getDB();
			result = mongoDB.command(command);
		} catch (RuntimeException ex) {
			// use MongoExceptionTranslator?
			throw new TransactionSystemException("tokuMx.doRollback: unexpected system exception: " + ex.getMessage(), ex);
		} finally {
			MongoDbUtils.closeDB(mongoDB);
		}
		String error = result.getErrorMessage();
		if (error != null) {
			throw new TransactionSystemException("tokuMx.doRollback: execution of " + command.toString() + " failed: "
					+ error);
		}
		logger.trace("tokuMx.doRollback: {}", command);
	}

	private static class MongoDBTransactionObject implements SmartTransactionObject {
		private DbHolder holder;

		private DbHolder getHolder() {
			return holder;
		}

		private void setHolder(DbHolder holder) {
			this.holder = holder;
		}

		@Override
		public boolean isRollbackOnly() {
			return this.holder.isRollbackOnly();
		}

		@Override
		public void flush() {
			// no-op
		}
	}

}
