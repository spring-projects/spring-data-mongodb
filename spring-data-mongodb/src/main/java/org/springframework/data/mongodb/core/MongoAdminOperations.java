package org.springframework.data.mongodb.core;

import org.springframework.jmx.export.annotation.ManagedOperation;

public interface MongoAdminOperations {

	@ManagedOperation
	public abstract void dropDatabase(String databaseName);

	@ManagedOperation
	public abstract void createDatabase(String databaseName);

	@ManagedOperation
	public abstract String getDatabaseStats(String databaseName);

}