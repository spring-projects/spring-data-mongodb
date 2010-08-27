package org.springframework.datastore.document.mongodb;

import org.springframework.dao.DataAccessResourceFailureException;

public class CannotGetMongoDbConnectionException extends DataAccessResourceFailureException {

	public CannotGetMongoDbConnectionException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
