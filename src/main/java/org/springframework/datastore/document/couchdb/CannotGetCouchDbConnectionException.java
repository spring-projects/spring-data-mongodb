package org.springframework.datastore.document.couchdb;

import org.springframework.dao.DataAccessResourceFailureException;

public class CannotGetCouchDbConnectionException extends DataAccessResourceFailureException {

	public CannotGetCouchDbConnectionException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
