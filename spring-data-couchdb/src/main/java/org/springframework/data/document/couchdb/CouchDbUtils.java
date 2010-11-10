/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.data.document.couchdb;

import org.jcouchdb.exception.CouchDBException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.UncategorizedDocumentStoreException;

/**
 * Helper class featuring helper methods for internal MongoDb classes.
 *
 * <p>Mainly intended for internal use within the framework.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class CouchDbUtils {

	/**
	 * Convert the given runtime exception to an appropriate exception from the
	 * <code>org.springframework.dao</code> hierarchy.
	 * Return null if no translation is appropriate: any other exception may
	 * have resulted from user code, and should not be translated.
	 * @param ex runtime exception that occurred
	 * @return the corresponding DataAccessException instance,
	 * or <code>null</code> if the exception should not be translated
	 */
	public static DataAccessException translateCouchExceptionIfPossible(RuntimeException ex) {

		// Check for well-known MongoException subclasses.
		
		// All other MongoExceptions
		if (ex instanceof CouchDBException) {
			return new UncategorizedDocumentStoreException(ex.getMessage(), ex);
		}
		
		// If we get here, we have an exception that resulted from user code,
		// rather than the persistence provider, so we return null to indicate
		// that translation should not occur.
		return null;				
	}

}
