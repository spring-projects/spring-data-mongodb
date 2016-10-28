/*
 * Copyright 2010-2016 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.BsonInvalidOperationException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.util.MongoDbErrorCodes;
import org.springframework.util.ClassUtils;

import com.mongodb.BulkWriteException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteError;

/**
 * Simple {@link PersistenceExceptionTranslator} for Mongo. Convert the given runtime exception to an appropriate
 * exception from the {@code org.springframework.dao} hierarchy. Return {@literal null} if no translation is
 * appropriate: any other exception may have resulted from user code, and should not be translated.
 * 
 * @author Oliver Gierke
 * @author Michal Vich
 * @author Christoph Strobl
 */
public class MongoExceptionTranslator implements PersistenceExceptionTranslator {

	private static final Set<String> DULICATE_KEY_EXCEPTIONS = new HashSet<String>(
			Arrays.asList("MongoException.DuplicateKey", "DuplicateKeyException"));

	private static final Set<String> RESOURCE_FAILURE_EXCEPTIONS = new HashSet<String>(
			Arrays.asList("MongoException.Network", "MongoSocketException", "MongoException.CursorNotFound",
					"MongoCursorNotFoundException", "MongoServerSelectionException", "MongoTimeoutException"));

	private static final Set<String> RESOURCE_USAGE_EXCEPTIONS = new HashSet<String>(
			Arrays.asList("MongoInternalException"));

	private static final Set<String> DATA_INTEGRETY_EXCEPTIONS = new HashSet<String>(
			Arrays.asList("WriteConcernException", "MongoWriteException", "MongoBulkWriteException"));

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {

		// Check for well-known MongoException subclasses.

		if (ex instanceof BsonInvalidOperationException) {
			throw new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
		}

		String exception = ClassUtils.getShortName(ClassUtils.getUserClass(ex.getClass()));

		if (DULICATE_KEY_EXCEPTIONS.contains(exception)) {
			return new DuplicateKeyException(ex.getMessage(), ex);
		}

		if (RESOURCE_FAILURE_EXCEPTIONS.contains(exception)) {
			return new DataAccessResourceFailureException(ex.getMessage(), ex);
		}

		if (RESOURCE_USAGE_EXCEPTIONS.contains(exception)) {
			return new InvalidDataAccessResourceUsageException(ex.getMessage(), ex);
		}

		if (DATA_INTEGRETY_EXCEPTIONS.contains(exception)) {

			if (ex instanceof MongoServerException) {
				if (((MongoServerException) ex).getCode() == 11000) {
					return new DuplicateKeyException(ex.getMessage(), ex);
				}
				if (ex instanceof MongoBulkWriteException) {
					for (BulkWriteError x : ((MongoBulkWriteException) ex).getWriteErrors()) {
						if (x.getCode() == 11000) {
							return new DuplicateKeyException(ex.getMessage(), ex);
						}
					}
				}
			}

			return new DataIntegrityViolationException(ex.getMessage(), ex);
		}

		if (ex instanceof BulkWriteException) {
			return new BulkOperationException(ex.getMessage(), (BulkWriteException) ex);
		}

		// All other MongoExceptions
		if (ex instanceof MongoException) {

			int code = ((MongoException) ex).getCode();

			if (MongoDbErrorCodes.isDuplicateKeyCode(code)) {
				throw new DuplicateKeyException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isDataAccessResourceFailureCode(code)) {
				throw new DataAccessResourceFailureException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isInvalidDataAccessApiUsageCode(code) || code == 10003 || code == 12001
					|| code == 12010 || code == 12011 || code == 12012) {
				throw new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isPermissionDeniedCode(code)) {
				throw new PermissionDeniedDataAccessException(ex.getMessage(), ex);
			}
			return new UncategorizedMongoDbException(ex.getMessage(), ex);
		}

		// If we get here, we have an exception that resulted from user code,
		// rather than the persistence provider, so we return null to indicate
		// that translation should not occur.
		return null;
	}
}
