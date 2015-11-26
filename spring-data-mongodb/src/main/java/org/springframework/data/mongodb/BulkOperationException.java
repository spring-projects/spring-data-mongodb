/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb;

import java.util.List;

import org.springframework.dao.DataAccessException;

import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteResult;

/**
 * Is thrown when errors occur during bulk operations.
 * 
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @since 1.9
 */
public class BulkOperationException extends DataAccessException {

	private static final long serialVersionUID = 73929601661154421L;

	private final List<BulkWriteError> errors;
	private final BulkWriteResult result;

	/**
	 * Creates a new {@link BulkOperationException} with the given message and source {@link BulkWriteException}.
	 * 
	 * @param message must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 */
	public BulkOperationException(String message, BulkWriteException source) {

		super(message, source);

		this.errors = source.getWriteErrors();
		this.result = source.getWriteResult();
	}

	public List<BulkWriteError> getErrors() {
		return errors;
	}

	public BulkWriteResult getResult() {
		return result;
	}
}
