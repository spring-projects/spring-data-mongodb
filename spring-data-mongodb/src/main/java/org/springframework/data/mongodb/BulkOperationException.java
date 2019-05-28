/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessException;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteResult;
import com.mongodb.BulkWriteUpsert;
import com.mongodb.MongoBulkWriteException;

/**
 * Is thrown when errors occur during bulk operations.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Jacob Botuck
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

	public BulkOperationException(String message, MongoBulkWriteException source) {
		super(message, source);

		this.errors = source.getWriteErrors().stream().map(error -> new BulkWriteError(error.getCode(), error.getMessage(),
				new BasicDBObject(error.getDetails()), error.getIndex())).collect(Collectors.toList());
		this.result = new BulkWriteResult() { // Create a child of BulkWriteResult that delegates to an instance of the
																					// newer BulkWriteResult
			@Override
			public boolean isAcknowledged() {
				return source.getWriteResult().wasAcknowledged();
			}

			@Override
			public int getInsertedCount() {
				return source.getWriteResult().getInsertedCount();
			}

			@Override
			public int getMatchedCount() {
				return source.getWriteResult().getMatchedCount();
			}

			@Override
			public int getRemovedCount() {
				return source.getWriteResult().getDeletedCount();
			}

			@Override
			public boolean isModifiedCountAvailable() {
				return true;
			}

			@Override
			public int getModifiedCount() {
				return source.getWriteResult().getModifiedCount();
			}

			@Override
			public List<BulkWriteUpsert> getUpserts() {
				return source.getWriteResult().getUpserts().stream()
						.map(upsert -> new BulkWriteUpsert(upsert.getIndex(), upsert.getId())).collect(Collectors.toList());
			}
		};
	}

	public List<BulkWriteError> getErrors() {
		return errors;
	}

	public BulkWriteResult getResult() {
		return result;
	}
}
