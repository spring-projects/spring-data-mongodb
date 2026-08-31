/*
 * Copyright 2015-present the original author or authors.
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

import java.io.Serial;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.util.Lazy;
import org.springframework.util.NumberUtils;

import com.mongodb.ClientBulkWriteException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;

/**
 * Is thrown when errors occur during bulk operations.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.9
 */
public class BulkOperationException extends DataAccessException {

	@Serial
	private static final long serialVersionUID = 73929601661154421L;

	private final Lazy<List<BulkWriteError>> errors;
	private final Lazy<BulkWriteResult> result;

	/**
	 * Creates a new {@link BulkOperationException} with the given message and source {@link MongoBulkWriteException}.
	 *
	 * @param message can be {@literal null}.
	 * @param source must not be {@literal null}.
	 */
	public BulkOperationException(@Nullable String message, MongoBulkWriteException source) {

		super(message, source);

		this.errors = Lazy.of(source.getWriteErrors());
		this.result = Lazy.of(source.getWriteResult());
	}

	/**
	 * @param message
	 * @param source
	 */
	public BulkOperationException(@Nullable String message, ClientBulkWriteException source) {
		super(message, source);

		this.errors = Lazy.of(() -> source.getWriteErrors().values().stream()
				.map(error -> new BulkWriteError(error.getCode(), error.getMessage(), error.getDetails(), -1)).toList());
		this.result = Lazy.of(() -> convertToBulkWriteResult(source.getPartialResult()));
	}

	public List<BulkWriteError> getErrors() {
		return errors.get();
	}

	public BulkWriteResult getResult() {
		return result.get();
	}

	private static BulkWriteResult convertToBulkWriteResult(Optional<ClientBulkWriteResult> source) {

		if (source.isEmpty()) {
			return BulkWriteResult.unacknowledged();
		}

		ClientBulkWriteResult clientBulkWriteResult = source.get();
		if (!clientBulkWriteResult.isAcknowledged()) {
			return BulkWriteResult.unacknowledged();
		}

		return new BulkWriteResult() {

			@Override
			public boolean wasAcknowledged() {
				return true;
			}

			@Override
			public int getInsertedCount() {
				return NumberUtils.convertNumberToTargetClass(clientBulkWriteResult.getInsertedCount(), Integer.class);
			}

			@Override
			public int getMatchedCount() {
				return NumberUtils.convertNumberToTargetClass(clientBulkWriteResult.getMatchedCount(), Integer.class);
			}

			@Override
			public int getDeletedCount() {
				return NumberUtils.convertNumberToTargetClass(clientBulkWriteResult.getDeletedCount(), Integer.class);
			}

			@Override
			public int getModifiedCount() {
				return NumberUtils.convertNumberToTargetClass(clientBulkWriteResult.getModifiedCount(), Integer.class);
			}

			@Override
			public List<BulkWriteInsert> getInserts() {
				throw new UnsupportedOperationException();
			}

			@Override
			public List<BulkWriteUpsert> getUpserts() {
				throw new UnsupportedOperationException();
			}
		};
	}

}
