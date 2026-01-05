/*
 * Copyright 2023-present the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.springframework.lang.Nullable;
import org.springframework.transaction.annotation.Transactional;

/**
 * Helper class for integration tests of {@link Transactional#label()} MongoDb options in non-reactive context.
 *
 * @param <T> root document type
 * @author Yan Kardziyaka
 * @see org.springframework.data.mongodb.ReactiveTransactionOptionsTestService
 */
public class TransactionOptionsTestService<T> {

	private final Function<Object, T> findByIdFunction;
	private final UnaryOperator<T> saveFunction;

	public TransactionOptionsTestService(MongoOperations operations, Class<T> entityClass) {
		this.findByIdFunction = id -> operations.findById(id, entityClass);
		this.saveFunction = operations::save;
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:maxCommitTime=-PT6H3M" })
	public T saveWithInvalidMaxCommitTime(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:maxCommitTime=PT1M" })
	public T saveWithinMaxCommitTime(T entity) {
		return saveFunction.apply(entity);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=available" })
	public T availableReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=invalid" })
	public T invalidReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=${tx.read.concern}" })
	public T environmentReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=majority" })
	public T majorityReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readPreference=primaryPreferred" })
	public T findFromPrimaryPreferredReplica(Object id) {
		return findByIdFunction.apply(id);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readPreference=invalid" })
	public T findFromInvalidReplica(Object id) {
		return findByIdFunction.apply(id);
	}

	@Nullable
	@Transactional(transactionManager = "txManager", label = { "mongo:readPreference=primary" })
	public T findFromPrimaryReplica(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:writeConcern=unacknowledged" })
	public T unacknowledgedWriteConcernSave(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:writeConcern=invalid" })
	public T invalidWriteConcernSave(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:writeConcern=acknowledged" })
	public T acknowledgedWriteConcernSave(T entity) {
		return saveFunction.apply(entity);
	}
}
