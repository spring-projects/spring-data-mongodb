/*
 * Copyright 2023 the original author or authors.
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

import reactor.core.publisher.Mono;

import java.util.function.Function;

import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.transaction.annotation.Transactional;

/**
 * Helper class for integration tests of {@link Transactional#label()} MongoDb options in reactive context.
 *
 * @param <T> root document type
 * @author Yan Kardziyaka
 * @see org.springframework.data.mongodb.core.TransactionOptionsTestService
 */
public class ReactiveTransactionOptionsTestService<T> {
	private final Function<Object, Mono<T>> findByIdFunction;

	private final Function<T, Mono<T>> saveFunction;

	public ReactiveTransactionOptionsTestService(ReactiveMongoOperations operations, Class<T> entityClass) {
		this.findByIdFunction = id -> operations.findById(id, entityClass);
		this.saveFunction = operations::save;
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:maxCommitTime=-PT6H3M" })
	public Mono<T> saveWithInvalidMaxCommitTime(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:maxCommitTime=PT1M" })
	public Mono<T> saveWithinMaxCommitTime(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=available" })
	public Mono<T> availableReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=invalid" })
	public Mono<T> invalidReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:readConcern=majority" })
	public Mono<T> majorityReadConcernFind(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:readPreference=primaryPreferred" })
	public Mono<T> findFromPrimaryPreferredReplica(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:readPreference=invalid" })
	public Mono<T> findFromInvalidReplica(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:readPreference=primary" })
	public Mono<T> findFromPrimaryReplica(Object id) {
		return findByIdFunction.apply(id);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:writeConcern=unacknowledged" })
	public Mono<T> unacknowledgedWriteConcernSave(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:writeConcern=invalid" })
	public Mono<T> invalidWriteConcernSave(T entity) {
		return saveFunction.apply(entity);
	}

	@Transactional(transactionManager = "txManager", label = { "mongo:writeConcern=acknowledged" })
	public Mono<T> acknowledgedWriteConcernSave(T entity) {
		return saveFunction.apply(entity);
	}
}
