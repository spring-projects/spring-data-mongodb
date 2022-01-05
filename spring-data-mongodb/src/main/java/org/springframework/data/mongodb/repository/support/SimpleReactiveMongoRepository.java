/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveFindOperation;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

import com.mongodb.client.result.DeleteResult;

/**
 * Reactive repository base implementation for Mongo.
 *
 * @author Mark Paluch
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Ruben J Garcia
 * @author Jens Schauder
 * @author Clément Petit
 * @since 2.0
 */
public class SimpleReactiveMongoRepository<T, ID extends Serializable> implements ReactiveMongoRepository<T, ID> {

	private final MongoEntityInformation<T, ID> entityInformation;
	private final ReactiveMongoOperations mongoOperations;

	public SimpleReactiveMongoRepository(MongoEntityInformation<T, ID> entityInformation,
			ReactiveMongoOperations mongoOperations) {

		Assert.notNull(entityInformation, "EntityInformation must not be null!");
		Assert.notNull(mongoOperations, "MongoOperations must not be null!");

		this.entityInformation = entityInformation;
		this.mongoOperations = mongoOperations;
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveCrudRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		if (entityInformation.isNew(entity)) {
			return mongoOperations.insert(entity, entityInformation.getCollectionName());
		}

		return mongoOperations.save(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		Streamable<S> source = Streamable.of(entities);

		return source.stream().allMatch(entityInformation::isNew) ? //
				mongoOperations.insert(source.stream().collect(Collectors.toList()), entityInformation.getCollectionName()) : //
				Flux.fromIterable(entities).flatMap(this::save);
	}

	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null!");

		return Flux.from(entityStream).flatMap(entity -> entityInformation.isNew(entity) ? //
				mongoOperations.insert(entity, entityInformation.getCollectionName()) : //
				mongoOperations.save(entity, entityInformation.getCollectionName()));
	}

	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	@Override
	public Mono<T> findById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The given id must not be null!");

		return Mono.from(publisher).flatMap(
				id -> mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName()));
	}

	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName());
	}

	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The given id must not be null!");

		return Mono.from(publisher).flatMap(id -> mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName()));
	}

	@Override
	public Flux<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public Flux<T> findAllById(Iterable<ID> ids) {

		Assert.notNull(ids, "The given Iterable of Id's must not be null!");

		return findAll(getIdQuery(ids));
	}

	@Override
	public Flux<T> findAllById(Publisher<ID> ids) {

		Assert.notNull(ids, "The given Publisher of Id's must not be null!");

		return Flux.from(ids).buffer().flatMap(this::findAllById);
	}

	@Override
	public Mono<Long> count() {
		return mongoOperations.count(new Query(), entityInformation.getCollectionName());
	}

	@Override
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations
				.remove(getIdQuery(id), entityInformation.getJavaType(), entityInformation.getCollectionName()).then();
	}

	@Override
	public Mono<Void> deleteById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "Id must not be null!");

		return Mono.from(publisher).flatMap(id -> mongoOperations.remove(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName())).then();
	}

	@Override
	public Mono<Void> delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null!");

		Mono<DeleteResult> remove = mongoOperations.remove(entity, entityInformation.getCollectionName());

		if (entityInformation.isVersioned()) {

			remove = remove.handle((deleteResult, sink) -> {

				if (deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 0) {
					sink.error(new OptimisticLockingFailureException(String.format(
							"The entity with id %s with version %s in %s cannot be deleted! Was it modified or deleted in the meantime?",
							entityInformation.getId(entity), entityInformation.getVersion(entity),
							entityInformation.getCollectionName())));
				} else {
					sink.next(deleteResult);
				}
			});
		}

		return remove.then();
	}

	@Override
	public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {

		Assert.notNull(ids, "The given Iterable of Id's must not be null!");

		return mongoOperations
				.remove(getIdQuery(ids), entityInformation.getJavaType(), entityInformation.getCollectionName()).then();
	}

	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		Collection<?> idCollection = StreamUtils.createStreamFromIterator(entities.iterator()).map(entityInformation::getId)
				.collect(Collectors.toList());

		Criteria idsInCriteria = where(entityInformation.getIdAttribute()).in(idCollection);

		return mongoOperations
				.remove(new Query(idsInCriteria), entityInformation.getJavaType(), entityInformation.getCollectionName())
				.then();
	}

	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null!");

		return Flux.from(entityStream)//
				.map(entityInformation::getRequiredId)//
				.flatMap(this::deleteById)//
				.then();
	}

	@Override
	public Mono<Void> deleteAll() {
		return mongoOperations.remove(new Query(), entityInformation.getCollectionName()).then(Mono.empty());
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveSortingRepository
	// -------------------------------------------------------------------------

	@Override
	public Flux<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		return findAll(new Query().with(sort));
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveMongoRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		return mongoOperations.insert(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		Collection<S> source = toCollection(entities);

		return source.isEmpty() ? Flux.empty() : mongoOperations.insertAll(source);
	}

	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entities) {

		Assert.notNull(entities, "The given Publisher of entities must not be null!");

		return Flux.from(entities).flatMap(entity -> mongoOperations.insert(entity, entityInformation.getCollectionName()));
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveMongoRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation()) //
				.limit(2);

		return mongoOperations.find(query, example.getProbeType(), entityInformation.getCollectionName()).buffer(2)
				.map(vals -> {

					if (vals.size() > 1) {
						throw new IncorrectResultSizeDataAccessException(1);
					}
					return vals.iterator().next();
				}).next();
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example) {

		Assert.notNull(example, "Example must not be null!");

		return findAll(example, Sort.unsorted());
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null!");
		Assert.notNull(sort, "Sort must not be null!");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation()) //
				.with(sort);

		return mongoOperations.find(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Mono<Long> count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());

		return mongoOperations.count(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Mono<Boolean> exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());

		return mongoOperations.exists(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T, R, P extends Publisher<R>> P findBy(Example<S> example,
			Function<FluentQuery.ReactiveFluentQuery<S>, P> queryFunction) {

		Assert.notNull(example, "Sample must not be null!");
		Assert.notNull(queryFunction, "Query function must not be null!");

		return queryFunction.apply(new ReactiveFluentQueryByExample<>(example, example.getProbeType()));
	}

	private Query getIdQuery(Object id) {
		return new Query(getIdCriteria(id));
	}

	private Criteria getIdCriteria(Object id) {
		return where(entityInformation.getIdAttribute()).is(id);
	}

	private Query getIdQuery(Iterable<? extends ID> ids) {
		return new Query(where(entityInformation.getIdAttribute()).in(toCollection(ids)));
	}

	private static <E> Collection<E> toCollection(Iterable<E> ids) {
		return ids instanceof Collection ? (Collection<E>) ids
				: StreamUtils.createStreamFromIterator(ids.iterator()).collect(Collectors.toList());
	}

	private Flux<T> findAll(Query query) {
		return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.ReactiveFluentQuery} using {@link Example}.
	 *
	 * @author Mark Paluch
	 * @since 3.3
	 */
	class ReactiveFluentQueryByExample<S, T> extends ReactiveFluentQuerySupport<Example<S>, T> {

		ReactiveFluentQueryByExample(Example<S> example, Class<T> resultType) {
			this(example, Sort.unsorted(), resultType, Collections.emptyList());
		}

		ReactiveFluentQueryByExample(Example<S> example, Sort sort, Class<T> resultType, List<String> fieldsToInclude) {
			super(example, sort, resultType, fieldsToInclude);
		}

		@Override
		protected <R> ReactiveFluentQueryByExample<S, R> create(Example<S> predicate, Sort sort, Class<R> resultType,
				List<String> fieldsToInclude) {
			return new ReactiveFluentQueryByExample<>(predicate, sort, resultType, fieldsToInclude);
		}

		@Override
		public Mono<T> one() {
			return createQuery().one();
		}

		@Override
		public Mono<T> first() {
			return createQuery().first();
		}

		@Override
		public Flux<T> all() {
			return createQuery().all();
		}

		@Override
		public Mono<Page<T>> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null!");

			Mono<List<T>> items = createQuery(q -> q.with(pageable)).all().collectList();

			return items.flatMap(content -> ReactivePageableExecutionUtils.getPage(content, pageable, this.count()));
		}

		@Override
		public Mono<Long> count() {
			return createQuery().count();
		}

		@Override
		public Mono<Boolean> exists() {
			return createQuery().exists();
		}

		private ReactiveFindOperation.TerminatingFind<T> createQuery() {
			return createQuery(UnaryOperator.identity());
		}

		private ReactiveFindOperation.TerminatingFind<T> createQuery(UnaryOperator<Query> queryCustomizer) {

			Query query = new Query(new Criteria().alike(getPredicate())) //
					.collation(entityInformation.getCollation());

			if (getSort().isSorted()) {
				query.with(getSort());
			}

			if (!getFieldsToInclude().isEmpty()) {
				query.fields().include(getFieldsToInclude().toArray(new String[0]));
			}

			query = queryCustomizer.apply(query);

			return mongoOperations.query(getPredicate().getProbeType()).inCollection(entityInformation.getCollectionName())
					.as(getResultType()).matching(query);
		}

	}
}
