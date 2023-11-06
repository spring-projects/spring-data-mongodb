/*
 * Copyright 2016-2023 the original author or authors.
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
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveFindOperation;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ReadPreference;
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

	private @Nullable CrudMethodMetadata crudMethodMetadata;
	private final MongoEntityInformation<T, ID> entityInformation;
	private final ReactiveMongoOperations mongoOperations;

	/**
	 * Creates a new {@link SimpleReactiveMongoRepository} for the given {@link MongoEntityInformation} and
	 * {@link MongoTemplate}.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public SimpleReactiveMongoRepository(MongoEntityInformation<T, ID> entityInformation,
			ReactiveMongoOperations mongoOperations) {

		Assert.notNull(entityInformation, "EntityInformation must not be null");
		Assert.notNull(mongoOperations, "MongoOperations must not be null");

		this.entityInformation = entityInformation;
		this.mongoOperations = mongoOperations;
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveCrudRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		if (entityInformation.isNew(entity)) {
			return mongoOperations.insert(entity, entityInformation.getCollectionName());
		}

		return mongoOperations.save(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		Streamable<S> source = Streamable.of(entities);

		return source.stream().allMatch(entityInformation::isNew) ? //
				mongoOperations.insert(source.stream().collect(Collectors.toList()), entityInformation.getCollectionName()) : //
				Flux.fromIterable(entities).flatMap(this::save);
	}

	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return Flux.from(entityStream).flatMapSequential(entity -> entityInformation.isNew(entity) ? //
				mongoOperations.insert(entity, entityInformation.getCollectionName()) : //
				mongoOperations.save(entity, entityInformation.getCollectionName()));
	}

	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		Query query = getIdQuery(id);
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.findOne(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	@Override
	public Mono<T> findById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The given id must not be null");
		Optional<ReadPreference> readPreference = getReadPreference();

		return Mono.from(publisher).flatMap(id -> {
			Query query = getIdQuery(id);
			readPreference.ifPresent(query::withReadPreference);
			return mongoOperations.findOne(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
		});
	}

	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		Query query = getIdQuery(id);
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.exists(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The given id must not be null");
		Optional<ReadPreference> readPreference = getReadPreference();

		return Mono.from(publisher).flatMap(id -> {
			Query query = getIdQuery(id);
			readPreference.ifPresent(query::withReadPreference);
			return mongoOperations.exists(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
		});
	}

	@Override
	public Flux<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public Flux<T> findAllById(Iterable<ID> ids) {

		Assert.notNull(ids, "The given Iterable of Id's must not be null");

		return findAll(getIdQuery(ids));
	}

	@Override
	public Flux<T> findAllById(Publisher<ID> ids) {

		Assert.notNull(ids, "The given Publisher of Id's must not be null");

		Optional<ReadPreference> readPreference = getReadPreference();
		return Flux.from(ids).buffer().flatMapSequential(listOfIds -> {
			Query query = getIdQuery(listOfIds);
			readPreference.ifPresent(query::withReadPreference);
			return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
		});
	}

	@Override
	public Mono<Long> count() {

		Query query = new Query();
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.count(query, entityInformation.getCollectionName());
	}

	@Override
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return deleteById(id, getReadPreference());
	}

	private Mono<Void> deleteById(ID id, Optional<ReadPreference> readPreference) {

		Assert.notNull(id, "The given id must not be null");

		Query query = getIdQuery(id);
		readPreference.ifPresent(query::withReadPreference);
		return mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName()).then();
	}

	@Override
	public Mono<Void> deleteById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "Id must not be null");

		Optional<ReadPreference> readPreference = getReadPreference();

		return Mono.from(publisher).flatMap(id -> {
			Query query = getIdQuery(id);
			readPreference.ifPresent(query::withReadPreference);
			return mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
		}).then();
	}

	@Override
	public Mono<Void> delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null");

		Mono<DeleteResult> remove = mongoOperations.remove(entity, entityInformation.getCollectionName());

		if (entityInformation.isVersioned()) {

			remove = remove.handle((deleteResult, sink) -> {

				if (deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 0) {
					sink.error(new OptimisticLockingFailureException(String.format(
							"The entity with id %s with version %s in %s cannot be deleted; Was it modified or deleted in the meantime",
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

		Assert.notNull(ids, "The given Iterable of Id's must not be null");

		Query query = getIdQuery(ids);
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName()).then();
	}

	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		Collection<?> idCollection = StreamUtils.createStreamFromIterator(entities.iterator()).map(entityInformation::getId)
				.collect(Collectors.toList());

		Criteria idsInCriteria = where(entityInformation.getIdAttribute()).in(idCollection);

		Query query = new Query(idsInCriteria);
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName()).then();
	}

	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		Optional<ReadPreference> readPreference = getReadPreference();
		return Flux.from(entityStream)//
				.map(entityInformation::getRequiredId)//
				.flatMap(id -> deleteById(id, readPreference))//
				.then();
	}

	@Override
	public Mono<Void> deleteAll() {
		Query query = new Query();
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.remove(query, entityInformation.getCollectionName()).then(Mono.empty());
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveSortingRepository
	// -------------------------------------------------------------------------

	@Override
	public Flux<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		return findAll(new Query().with(sort));
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveMongoRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		return mongoOperations.insert(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		Collection<S> source = toCollection(entities);

		return source.isEmpty() ? Flux.empty() : mongoOperations.insertAll(source);
	}

	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entities) {

		Assert.notNull(entities, "The given Publisher of entities must not be null");

		return Flux.from(entities)
				.flatMapSequential(entity -> mongoOperations.insert(entity, entityInformation.getCollectionName()));
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveMongoRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation()) //
				.limit(2);
		getReadPreference().ifPresent(query::withReadPreference);

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

		Assert.notNull(example, "Example must not be null");

		return findAll(example, Sort.unsorted());
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null");
		Assert.notNull(sort, "Sort must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation()) //
				.with(sort);
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.find(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Mono<Long> count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.count(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Mono<Boolean> exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.exists(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T, R, P extends Publisher<R>> P findBy(Example<S> example,
			Function<FluentQuery.ReactiveFluentQuery<S>, P> queryFunction) {

		Assert.notNull(example, "Sample must not be null");
		Assert.notNull(queryFunction, "Query function must not be null");

		return queryFunction
				.apply(new ReactiveFluentQueryByExample<>(example, example.getProbeType(), getReadPreference()));
	}

	/**
	 * Configures a custom {@link CrudMethodMetadata} to be used to detect {@link ReadPreference}s and query hints to be
	 * applied to queries.
	 *
	 * @param crudMethodMetadata
	 * @since 4.2
	 */
	void setRepositoryMethodMetadata(CrudMethodMetadata crudMethodMetadata) {
		this.crudMethodMetadata = crudMethodMetadata;
	}

	private Optional<ReadPreference> getReadPreference() {

		if (crudMethodMetadata == null) {
			return Optional.empty();
		}

		return crudMethodMetadata.getReadPreference();
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
		return ids instanceof Collection<E> collection ? collection
				: StreamUtils.createStreamFromIterator(ids.iterator()).collect(Collectors.toList());
	}

	private Flux<T> findAll(Query query) {

		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.ReactiveFluentQuery} using {@link Example}.
	 *
	 * @author Mark Paluch
	 * @since 3.3
	 */
	class ReactiveFluentQueryByExample<S, T> extends ReactiveFluentQuerySupport<Example<S>, T> {

		private final Optional<ReadPreference> readPreference;

		ReactiveFluentQueryByExample(Example<S> example, Class<T> resultType, Optional<ReadPreference> readPreference) {
			this(example, Sort.unsorted(), 0, resultType, Collections.emptyList(), readPreference);
		}

		ReactiveFluentQueryByExample(Example<S> example, Sort sort, int limit, Class<T> resultType,
				List<String> fieldsToInclude, Optional<ReadPreference> readPreference) {
			super(example, sort, limit, resultType, fieldsToInclude);
			this.readPreference = readPreference;
		}

		@Override
		protected <R> ReactiveFluentQueryByExample<S, R> create(Example<S> predicate, Sort sort, int limit,
				Class<R> resultType, List<String> fieldsToInclude) {
			return new ReactiveFluentQueryByExample<>(predicate, sort, limit, resultType, fieldsToInclude, readPreference);
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
		public Mono<Window<T>> scroll(ScrollPosition scrollPosition) {
			return createQuery().scroll(scrollPosition);
		}

		@Override
		public Mono<Page<T>> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null");

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

			query.limit(getLimit());

			if (!getFieldsToInclude().isEmpty()) {
				query.fields().include(getFieldsToInclude().toArray(new String[0]));
			}

			readPreference.ifPresent(query::withReadPreference);

			query = queryCustomizer.apply(query);

			return mongoOperations.query(getPredicate().getProbeType()).inCollection(entityInformation.getCollectionName())
					.as(getResultType()).matching(query);
		}

	}
}
