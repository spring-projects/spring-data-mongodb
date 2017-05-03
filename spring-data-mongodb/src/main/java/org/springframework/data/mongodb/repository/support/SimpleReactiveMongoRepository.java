/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.springframework.data.mongodb.core.query.Criteria.*;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Reactive repository base implementation for Mongo.
 *
 * @author Mark Paluch
 * @author Oliver Gierke
 * @since 2.0
 */
@RequiredArgsConstructor
public class SimpleReactiveMongoRepository<T, ID extends Serializable> implements ReactiveMongoRepository<T, ID> {

	private final @NonNull MongoEntityInformation<T, ID> entityInformation;
	private final @NonNull ReactiveMongoOperations mongoOperations;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(java.lang.Object)
	 */
	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(reactor.core.publisher.Mono)
	 */
	@Override
	public Mono<T> findById(Mono<ID> mono) {

		Assert.notNull(mono, "The given id must not be null!");

		return mono.flatMap(
				id -> mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(reactor.core.publisher.Mono)
	 */
	@Override
	public Mono<Boolean> existsById(Mono<ID> mono) {

		Assert.notNull(mono, "The given id must not be null!");

		return mono.flatMap(id -> mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName()));

	}

	@Override
	public Flux<T> findAll() {
		return findAll(new Query());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(java.lang.Iterable)
	 */
	@Override
	public Flux<T> findAllById(Iterable<ID> ids) {

		Assert.notNull(ids, "The given Iterable of Id's must not be null!");

		return findAll(new Query(new Criteria(entityInformation.getIdAttribute())
				.in(Streamable.of(ids).stream().collect(StreamUtils.toUnmodifiableList()))));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<T> findAllById(Publisher<ID> ids) {

		Assert.notNull(ids, "The given Publisher of Id's must not be null!");

		return Flux.from(ids).buffer().flatMap(this::findAllById);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveSortingRepository#findAll(org.springframework.data.domain.Sort)
	 */
	@Override
	public Flux<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.ReactiveMongoRepository#findAll(org.springframework.data.domain.Example, org.springframework.data.domain.Sort)
	 */
	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null!");
		Assert.notNull(sort, "Sort must not be null!");

		Query query = new Query(new Criteria().alike(example)).with(sort);

		return mongoOperations.find(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.ReactiveMongoRepository#findAll(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> Flux<S> findAll(Example<S> example) {
		return findAll(example, Sort.unsorted());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
	 */
	public Mono<Long> count() {
		return mongoOperations.count(new Query(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.ReactiveMongoRepository#insert(java.lang.Object)
	 */
	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		return mongoOperations.insert(entity, entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.ReactiveMongoRepository#insert(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		List<S> source = Streamable.of(entities).stream().collect(StreamUtils.toUnmodifiableList());

		return source.isEmpty() ? Flux.empty() : Flux.from(mongoOperations.insertAll(source));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.ReactiveMongoRepository#insert(org.reactivestreams.Publisher)
	 */
	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entities) {

		Assert.notNull(entities, "The given Publisher of entities must not be null!");

		return Flux.from(entities).flatMap(entity -> mongoOperations.insert(entity, entityInformation.getCollectionName()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(java.lang.Object)
	 */
	@Override
	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		if (entityInformation.isNew(entity)) {
			return mongoOperations.insert(entity, entityInformation.getCollectionName());
		}

		return mongoOperations.save(entity, entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		Streamable<S> source = Streamable.of(entities);

		return source.stream().allMatch(it -> entityInformation.isNew(it)) ? //
				mongoOperations.insertAll(source.stream().collect(Collectors.toList())) : //
				Flux.fromIterable(entities).flatMap(this::save);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(org.reactivestreams.Publisher)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null!");

		return Flux.from(entityStream)
				.flatMap(entity -> entityInformation.isNew(entity) ? //
						mongoOperations.insert(entity, entityInformation.getCollectionName()).then(Mono.just(entity)) : //
						mongoOperations.save(entity, entityInformation.getCollectionName()).then(Mono.just(entity)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations
				.remove(getIdQuery(id), entityInformation.getJavaType(), entityInformation.getCollectionName()).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
	 */
	@Override
	public Mono<Void> delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null!");

		return deleteById(entityInformation.getRequiredId(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(java.lang.Iterable)
	 */
	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		return Flux.fromIterable(entities).flatMap(entity -> deleteById(entityInformation.getRequiredId(entity))).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null!");

		return Flux.from(entityStream)//
				.map(it -> entityInformation.getRequiredId(it))//
				.flatMap(this::deleteById)//
				.then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
	 */
	public Mono<Void> deleteAll() {
		return mongoOperations.remove(new Query(), entityInformation.getCollectionName()).then(Mono.empty());
	}

	public <S extends T> Mono<Boolean> exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.exists(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	public <S extends T> Mono<S> findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.findOne(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	public <S extends T> Mono<Long> count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.count(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	private Query getIdQuery(Object id) {
		return new Query(getIdCriteria(id));
	}

	private Criteria getIdCriteria(Object id) {
		return where(entityInformation.getIdAttribute()).is(id);
	}

	private Flux<T> findAll(Query query) {

		if (query == null) {
			return Flux.empty();
		}

		return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}
}
