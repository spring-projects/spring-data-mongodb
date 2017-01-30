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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive repository base implementation for Mongo.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class SimpleReactiveMongoRepository<T, ID extends Serializable> implements ReactiveMongoRepository<T, ID> {

	private final ReactiveMongoOperations mongoOperations;
	private final MongoEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleReactiveMongoRepository} for the given {@link MongoEntityInformation} and
	 * {@link ReactiveMongoOperations}.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public SimpleReactiveMongoRepository(MongoEntityInformation<T, ID> metadata,
			ReactiveMongoOperations mongoOperations) {

		Assert.notNull(metadata, "MongoEntityInformation must not be null!");
		Assert.notNull(mongoOperations, "ReactiveMongoOperations must not be null!");

		this.entityInformation = metadata;
		this.mongoOperations = mongoOperations;
	}

	public Mono<T> findOne(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	public Mono<T> findOne(Mono<ID> mono) {

		Assert.notNull(mono, "The given id must not be null!");

		return mono.then(
				id -> mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName()));
	}

	public <S extends T> Mono<S> findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.findOne(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	public Mono<Boolean> exists(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName());
	}

	public Mono<Boolean> exists(Mono<ID> mono) {

		Assert.notNull(mono, "The given id must not be null!");

		return mono.then(id -> mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName()));

	}

	public <S extends T> Mono<Boolean> exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.exists(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public Flux<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public Flux<T> findAll(Iterable<ID> ids) {

		Assert.notNull(ids, "The given Iterable of Id's must not be null!");

		Set<ID> parameters = new HashSet<ID>(tryDetermineRealSizeOrReturn(ids, 10));
		for (ID id : ids) {
			parameters.add(id);
		}

		return findAll(new Query(new Criteria(entityInformation.getIdAttribute()).in(parameters)));
	}

	@Override
	public Flux<T> findAll(Publisher<ID> idStream) {

		Assert.notNull(idStream, "The given Publisher of Id's must not be null!");

		return Flux.from(idStream).buffer().flatMap(this::findAll);
	}

	@Override
	public Flux<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));

		if (sort != null) {
			q.with(sort);
		}

		return mongoOperations.find(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example) {
		return findAll(example, null);
	}

	public Mono<Long> count() {
		return mongoOperations.count(new Query(), entityInformation.getCollectionName());
	}

	public <S extends T> Mono<Long> count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.count(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		return mongoOperations.insert(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		List<S> list = convertIterableToList(entities);

		if (list.isEmpty()) {
			return Flux.empty();
		}

		return Flux.from(mongoOperations.insertAll(list));
	}

	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entities) {

		Assert.notNull(entities, "The given Publisher of entities must not be null!");

		return Flux.from(entities).flatMap(entity -> mongoOperations.insert(entity, entityInformation.getCollectionName()));
	}

	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		if (entityInformation.isNew(entity)) {
			return mongoOperations.insert(entity, entityInformation.getCollectionName());
		}

		return mongoOperations.save(entity, entityInformation.getCollectionName());
	}

	public <S extends T> Flux<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		List<S> result = convertIterableToList(entities);
		boolean allNew = true;

		for (S entity : entities) {
			if (allNew && !entityInformation.isNew(entity)) {
				allNew = false;
			}
		}

		if (allNew) {
			return Flux.from(mongoOperations.insertAll(result));
		}

		List<Mono<S>> monos = new ArrayList<>();
		for (S entity : result) {
			monos.add(save(entity));
		}

		return Flux.merge(monos);
	}

	@Override
	public <S extends T> Flux<S> save(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null!");

		return Flux.from(entityStream).flatMap(entity -> {

			if (entityInformation.isNew(entity)) {
				return mongoOperations.insert(entity, entityInformation.getCollectionName()).then(aVoid -> Mono.just(entity));
			}

			return mongoOperations.save(entity, entityInformation.getCollectionName()).then(aVoid -> Mono.just(entity));
		});
	}

	// TODO: should this one really be void?
	public Mono<Void> delete(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations
				.remove(getIdQuery(id), entityInformation.getJavaType(), entityInformation.getCollectionName()).then();

	}

	// TODO: should this one really be void?
	public Mono<Void> delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null!");

		return delete(entityInformation.getId(entity).get());
	}

	// TODO: should this one really be void?
	public Mono<Void> delete(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");

		return Flux.fromIterable(entities).flatMap(entity -> delete(entityInformation.getId(entity).get())).then();
	}

	// TODO: should this one really be void?
	@Override
	public Mono<Void> delete(Publisher<? extends T> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null!");

		return Flux.from(entityStream).flatMap(entity -> delete(entityInformation.getId(entity).get())).then();
	}

	// TODO: should this one really be void?
	public Mono<Void> deleteAll() {
		return mongoOperations.remove(new Query(), entityInformation.getCollectionName())
				.then(deleteResult -> Mono.empty());
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

	private static <T> List<T> convertIterableToList(Iterable<T> entities) {

		if (entities instanceof List) {
			return (List<T>) entities;
		}

		int capacity = tryDetermineRealSizeOrReturn(entities, 10);

		if (capacity == 0 || entities == null) {
			return Collections.emptyList();
		}

		List<T> list = new ArrayList<T>(capacity);
		for (T entity : entities) {
			list.add(entity);
		}

		return list;
	}

	private static int tryDetermineRealSizeOrReturn(Iterable<?> iterable, int defaultSize) {
		return iterable == null ? 0 : (iterable instanceof Collection) ? ((Collection<?>) iterable).size() : defaultSize;
	}

}
