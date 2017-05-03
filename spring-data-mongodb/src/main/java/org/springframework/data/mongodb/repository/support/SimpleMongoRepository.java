/*
 * Copyright 2010-2017 the original author or authors.
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Repository base implementation for Mongo.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class SimpleMongoRepository<T, ID extends Serializable> implements MongoRepository<T, ID> {

	private final MongoOperations mongoOperations;
	private final MongoEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleMongoRepository} for the given {@link MongoEntityInformation} and {@link MongoTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public SimpleMongoRepository(MongoEntityInformation<T, ID> metadata, MongoOperations mongoOperations) {

		Assert.notNull(metadata, "MongoEntityInformation must not be null!");
		Assert.notNull(mongoOperations, "MongoOperations must not be null!");

		this.entityInformation = metadata;
		this.mongoOperations = mongoOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
	 */
	@Override
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		if (entityInformation.isNew(entity)) {
			mongoOperations.insert(entity, entityInformation.getCollectionName());
		} else {
			mongoOperations.save(entity, entityInformation.getCollectionName());
		}

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	public <S extends T> List<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		Streamable<S> source = Streamable.of(entities);
		boolean allNew = source.stream().allMatch(it -> entityInformation.isNew(it));

		if (allNew) {

			List<S> result = source.stream().collect(Collectors.toList());
			mongoOperations.insertAll(result);
			return result;

		} else {
			return source.stream().map(this::save).collect(Collectors.toList());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	@Override
	public Optional<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return Optional.ofNullable(
				mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#existsById(java.lang.Object)
	 */
	@Override
	public boolean existsById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		return mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	@Override
	public long count() {
		return mongoOperations.getCollection(entityInformation.getCollectionName()).count();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	public void deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		mongoOperations.remove(getIdQuery(id), entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	@Override
	public void delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null!");

		deleteById(entityInformation.getRequiredId(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	@Override
	public void deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		entities.forEach(this::delete);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteAll()
	 */
	@Override
	public void deleteAll() {
		mongoOperations.remove(new Query(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	@Override
	public List<T> findAll() {
		return findAll(new Query());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAllById(java.lang.Iterable)
	 */
	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {

		return findAll(new Query(new Criteria(entityInformation.getIdAttribute())
				.in(Streamable.of(ids).stream().collect(StreamUtils.toUnmodifiableList()))));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Pageable)
	 */
	@Override
	public Page<T> findAll(Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be null!");

		Long count = count();
		List<T> list = findAll(new Query().with(pageable));

		return new PageImpl<T>(list, pageable, count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
	 */
	@Override
	public List<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		return findAll(new Query().with(sort));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Object)
	 */
	@Override
	public <S extends T> S insert(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		mongoOperations.insert(entity, entityInformation.getCollectionName());
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Iterable)
	 */
	@Override
	public <S extends T> List<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> list = Streamable.of(entities).stream().collect(StreamUtils.toUnmodifiableList());

		if (list.isEmpty()) {
			return list;
		}

		mongoOperations.insertAll(list);
		return list;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#findAllByExample(org.springframework.data.domain.Example, org.springframework.data.domain.Pageable)
	 */
	@Override
	public <S extends T> Page<S> findAll(final Example<S> example, Pageable pageable) {

		Assert.notNull(example, "Sample must not be null!");
		Assert.notNull(pageable, "Pageable must not be null!");

		Query q = new Query(new Criteria().alike(example)).with(pageable);
		List<S> list = mongoOperations.find(q, example.getProbeType(), entityInformation.getCollectionName());

		return PageableExecutionUtils.getPage(list, pageable,
				() -> mongoOperations.count(q, example.getProbeType(), entityInformation.getCollectionName()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#findAllByExample(org.springframework.data.domain.Example, org.springframework.data.domain.Sort)
	 */
	@Override
	public <S extends T> List<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null!");
		Assert.notNull(sort, "Sort must not be null!");

		Query q = new Query(new Criteria().alike(example)).with(sort);

		return mongoOperations.find(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#findAllByExample(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> List<S> findAll(Example<S> example) {
		return findAll(example, Sort.unsorted());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findOne(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> S findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.findOne(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#count(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> long count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.count(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#exists(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> boolean exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.exists(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	private Query getIdQuery(Object id) {
		return new Query(getIdCriteria(id));
	}

	private Criteria getIdCriteria(Object id) {
		return where(entityInformation.getIdAttribute()).is(id);
	}

	private List<T> findAll(Query query) {

		if (query == null) {
			return Collections.emptyList();
		}

		return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}
}
