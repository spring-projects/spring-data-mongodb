/*
 * Copyright 2010-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.ExecutableFindOperation;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ReadPreference;
import com.mongodb.client.result.DeleteResult;

/**
 * Repository base implementation for Mongo.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Mehran Behnam
 * @author Jens Schauder
 */
public class SimpleMongoRepository<T, ID> implements MongoRepository<T, ID> {

	private @Nullable CrudMethodMetadata crudMethodMetadata;
	private final MongoEntityInformation<T, ID> entityInformation;
	private final MongoOperations mongoOperations;

	/**
	 * Creates a new {@link SimpleMongoRepository} for the given {@link MongoEntityInformation} and {@link MongoTemplate}.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public SimpleMongoRepository(MongoEntityInformation<T, ID> metadata, MongoOperations mongoOperations) {

		Assert.notNull(metadata, "MongoEntityInformation must not be null");
		Assert.notNull(mongoOperations, "MongoOperations must not be null");

		this.entityInformation = metadata;
		this.mongoOperations = mongoOperations;
	}

	// -------------------------------------------------------------------------
	// Methods from CrudRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		if (entityInformation.isNew(entity)) {
			return mongoOperations.insert(entity, entityInformation.getCollectionName());
		}

		return mongoOperations.save(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> List<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null");

		Streamable<S> source = Streamable.of(entities);
		boolean allNew = source.stream().allMatch(entityInformation::isNew);

		if (allNew) {

			List<S> result = source.stream().collect(Collectors.toList());
			return new ArrayList<>(mongoOperations.insert(result, entityInformation.getCollectionName()));
		}

		return source.stream().map(this::save).collect(Collectors.toList());
	}

	@Override
	public Optional<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		Query query = getIdQuery(id);
		getReadPreference().ifPresent(query::withReadPreference);

		return Optional.ofNullable(
				mongoOperations.findOne(query, entityInformation.getJavaType(), entityInformation.getCollectionName()));
	}

	@Override
	public boolean existsById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		Query query = getIdQuery(id);
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.exists(query, entityInformation.getJavaType(),
				entityInformation.getCollectionName());
	}

	@Override
	public List<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids) {

		Assert.notNull(ids, "The given Ids of entities not be null");

		return findAll(getIdQuery(ids));
	}

	@Override
	public long count() {

		Query query = new Query();
		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.count(query, entityInformation.getCollectionName());
	}

	@Override
	public void deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		Query query = getIdQuery(id);
		getReadPreference().ifPresent(query::withReadPreference);
		mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	@Override
	public void delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null");

		DeleteResult deleteResult = mongoOperations.remove(entity, entityInformation.getCollectionName());

		if (entityInformation.isVersioned() && deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 0) {
			throw new OptimisticLockingFailureException(String.format(
					"The entity with id %s with version %s in %s cannot be deleted; Was it modified or deleted in the meantime",
					entityInformation.getId(entity), entityInformation.getVersion(entity),
					entityInformation.getCollectionName()));
		}
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids) {

		Assert.notNull(ids, "The given Iterable of ids must not be null");

		Query query = getIdQuery(ids);
		getReadPreference().ifPresent(query::withReadPreference);
		mongoOperations.remove(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		entities.forEach(this::delete);
	}

	@Override
	public void deleteAll() {

		Query query = new Query();
		getReadPreference().ifPresent(query::withReadPreference);

		mongoOperations.remove(query, entityInformation.getCollectionName());
	}

	// -------------------------------------------------------------------------
	// Methods from PagingAndSortingRepository
	// -------------------------------------------------------------------------

	@Override
	public Page<T> findAll(Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be null");

		long count = count();
		List<T> list = findAll(new Query().with(pageable));

		return new PageImpl<>(list, pageable, count);
	}

	@Override
	public List<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		Query query = new Query().with(sort);
		return findAll(query);
	}

	// -------------------------------------------------------------------------
	// Methods from MongoRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> S insert(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		return mongoOperations.insert(entity, entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> List<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null");

		Collection<S> list = toCollection(entities);

		if (list.isEmpty()) {
			return Collections.emptyList();
		}

		return new ArrayList<>(mongoOperations.insertAll(list));
	}

	// -------------------------------------------------------------------------
	// Methods from QueryByExampleExecutor
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Optional<S> findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());
		getReadPreference().ifPresent(query::withReadPreference);

		return Optional
				.ofNullable(mongoOperations.findOne(query, example.getProbeType(), entityInformation.getCollectionName()));
	}

	@Override
	public <S extends T> List<S> findAll(Example<S> example) {
		return findAll(example, Sort.unsorted());
	}

	@Override
	public <S extends T> List<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null");
		Assert.notNull(sort, "Sort must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation()) //
				.with(sort);
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.find(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> Page<S> findAll(Example<S> example, Pageable pageable) {

		Assert.notNull(example, "Sample must not be null");
		Assert.notNull(pageable, "Pageable must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation()).with(pageable); //
		getReadPreference().ifPresent(query::withReadPreference);

		List<S> list = mongoOperations.find(query, example.getProbeType(), entityInformation.getCollectionName());

		return PageableExecutionUtils.getPage(list, pageable, () -> mongoOperations
				.count(Query.of(query).limit(-1).skip(-1), example.getProbeType(), entityInformation.getCollectionName()));
	}

	@Override
	public <S extends T> long count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.count(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T> boolean exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null");

		Query query = new Query(new Criteria().alike(example)) //
				.collation(entityInformation.getCollation());
		getReadPreference().ifPresent(query::withReadPreference);

		return mongoOperations.exists(query, example.getProbeType(), entityInformation.getCollectionName());
	}

	@Override
	public <S extends T, R> R findBy(Example<S> example,
			Function<org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery<S>, R> queryFunction) {

		Assert.notNull(example, "Sample must not be null");
		Assert.notNull(queryFunction, "Query function must not be null");

		return queryFunction.apply(new FluentQueryByExample<>(example, example.getProbeType()));
	}

	// -------------------------------------------------------------------------
	// Utility methods
	// -------------------------------------------------------------------------

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

		Query query = new Query(new Criteria(entityInformation.getIdAttribute()).in(toCollection(ids)));
		getReadPreference().ifPresent(query::withReadPreference);
		return query;
	}

	private static <E> Collection<E> toCollection(Iterable<E> ids) {
		return ids instanceof Collection<E> collection ? collection
				: StreamUtils.createStreamFromIterator(ids.iterator()).collect(Collectors.toList());
	}

	private List<T> findAll(@Nullable Query query) {

		if (query == null) {
			return Collections.emptyList();
		}

		getReadPreference().ifPresent(query::withReadPreference);
		return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery} using {@link Example}.
	 *
	 * @author Mark Paluch
	 * @since 3.3
	 */
	class FluentQueryByExample<S, T> extends FetchableFluentQuerySupport<Example<S>, T> {

		FluentQueryByExample(Example<S> example, Class<T> resultType) {
			this(example, Sort.unsorted(), 0, resultType, Collections.emptyList());
		}

		FluentQueryByExample(Example<S> example, Sort sort, int limit, Class<T> resultType, List<String> fieldsToInclude) {
			super(example, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		protected <R> FluentQueryByExample<S, R> create(Example<S> predicate, Sort sort, int limit, Class<R> resultType,
				List<String> fieldsToInclude) {
			return new FluentQueryByExample<>(predicate, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		public T oneValue() {
			return createQuery().oneValue();
		}

		@Override
		public T firstValue() {
			return createQuery().firstValue();
		}

		@Override
		public List<T> all() {
			return createQuery().all();
		}

		@Override
		public Window<T> scroll(ScrollPosition scrollPosition) {
			return createQuery().scroll(scrollPosition);
		}

		@Override
		public Page<T> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null");

			List<T> list = createQuery(q -> q.with(pageable)).all();

			return PageableExecutionUtils.getPage(list, pageable, this::count);
		}

		@Override
		public Stream<T> stream() {
			return createQuery().stream();
		}

		@Override
		public long count() {
			return createQuery().count();
		}

		@Override
		public boolean exists() {
			return createQuery().exists();
		}

		private ExecutableFindOperation.TerminatingFind<T> createQuery() {
			return createQuery(UnaryOperator.identity());
		}

		private ExecutableFindOperation.TerminatingFind<T> createQuery(UnaryOperator<Query> queryCustomizer) {

			Query query = new Query(new Criteria().alike(getPredicate())) //
					.collation(entityInformation.getCollation());

			if (getSort().isSorted()) {
				query.with(getSort());
			}

			query.limit(getLimit());

			if (!getFieldsToInclude().isEmpty()) {
				query.fields().include(getFieldsToInclude().toArray(new String[0]));
			}

			getReadPreference().ifPresent(query::withReadPreference);

			query = queryCustomizer.apply(query);

			return mongoOperations.query(getPredicate().getProbeType()).inCollection(entityInformation.getCollectionName())
					.as(getResultType()).matching(query);
		}

	}
}
