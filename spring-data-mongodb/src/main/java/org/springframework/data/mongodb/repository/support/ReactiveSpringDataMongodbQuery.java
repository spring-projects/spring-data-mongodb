/*
 * Copyright 2019-2021 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithProjection;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import com.querydsl.core.JoinExpression;
import com.querydsl.core.QueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Operation;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.mongodb.MongodbOps;
import com.querydsl.mongodb.document.MongodbDocumentSerializer;

/**
 * MongoDB query with utilizing {@link ReactiveMongoOperations} for command execution.
 *
 * @implNote This class uses {@link MongoOperations} to directly convert documents into the target entity type. Also, we
 *           want entites to participate in lifecycle events and entity callbacks.
 * @param <K> result type
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
class ReactiveSpringDataMongodbQuery<K> extends SpringDataMongodbQuerySupport<ReactiveSpringDataMongodbQuery<K>> {

	private final ReactiveMongoOperations mongoOperations;
	private final FindWithProjection<K> find;

	ReactiveSpringDataMongodbQuery(ReactiveMongoOperations mongoOperations, Class<? extends K> entityClass) {
		this(new SpringDataMongodbSerializer(mongoOperations.getConverter()), mongoOperations, entityClass, null);
	}

	@SuppressWarnings("unchecked")
	ReactiveSpringDataMongodbQuery(MongodbDocumentSerializer serializer, ReactiveMongoOperations mongoOperations,
			Class<? extends K> entityClass, @Nullable String collection) {

		super(serializer);

		this.mongoOperations = mongoOperations;
		this.find = StringUtils.hasText(collection) ? mongoOperations.query((Class<K>) entityClass).inCollection(collection)
				: mongoOperations.query((Class<K>) entityClass);
	}

	/**
	 * Fetch all matching query results.
	 *
	 * @return {@link Flux} emitting all query results or {@link Flux#empty()} if there are none.
	 */
	Flux<K> fetch() {
		return createQuery().flatMapMany(it -> find.matching(it).all());
	}

	/**
	 * Fetch the first matching query result.
	 *
	 * @return {@link Mono} emitting the first query result or {@link Mono#empty()} if there are none.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 */
	Mono<K> fetchOne() {
		return createQuery().flatMap(it -> find.matching(it).one());
	}

	/**
	 * Fetch the count of matching query results.
	 *
	 * @return {@link Mono} emitting the first query result count. Emits always a count even item.
	 */
	Mono<Long> fetchCount() {
		return createQuery().flatMap(it -> find.matching(it).count());
	}

	protected Mono<Query> createQuery() {

		QueryMetadata metadata = getQueryMixin().getMetadata();

		return createQuery(createReactiveFilter(metadata), metadata.getProjection(), metadata.getModifiers(),
				metadata.getOrderBy());
	}

	/**
	 * Creates a MongoDB query that is emitted through a {@link Mono} given {@link Mono} of {@link Predicate}.
	 *
	 * @param filter must not be {@literal null}.
	 * @param projection can be {@literal null} if no projection is given. Query requests all fields in such case.
	 * @param modifiers must not be {@literal null}.
	 * @param orderBy must not be {@literal null}.
	 * @return {@link Mono} emitting the {@link Query}.
	 */
	protected Mono<Query> createQuery(Mono<Predicate> filter, @Nullable Expression<?> projection,
			QueryModifiers modifiers, List<OrderSpecifier<?>> orderBy) {

		return filter.map(this::createQuery) //
				.defaultIfEmpty(createQuery(null)) //
				.map(it -> {

					Document fields = createProjection(projection);
					BasicQuery basicQuery = new BasicQuery(it, fields == null ? new Document() : fields);

					Integer limit = modifiers.getLimitAsInteger();
					Integer offset = modifiers.getOffsetAsInteger();

					if (limit != null) {
						basicQuery.limit(limit);
					}
					if (offset != null) {
						basicQuery.skip(offset);
					}
					if (orderBy.size() > 0) {
						basicQuery.setSortObject(createSort(orderBy));
					}

					return basicQuery;
				});
	}

	protected Mono<Predicate> createReactiveFilter(QueryMetadata metadata) {

		if (!metadata.getJoins().isEmpty()) {

			return createReactiveJoinFilter(metadata).map(it -> ExpressionUtils.allOf(metadata.getWhere(), it))
					.switchIfEmpty(Mono.justOrEmpty(metadata.getWhere()));
		}

		return Mono.justOrEmpty(metadata.getWhere());
	}

	/**
	 * Creates a Join filter by querying {@link com.mongodb.DBRef references}.
	 *
	 * @param metadata
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Mono<Predicate> createReactiveJoinFilter(QueryMetadata metadata) {

		MultiValueMap<Expression<?>, Mono<Predicate>> predicates = new LinkedMultiValueMap<>();
		List<JoinExpression> joins = metadata.getJoins();

		for (int i = joins.size() - 1; i >= 0; i--) {

			JoinExpression join = joins.get(i);
			Path<?> source = (Path) ((Operation<?>) join.getTarget()).getArg(0);
			Path<?> target = (Path) ((Operation<?>) join.getTarget()).getArg(1);
			Collection<Mono<Predicate>> extraFilters = predicates.get(target.getRoot());

			Mono<Predicate> filter = allOf(extraFilters).map(it -> ExpressionUtils.allOf(join.getCondition(), it))
					.switchIfEmpty(Mono.justOrEmpty(join.getCondition()));

			Mono<Predicate> predicate = getIds(target.getType(), filter) //
					.collectList() //
					.handle((it, sink) -> {

						if (it.isEmpty()) {
							sink.error(new NoMatchException(source));
							return;
						}

						Path<?> path = ExpressionUtils.path(String.class, source, "$id");
						sink.next(ExpressionUtils.in((Path<Object>) path, it));
					});

			predicates.add(source.getRoot(), predicate);
		}

		Path<?> source = (Path) ((Operation) joins.get(0).getTarget()).getArg(0);
		return allOf(predicates.get(source.getRoot())).onErrorResume(NoMatchException.class,
				e -> Mono.just(ExpressionUtils.predicate(MongodbOps.NO_MATCH, e.source)));
	}

	private Mono<Predicate> allOf(@Nullable Collection<Mono<Predicate>> predicates) {
		return predicates != null ? Flux.concat(predicates).collectList().map(ExpressionUtils::allOf) : Mono.empty();
	}

	/**
	 * Fetch the list of ids matching a given condition.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param condition must not be {@literal null}.
	 * @return empty {@link List} if none found.
	 */
	protected Flux<Object> getIds(Class<?> targetType, Mono<Predicate> condition) {

		return condition.flatMapMany(it -> getJoinIds(targetType, it))
				.switchIfEmpty(Flux.defer(() -> getJoinIds(targetType, (Predicate) null)));
	}

	/**
	 * Fetch the list of ids matching a given condition.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param condition must not be {@literal null}.
	 * @return empty {@link List} if none found.
	 */
	protected Flux<Object> getJoinIds(Class<?> targetType, @Nullable Predicate condition) {

		return createQuery(Mono.justOrEmpty(condition), null, QueryModifiers.EMPTY, Collections.emptyList())
				.flatMapMany(query -> mongoOperations.findDistinct(query, "_id", targetType, Object.class));
	}

	@Override
	protected List<Object> getIds(Class<?> aClass, Predicate predicate) {
		throw new UnsupportedOperationException(
				"Use create Flux<Object> getIds(Class<?> targetType, Mono<Predicate> condition)");
	}

	/**
	 * Marker exception to indicate no matches for a query using reference Id's.
	 */
	static class NoMatchException extends RuntimeException {

		final Path<?> source;

		NoMatchException(Path<?> source) {
			this.source = source;
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return null;
		}
	}
}
