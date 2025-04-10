/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.bson.Document;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * {@link AbstractMongoQuery} implementation to run a {@link VectorSearchAggregation}. The pre-filter is either derived
 * from the method name or provided through {@link VectorSearch#filter()}.
 *
 * @author Mark Paluch
 * @since 5.0
 */
public class VectorSearchAggregation extends AbstractMongoQuery {

	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final MongoPersistentEntity<?> collectionEntity;
	private final VectorSearchQueryFactory queryFactory;
	private final VectorSearchOperation.SearchType searchType;
	private final @Nullable Integer numCandidates;
	private final @Nullable String numCandidatesExpression;

	private final Limit limit;
	private final @Nullable String limitExpression;

	/**
	 * Creates a new {@link VectorSearchAggregation} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 */
	public VectorSearchAggregation(MongoQueryMethod method, MongoOperations mongoOperations,
			ValueExpressionDelegate delegate) {

		super(method, mongoOperations, delegate);

		if (!method.isSearchQuery() && !method.isCollectionQuery()) {
			throw new InvalidMongoDbApiUsageException(String.format(
					"Repository Vector Search method '%s' must return either return SearchResults<T> or List<T> but was %s",
					method.getName(), method.getReturnType().getType().getSimpleName()));
		}

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.collectionEntity = method.getEntityInformation().getCollectionEntity();

		VectorSearch vectorSearch = method.findAnnotatedVectorSearch().orElseThrow();

		this.searchType = vectorSearch.searchType();

		if (StringUtils.hasText(vectorSearch.numCandidates())) {

			ValueExpression expression = delegate.getValueExpressionParser().parse(vectorSearch.numCandidates());

			if (expression.isLiteral()) {
				numCandidates = Integer.parseInt(vectorSearch.numCandidates());
				numCandidatesExpression = null;
			} else {
				numCandidates = null;
				numCandidatesExpression = vectorSearch.numCandidates();
			}

		} else {
			numCandidates = null;
			numCandidatesExpression = null;
		}

		if (StringUtils.hasText(vectorSearch.limit())) {

			ValueExpression expression = delegate.getValueExpressionParser().parse(vectorSearch.limit());

			if (expression.isLiteral()) {
				limit = Limit.of(Integer.parseInt(vectorSearch.limit()));
				limitExpression = null;
			} else {
				limit = Limit.unlimited();
				limitExpression = vectorSearch.limit();
			}

		} else {
			limit = Limit.unlimited();
			limitExpression = null;
		}

		if (StringUtils.hasText(vectorSearch.filter())) {
			queryFactory = StringUtils.hasText(vectorSearch.path())
					? new AnnotatedQueryFactory(vectorSearch.filter(), vectorSearch.path())
					: new AnnotatedQueryFactory(vectorSearch.filter(), collectionEntity);
		} else {
			queryFactory = new PartTreeQueryFactory(
					new PartTree(method.getName(), method.getResultProcessor().getReturnedType().getDomainType()),
					mongoConverter.getMappingContext());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object doExecute(MongoQueryMethod method, ResultProcessor processor, ConvertingParameterAccessor accessor,
			@Nullable Class<?> typeToRead) {

		ValueExpressionEvaluator evaluator = getExpressionEvaluatorFor(accessor);
		Integer numCandidates = null;
		Limit limit;
		Class<?> outputType = typeToRead != null ? typeToRead : processor.getReturnedType().getReturnedType();

		if (this.numCandidatesExpression != null) {
			numCandidates = ((Number) evaluator.evaluate(this.numCandidatesExpression)).intValue();
		} else if (this.numCandidates != null) {
			numCandidates = this.numCandidates;
		}

		if (this.limitExpression != null) {

			Object value = evaluator.evaluate(this.limitExpression);
			limit = value instanceof Limit l ? l : Limit.of(((Number) value).intValue());
		} else if (this.limit.isLimited()) {
			limit = this.limit;
		} else {
			limit = accessor.getLimit();
		}

		VectorSearchQuery query = createVectorSearchQuery(accessor);

		if (limit.isLimited()) {
			query.query().limit(limit);
		}

		MongoQueryExecution.VectorSearchExecution execution = new MongoQueryExecution.VectorSearchExecution(mongoOperations,
				method, collectionEntity.getCollection(), query.path(), numCandidates, searchType, accessor,
				(Class<Object>) outputType);

		return execution.execute(query.query());
	}

	VectorSearchQuery createVectorSearchQuery(MongoParameterAccessor accessor) {
		return queryFactory.createQuery(accessor);
	}

	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected boolean isCountQuery() {
		return false;
	}

	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	@Override
	protected boolean isDeleteQuery() {
		return false;
	}

	@Override
	protected boolean isLimiting() {
		return false;
	}

	interface VectorSearchQueryFactory {

		VectorSearchQuery createQuery(MongoParameterAccessor parameterAccessor);
	}

	class AnnotatedQueryFactory implements VectorSearchQueryFactory {

		private final String query;
		private final String path;

		AnnotatedQueryFactory(String query, String path) {

			this.query = query;
			this.path = path;
		}

		AnnotatedQueryFactory(String query, MongoPersistentEntity<?> entity) {

			this.query = query;
			String path = null;
			for (MongoPersistentProperty property : entity) {
				if (Vector.class.isAssignableFrom(property.getType())) {
					path = property.getFieldName();
					break;
				}
			}

			if (path == null) {
				throw new InvalidMongoDbApiUsageException(
						"Cannot find Vector Search property in entity [%s]".formatted(entity.getName()));
			}

			this.path = path;
		}

		public VectorSearchQuery createQuery(MongoParameterAccessor parameterAccessor) {

			Document queryObject = decode(this.query, prepareBindingContext(this.query, parameterAccessor));
			Query query = new BasicQuery(queryObject);

			Sort sort = parameterAccessor.getSort();
			if (sort.isSorted()) {
				query = query.with(sort);
			}

			return new VectorSearchQuery(path, query);
		}

	}

	class PartTreeQueryFactory implements VectorSearchQueryFactory {

		private final String path;
		private final Part.Type type;
		private final MappingContext<?, MongoPersistentProperty> context;
		private final PartTree partTree;

		@SuppressWarnings("NullableProblems")
		PartTreeQueryFactory(PartTree partTree, MappingContext<?, MongoPersistentProperty> context) {

			String path = null;
			Part.Type type = null;
			for (PartTree.OrPart part : partTree) {
				for (Part p : part) {
					if (p.getType() == Part.Type.SIMPLE_PROPERTY || p.getType() == Part.Type.NEAR
							|| p.getType() == Part.Type.WITHIN || p.getType() == Part.Type.BETWEEN) {
						PersistentPropertyPath<MongoPersistentProperty> ppp = context.getPersistentPropertyPath(p.getProperty());
						MongoPersistentProperty property = ppp.getLeafProperty();

						if (Vector.class.isAssignableFrom(property.getType())) {
							path = p.getProperty().toDotPath();
							type = p.getType();
							break;
						}
					}
				}
			}

			if (path == null) {
				throw new InvalidMongoDbApiUsageException(
						"No Simple Property/Near/Within/Between part found for a Vector property");
			}

			this.path = path;
			this.type = type;

			this.partTree = partTree;
			this.context = context;
		}

		public VectorSearchQuery createQuery(MongoParameterAccessor parameterAccessor) {

			MongoQueryCreator creator = new MongoQueryCreator(partTree, parameterAccessor, mongoConverter.getMappingContext(),
					false, true);

			Query query = creator.createQuery(parameterAccessor.getSort());

			return new VectorSearchQuery(path, query);
		}

	}

	record VectorSearchQuery(String path, Query query) {

	}

}
