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

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Delegate to assemble information about Vector Search queries necessary to run a MongoDB {@code $vectorSearch}.
 *
 * @author Mark Paluch
 */
class VectorSearchDelegate {

	private final VectorSearchQueryFactory queryFactory;
	private final VectorSearchOperation.SearchType searchType;
	private final @Nullable Integer numCandidates;
	private final @Nullable String numCandidatesExpression;
	private final Limit limit;
	private final @Nullable String limitExpression;
	private final MongoConverter converter;

	public VectorSearchDelegate(MongoQueryMethod method, MongoConverter converter, ValueExpressionDelegate delegate) {

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

		this.converter = converter;

		if (StringUtils.hasText(vectorSearch.filter())) {
			queryFactory = StringUtils.hasText(vectorSearch.path())
					? new AnnotatedQueryFactory(vectorSearch.filter(), vectorSearch.path())
					: new AnnotatedQueryFactory(vectorSearch.filter(), method.getEntityInformation().getCollectionEntity());
		} else {
			queryFactory = new PartTreeQueryFactory(
					new PartTree(method.getName(), method.getResultProcessor().getReturnedType().getDomainType()),
					converter.getMappingContext());
		}
	}

	/**
	 * Create Query Metadata for {@code $vectorSearch}.
	 */
	public QueryMetadata createQuery(ValueExpressionEvaluator evaluator, ResultProcessor processor,
			MongoParameterAccessor accessor, @Nullable Class<?> typeToRead, ParameterBindingDocumentCodec codec,
			ParameterBindingContext context) {

		Integer numCandidates = null;
		Limit limit;
		Class<?> outputType = typeToRead != null ? typeToRead : processor.getReturnedType().getReturnedType();
		VectorSearchInput query = queryFactory.createQuery(accessor, codec, context);

		if (this.limitExpression != null) {
			Object value = evaluator.evaluate(this.limitExpression);
			limit = value instanceof Limit l ? l : Limit.of(((Number) value).intValue());
		} else if (this.limit.isLimited()) {
			limit = this.limit;
		} else {
			limit = accessor.getLimit();
		}

		if (limit.isLimited()) {
			query.query().limit(limit);
		}

		if (this.numCandidatesExpression != null) {
			numCandidates = ((Number) evaluator.evaluate(this.numCandidatesExpression)).intValue();
		} else if (this.numCandidates != null) {
			numCandidates = this.numCandidates;
		} else if (query.query().isLimited() && searchType == VectorSearchOperation.SearchType.ANN) {

			/*
			MongoDB: We recommend that you specify a number at least 20 times higher than the number of documents to return (limit) to increase accuracy.
			 */
			numCandidates = query.query().getLimit() * 20;
		}

		return new QueryMetadata(query.path, "__score__", query.query, searchType, outputType, numCandidates,
				getSimilarityFunction(accessor));
	}

	public String getQueryString() {
		return queryFactory.getQueryString();
	}

	ScoringFunction getSimilarityFunction(MongoParameterAccessor accessor) {

		Score score = accessor.getScore();

		if (score != null) {
			return score.getFunction();
		}

		Range<Score> scoreRange = accessor.getScoreRange();

		if (scoreRange != null) {
			if (scoreRange.getUpperBound().isBounded()) {
				return scoreRange.getUpperBound().getValue().get().getFunction();
			}

			if (scoreRange.getLowerBound().isBounded()) {
				return scoreRange.getLowerBound().getValue().get().getFunction();
			}
		}

		return ScoringFunction.unspecified();
	}

	/**
	 * Metadata for a Vector Search Aggregation.
	 *
	 * @param path
	 * @param query
	 * @param searchType
	 * @param outputType
	 * @param numCandidates
	 * @param scoringFunction
	 */
	public record QueryMetadata(String path, String scoreField, Query query, VectorSearchOperation.SearchType searchType,
			Class<?> outputType, @org.jspecify.annotations.Nullable Integer numCandidates, ScoringFunction scoringFunction) {

		/**
		 * Create the Aggregation Pipeline.
		 *
		 * @param queryMethod
		 * @param accessor
		 * @return
		 */
		public List<AggregationOperation> getAggregationPipeline(MongoQueryMethod queryMethod,
				MongoParameterAccessor accessor) {

			Vector vector = accessor.getVector();
			Score score = accessor.getScore();
			Range<Score> distance = accessor.getScoreRange();
			int limit;

			if (query.isLimited()) {
				limit = query.getLimit();
			} else {
				limit = Math.max(1, numCandidates() != null ? numCandidates() / 20 : 1);
			}

			List<AggregationOperation> stages = new ArrayList<>();
			VectorSearchOperation $vectorSearch = Aggregation.vectorSearch(queryMethod.getAnnotatedHint()).path(path())
					.vector(vector).limit(limit);

			if (numCandidates() != null) {
				$vectorSearch = $vectorSearch.numCandidates(numCandidates());
			}

			$vectorSearch = $vectorSearch.filter(query.getQueryObject());
			$vectorSearch = $vectorSearch.searchType(searchType());
			$vectorSearch = $vectorSearch.withSearchScore(scoreField());

			if (score != null) {
				$vectorSearch = $vectorSearch.withFilterBySore(c -> {
					c.gt(score.getValue());
				});
			} else if (distance.getLowerBound().isBounded() || distance.getUpperBound().isBounded()) {
				$vectorSearch = $vectorSearch.withFilterBySore(c -> {
					Range.Bound<Score> lower = distance.getLowerBound();
					if (lower.isBounded()) {
						double value = lower.getValue().get().getValue();
						if (lower.isInclusive()) {
							c.gte(value);
						} else {
							c.gt(value);
						}
					}

					Range.Bound<Score> upper = distance.getUpperBound();
					if (upper.isBounded()) {

						double value = upper.getValue().get().getValue();
						if (upper.isInclusive()) {
							c.lte(value);
						} else {
							c.lt(value);
						}
					}
				});
			}

			stages.add($vectorSearch);

			if (query.isSorted()) {
				// TODO stages.add(Aggregation.sort(query.with()));
			} else {
				stages.add(Aggregation.sort(Sort.Direction.DESC, "__score__"));
			}

			return stages;
		}

	}

	/**
	 * Strategy interface to implement a query factory for the Vector Search pre-filter query.
	 */
	private interface VectorSearchQueryFactory {

		VectorSearchInput createQuery(MongoParameterAccessor parameterAccessor, ParameterBindingDocumentCodec codec,
				ParameterBindingContext context);

		/**
		 * @return the underlying query string to determine {@link ParameterBindingContext}.
		 */
		String getQueryString();
	}

	private static class AnnotatedQueryFactory implements VectorSearchQueryFactory {

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

		public VectorSearchInput createQuery(MongoParameterAccessor parameterAccessor, ParameterBindingDocumentCodec codec,
				ParameterBindingContext context) {

			Document queryObject = codec.decode(this.query, context);
			Query query = new BasicQuery(queryObject);

			Sort sort = parameterAccessor.getSort();
			if (sort.isSorted()) {
				query = query.with(sort);
			}

			return new VectorSearchInput(path, query);
		}

		@Override
		public String getQueryString() {
			return this.query;
		}
	}

	private class PartTreeQueryFactory implements VectorSearchQueryFactory {

		private final String path;
		private final PartTree tree;

		@SuppressWarnings("NullableProblems")
		PartTreeQueryFactory(PartTree tree, MappingContext<?, MongoPersistentProperty> context) {

			String path = null;
			for (PartTree.OrPart part : tree) {
				for (Part p : part) {
					if (p.getType() == Part.Type.SIMPLE_PROPERTY || p.getType() == Part.Type.NEAR
							|| p.getType() == Part.Type.WITHIN || p.getType() == Part.Type.BETWEEN) {
						PersistentPropertyPath<MongoPersistentProperty> ppp = context.getPersistentPropertyPath(p.getProperty());
						MongoPersistentProperty property = ppp.getLeafProperty();

						if (Vector.class.isAssignableFrom(property.getType())) {
							path = p.getProperty().toDotPath();
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
			this.tree = tree;
		}

		public VectorSearchInput createQuery(MongoParameterAccessor parameterAccessor, ParameterBindingDocumentCodec codec,
				ParameterBindingContext context) {

			MongoQueryCreator creator = new MongoQueryCreator(tree, parameterAccessor, converter.getMappingContext(),
					false, true);

			Query query = creator.createQuery(parameterAccessor.getSort());

			if (tree.isLimiting()) {
				query.limit(tree.getMaxResults());
			}

			return new VectorSearchInput(path, query);
		}

		@Override
		public String getQueryString() {
			return "";
		}
	}

	private record VectorSearchInput(String path, Query query) {

	}

}
