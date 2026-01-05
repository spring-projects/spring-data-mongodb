/*
 * Copyright 2025-present the original author or authors.
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
import org.jspecify.annotations.Nullable;
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
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
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
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Delegate to assemble information about Vector Search queries necessary to run a MongoDB {@code $vectorSearch}.
 *
 * @author Mark Paluch
 */
class VectorSearchDelegate {

	private final VectorSearchQueryFactory queryFactory;
	private final VectorSearchOperation.SearchType searchType;
	private final String indexName;
	private final @Nullable Integer numCandidates;
	private final @Nullable String numCandidatesExpression;
	private final Limit limit;
	private final @Nullable String limitExpression;
	private final MongoConverter converter;

	VectorSearchDelegate(MongoQueryMethod method, MongoConverter converter, ValueExpressionDelegate delegate) {

		VectorSearch vectorSearch = method.findAnnotatedVectorSearch().orElseThrow();

		this.searchType = vectorSearch.searchType();
		this.indexName = method.getAnnotatedHint();

		if (StringUtils.hasText(vectorSearch.numCandidates())) {

			ValueExpression expression = delegate.getValueExpressionParser().parse(vectorSearch.numCandidates());

			if (expression.isLiteral()) {
				this.numCandidates = Integer.parseInt(vectorSearch.numCandidates());
				this.numCandidatesExpression = null;
			} else {
				this.numCandidates = null;
				this.numCandidatesExpression = vectorSearch.numCandidates();
			}

		} else {
			this.numCandidates = null;
			this.numCandidatesExpression = null;
		}

		if (StringUtils.hasText(vectorSearch.limit())) {

			ValueExpression expression = delegate.getValueExpressionParser().parse(vectorSearch.limit());

			if (expression.isLiteral()) {
				this.limit = Limit.of(Integer.parseInt(vectorSearch.limit()));
				this.limitExpression = null;
			} else {
				this.limit = Limit.unlimited();
				this.limitExpression = vectorSearch.limit();
			}

		} else {
			this.limit = Limit.unlimited();
			this.limitExpression = null;
		}

		this.converter = converter;

		if (StringUtils.hasText(vectorSearch.filter())) {
			this.queryFactory = StringUtils.hasText(vectorSearch.path())
					? new AnnotatedQueryFactory(vectorSearch.filter(), vectorSearch.path())
					: new AnnotatedQueryFactory(vectorSearch.filter(), method.getEntityInformation().getCollectionEntity());
		} else {
			this.queryFactory = new PartTreeQueryFactory(
					new PartTree(method.getName(), method.getResultProcessor().getReturnedType().getDomainType()),
					converter.getMappingContext());
		}
	}

	/**
	 * Create Query Metadata for {@code $vectorSearch}.
	 */
	QueryContainer createQuery(ValueExpressionEvaluator evaluator, ResultProcessor processor,
			MongoParameterAccessor accessor, @Nullable Class<?> typeToRead, ParameterBindingDocumentCodec codec,
			ParameterBindingContext context) {

		String scoreField = "__score__";
		Class<?> outputType = typeToRead != null ? typeToRead : processor.getReturnedType().getReturnedType();
		VectorSearchInput vectorSearchInput = createSearchInput(evaluator, accessor, codec, context);
		AggregationPipeline pipeline = createVectorSearchPipeline(vectorSearchInput, scoreField, outputType, accessor,
				evaluator);

		return new QueryContainer(vectorSearchInput.path, scoreField, vectorSearchInput.query, pipeline, searchType,
				outputType, getSimilarityFunction(accessor), indexName);
	}

	@SuppressWarnings("NullAway")
	AggregationPipeline createVectorSearchPipeline(VectorSearchInput input, String scoreField, Class<?> outputType,
			MongoParameterAccessor accessor, ValueExpressionEvaluator evaluator) {

		Vector vector = accessor.getVector();
		Score score = accessor.getScore();
		Range<Score> distance = accessor.getScoreRange();
		Limit limit = Limit.of(input.query().getLimit());

		List<AggregationOperation> stages = new ArrayList<>();
		VectorSearchOperation $vectorSearch = Aggregation.vectorSearch(indexName).path(input.path()).vector(vector)
				.limit(limit);

		Integer candidates = null;
		if (this.numCandidatesExpression != null) {
			candidates = ((Number) evaluator.evaluate(this.numCandidatesExpression)).intValue();
		} else if (this.numCandidates != null) {
			candidates = this.numCandidates;
		} else if (input.query().isLimited() && (searchType == VectorSearchOperation.SearchType.ANN
				|| searchType == VectorSearchOperation.SearchType.DEFAULT)) {

			/*
			MongoDB: We recommend that you specify a number at least 20 times higher than the number of documents to return (limit) to increase accuracy.
			 */
			candidates = input.query().getLimit() * 20;
		}

		if (candidates != null) {
			$vectorSearch = $vectorSearch.numCandidates(candidates);
		}
		//
		$vectorSearch = $vectorSearch.filter(input.query.getQueryObject());
		$vectorSearch = $vectorSearch.searchType(this.searchType);
		$vectorSearch = $vectorSearch.withSearchScore(scoreField);

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

		if (input.query().isSorted()) {

			stages.add(ctx -> {

				Document mappedSort = ctx.getMappedObject(input.query().getSortObject(), outputType);
				mappedSort.append(scoreField, -1);
				return ctx.getMappedObject(new Document("$sort", mappedSort));
			});
		} else {
			stages.add(Aggregation.sort(Sort.Direction.DESC, scoreField));
		}

		return new AggregationPipeline(stages);
	}

	private VectorSearchInput createSearchInput(ValueExpressionEvaluator evaluator, MongoParameterAccessor accessor,
			ParameterBindingDocumentCodec codec, ParameterBindingContext context) {

		VectorSearchInput input = queryFactory.createQuery(accessor, codec, context);
		Limit limit = getLimit(evaluator, accessor);
		if(!input.query.isLimited() || (input.query.isLimited() && !limit.isUnlimited())) {
			input.query().limit(limit);
		}
		return input;
	}

	private Limit getLimit(ValueExpressionEvaluator evaluator, MongoParameterAccessor accessor) {

		if (this.limitExpression != null) {

			Object value = evaluator.evaluate(this.limitExpression);
			if (value != null) {
				if (value instanceof Limit l) {
					return l;
				}
				if (value instanceof Number n) {
					return Limit.of(n.intValue());
				}
				if (value instanceof String s) {
					return Limit.of(NumberUtils.parseNumber(s, Integer.class));
				}
				throw new IllegalArgumentException("Invalid type for Limit. Found [%s], expected Limit or Number");
			}
		}

		if (this.limit.isLimited()) {
			return this.limit;
		}

		return accessor.getLimit();
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
	 * @param scoringFunction
	 */
	record QueryContainer(String path, String scoreField, Query query, AggregationPipeline pipeline,
			VectorSearchOperation.SearchType searchType, Class<?> outputType, ScoringFunction scoringFunction, String index) {

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

		@SuppressWarnings("NullAway")
		public VectorSearchInput createQuery(MongoParameterAccessor parameterAccessor, ParameterBindingDocumentCodec codec,
				ParameterBindingContext context) {

			MongoQueryCreator creator = new MongoQueryCreator(tree, parameterAccessor, converter.getMappingContext(), false,
					true);

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
