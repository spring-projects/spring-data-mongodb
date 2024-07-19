/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.domain.Limit;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class VectorSearchOperation implements AggregationOperation {

	public enum SearchType {

		/** MongoDB Server default (value will be omitted) */
		DEFAULT,
		/** Approximate Nearest Neighbour */
		ANN,
		/** Exact Nearest Neighbour */
		ENN
	}

	// A query path cannot only contain the name of the filed but may also hold additional information about the
	// analyzer to use;
	// "path": [ "names", "notes", { "value": "comments", "multi": "mySecondaryAnalyzer" } ]
	// see: https://www.mongodb.com/docs/atlas/atlas-search/path-construction/#std-label-ref-path
	public static class QueryPaths {

		Set<QueryPath<?>> paths;

		public static QueryPaths of(QueryPath<String> path) {

			QueryPaths queryPaths = new QueryPaths();
			queryPaths.paths = new LinkedHashSet<>(2);
			queryPaths.paths.add(path);
			return queryPaths;
		}

		Object getPathObject() {

			if (paths.size() == 1) {
				return paths.iterator().next().value();
			}
			return paths.stream().map(QueryPath::value).collect(Collectors.toList());
		}
	}

	public interface QueryPath<T> {

		T value();

		static QueryPath<String> path(String field) {
			return new SimplePath(field);
		}

		static QueryPath<Map<String, Object>> wildcard(String field) {
			return new WildcardPath(field);
		}

		static QueryPath<Map<String, Object>> multi(String field, String analyzer) {
			return new MultiPath(field, analyzer);
		}
	}

	public static class SimplePath implements QueryPath<String> {

		String name;

		public SimplePath(String name) {
			this.name = name;
		}

		@Override
		public String value() {
			return name;
		}
	}

	public static class WildcardPath implements QueryPath<Map<String, Object>> {

		String name;

		public WildcardPath(String name) {
			this.name = name;
		}

		@Override
		public Map<String, Object> value() {
			return Map.of("wildcard", name);
		}
	}

	public static class MultiPath implements QueryPath<Map<String, Object>> {

		String field;
		String analyzer;

		public MultiPath(String field, String analyzer) {
			this.field = field;
			this.analyzer = analyzer;
		}

		@Override
		public Map<String, Object> value() {
			return Map.of("value", field, "multi", analyzer);
		}
	}

	private SearchType searchType;
	private CriteriaDefinition filter;
	private String indexName;
	private Limit limit;
	private Integer numCandidates;
	private QueryPaths path;
	private List<Double> vector;

	private String score;
	private Consumer<Criteria> scoreCriteria;

	private VectorSearchOperation(SearchType searchType, CriteriaDefinition filter, String indexName, Limit limit,
			Integer numCandidates, QueryPaths path, List<Double> vector, String searchScore,
			Consumer<Criteria> scoreCriteria) {

		this.searchType = searchType;
		this.filter = filter;
		this.indexName = indexName;
		this.limit = limit;
		this.numCandidates = numCandidates;
		this.path = path;
		this.vector = vector;
		this.score = searchScore;
		this.scoreCriteria = scoreCriteria;
	}

	public VectorSearchOperation(String indexName, QueryPaths path, Limit limit, List<Double> vector) {
		this(SearchType.DEFAULT, null, indexName, limit, null, path, vector, null, null);
	}

	static PathContributor search(String index) {
		return new VectorSearchBuilder().index(index);
	}

	public VectorSearchOperation(String indexName, String path, Limit limit, List<Double> vector) {
		this(indexName, QueryPaths.of(QueryPath.path(path)), limit, vector);
	}

	public VectorSearchOperation searchType(SearchType searchType) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, score,
				scoreCriteria);
	}

	public VectorSearchOperation filter(Document filter) {

		return filter(new CriteriaDefinition() {
			@Override
			public Document getCriteriaObject() {
				return filter;
			}

			@Nullable
			@Override
			public String getKey() {
				return null;
			}
		});
	}

	public VectorSearchOperation filter(CriteriaDefinition filter) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, score,
				scoreCriteria);
	}

	public VectorSearchOperation numCandidates(int numCandidates) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, score,
				scoreCriteria);
	}

	public VectorSearchOperation searchScore() {
		return searchScore("score");
	}

	public VectorSearchOperation searchScore(String scoreFieldName) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, scoreFieldName,
				scoreCriteria);
	}

	public VectorSearchOperation filterBySore(Consumer<Criteria> score) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector,
				StringUtils.hasText(this.score) ? this.score : "score", score);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document $vectorSearch = new Document();

		$vectorSearch.append("index", indexName);
		$vectorSearch.append("path", path.getPathObject());
		$vectorSearch.append("queryVector", vector);
		$vectorSearch.append("limit", limit.max());

		if (searchType != null && !searchType.equals(SearchType.DEFAULT)) {
			$vectorSearch.append("exact", searchType.equals(SearchType.ENN));
		}

		if (filter != null) {
			$vectorSearch.append("filter", context.getMappedObject(filter.getCriteriaObject()));
		}

		if (numCandidates != null) {
			$vectorSearch.append("numCandidates", numCandidates);
		}

		return new Document(getOperator(), $vectorSearch);
	}

	@Override
	public List<Document> toPipelineStages(AggregationOperationContext context) {

		if (!StringUtils.hasText(score)) {
			return List.of(toDocument(context));
		}

		AddFieldsOperation $vectorSearchScore = Aggregation.addFields().addField(score)
				.withValueOfExpression("{\"$meta\":\"vectorSearchScore\"}").build();

		if (scoreCriteria == null) {
			return List.of(toDocument(context), $vectorSearchScore.toDocument(context));
		}

		Criteria criteria = Criteria.where(score);
		scoreCriteria.accept(criteria);
		MatchOperation $filterByScore = Aggregation.match(criteria);

		return List.of(toDocument(context), $vectorSearchScore.toDocument(context), $filterByScore.toDocument(context));
	}

	@Override
	public String getOperator() {
		return "$vectorSearch";
	}

	public static class VectorSearchBuilder implements PathContributor, VectorContributor, LimitContributor {

		String index;
		QueryPaths paths;
		private List<Double> vector;

		PathContributor index(String index) {
			this.index = index;
			return this;
		}

		@Override
		public VectorContributor path(QueryPaths paths) {
			this.paths = paths;
			return this;
		}

		@Override
		public VectorSearchOperation limit(Limit limit) {
			return new VectorSearchOperation(index, paths, limit, vector);
		}

		@Override
		public LimitContributor vectors(List<Double> vectors) {
			this.vector = vectors;
			return this;
		}
	}

	public interface PathContributor {
		default VectorContributor path(String path) {
			return path(QueryPaths.of(QueryPath.path(path)));
		}

		VectorContributor path(QueryPaths paths);
	}

	public interface VectorContributor {
		default LimitContributor vectors(Double... vectors) {
			return vectors(Arrays.asList(vectors));
		}

		LimitContributor vectors(List<Double> vectors);
	}

	public interface LimitContributor {
		default VectorSearchOperation limit(int limit) {
			return limit(Limit.of(limit));
		}

		VectorSearchOperation limit(Limit limit);
	}

}
