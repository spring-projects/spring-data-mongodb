/*
 * Copyright 2024-present the original author or authors.
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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bson.BinaryVector;
import org.bson.Document;

import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.mapping.MongoVector;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Performs a semantic search on data in your Atlas cluster. This stage is only available for Atlas Vector Search.
 * Vector data must be less than or equal to 4096 dimensions in width.
 * <h3>Limitations</h3> You cannot use this stage together with:
 * <ul>
 * <li>{@link org.springframework.data.mongodb.core.aggregation.LookupOperation Lookup} stages</li>
 * <li>{@link org.springframework.data.mongodb.core.aggregation.FacetOperation Facet} stage</li>
 * </ul>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.5
 */
public class VectorSearchOperation implements AggregationOperation {

	private final SearchType searchType;
	private final @Nullable CriteriaDefinition filter;
	private final String indexName;
	private final Limit limit;
	private final @Nullable Integer numCandidates;
	private final QueryPaths path;
	private final Vector vector;
	private final @Nullable String score;
	private final @Nullable Consumer<Criteria> scoreCriteria;

	private VectorSearchOperation(SearchType searchType, @Nullable CriteriaDefinition filter, String indexName,
			Limit limit, @Nullable Integer numCandidates, QueryPaths path, Vector vector, @Nullable String searchScore,
			@Nullable Consumer<Criteria> scoreCriteria) {

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

	VectorSearchOperation(String indexName, QueryPaths path, Limit limit, Vector vector) {
		this(SearchType.DEFAULT, null, indexName, limit, null, path, vector, null, null);
	}

	/**
	 * Entrypoint to build a {@link VectorSearchOperation} starting from the {@code index} name to search. Atlas Vector
	 * Search doesn't return results if you misspell the index name or if the specified index doesn't already exist on the
	 * cluster.
	 *
	 * @param index must not be {@literal null} or empty.
	 * @return new instance of {@link VectorSearchOperation.PathContributor}.
	 */
	public static PathContributor search(String index) {
		return new VectorSearchBuilder().index(index);
	}

	/**
	 * Configure the search type to use. {@link SearchType#ENN} leads to an exact search while {@link SearchType#ANN} uses
	 * {@code exact=false}.
	 *
	 * @param searchType must not be null.
	 * @return a new {@link VectorSearchOperation} with {@link SearchType} applied.
	 */
	@Contract("_ -> new")
	public VectorSearchOperation searchType(SearchType searchType) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, score,
				scoreCriteria);
	}

	/**
	 * Criteria expression that compares an indexed field with a boolean, date, objectId, number (not decimals), string,
	 * or UUID to use as a pre-filter.
	 * <p>
	 * Atlas Vector Search supports only the filters for the following MQL match expressions:
	 * <ul>
	 * <li>$gt</li>
	 * <li>$lt</li>
	 * <li>$gte</li>
	 * <li>$lte</li>
	 * <li>$eq</li>
	 * <li>$ne</li>
	 * <li>$in</li>
	 * <li>$nin</li>
	 * <li>$nor</li>
	 * <li>$not</li>
	 * <li>$and</li>
	 * <li>$or</li>
	 * </ul>
	 *
	 * @param filter must not be null.
	 * @return a new {@link VectorSearchOperation} with {@link CriteriaDefinition} applied.
	 */
	@Contract("_ -> new")
	public VectorSearchOperation filter(CriteriaDefinition filter) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, score,
				scoreCriteria);
	}

	/**
	 * Criteria expression that compares an indexed field with a boolean, date, objectId, number (not decimals), string,
	 * or UUID to use as a pre-filter.
	 * <p>
	 * Atlas Vector Search supports only the filters for the following MQL match expressions:
	 * <ul>
	 * <li>$gt</li>
	 * <li>$lt</li>
	 * <li>$gte</li>
	 * <li>$lte</li>
	 * <li>$eq</li>
	 * <li>$ne</li>
	 * <li>$in</li>
	 * <li>$nin</li>
	 * <li>$nor</li>
	 * <li>$not</li>
	 * <li>$and</li>
	 * <li>$or</li>
	 * </ul>
	 *
	 * @param filter must not be null.
	 * @return a new {@link VectorSearchOperation} with {@link CriteriaDefinition} applied.
	 */
	@Contract("_ -> new")
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

	/**
	 * Number of nearest neighbors to use during the search. Value must be less than or equal to ({@code <=}) {@code 10000}. You
	 * can't specify a number less than the number of documents to return (limit). This field is required if
	 * {@link #searchType(SearchType)} is {@link SearchType#ANN} or {@link SearchType#DEFAULT}.
	 *
	 * @param numCandidates number of nearest neighbors to use during the search
	 * @return a new {@link VectorSearchOperation} with {@code numCandidates} applied.
	 */
	@Contract("_ -> new")
	public VectorSearchOperation numCandidates(int numCandidates) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, score,
				scoreCriteria);
	}

	/**
	 * Add a {@link AddFieldsOperation} stage including the search score using {@code score} as field name.
	 *
	 * @return a new {@link VectorSearchOperation} with search score applied.
	 * @see #withSearchScore(String)
	 */
	@Contract("-> new")
	public VectorSearchOperation withSearchScore() {
		return withSearchScore("score");
	}

	/**
	 * Add a {@link AddFieldsOperation} stage including the search score using {@code scoreFieldName} as field name.
	 *
	 * @param scoreFieldName name of the score field.
	 * @return a new {@link VectorSearchOperation} with {@code scoreFieldName} applied.
	 * @see #withSearchScore()
	 */
	@Contract("_ -> new")
	public VectorSearchOperation withSearchScore(String scoreFieldName) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector, scoreFieldName,
				scoreCriteria);
	}

	/**
	 * Add a {@link MatchOperation} stage targeting the score field name. Implies that the score field is present by
	 * either reusing a previous {@link AddFieldsOperation} from {@link #withSearchScore()} or
	 * {@link #withSearchScore(String)} or by adding a new {@link AddFieldsOperation} stage.
	 *
	 * @return a new {@link VectorSearchOperation} with search score filter applied.
	 */
	@Contract("_ -> new")
	public VectorSearchOperation withFilterBySore(Consumer<Criteria> score) {
		return new VectorSearchOperation(searchType, filter, indexName, limit, numCandidates, path, vector,
				StringUtils.hasText(this.score) ? this.score : "score", score);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document $vectorSearch = new Document();

		if (searchType != null && !searchType.equals(SearchType.DEFAULT)) {
			$vectorSearch.append("exact", searchType.equals(SearchType.ENN));
		}

		if (filter != null) {
			$vectorSearch.append("filter", context.getMappedObject(filter.getCriteriaObject()));
		}

		$vectorSearch.append("index", indexName);

		if(limit.isLimited()) {
			$vectorSearch.append("limit", limit.max());
		}

		if (numCandidates != null) {
			$vectorSearch.append("numCandidates", numCandidates);
		}

		Object path = this.path.getPathObject();

		if (path instanceof String pathFieldName) {
			Document mappedObject = context.getMappedObject(new Document(pathFieldName, 1));
			path = mappedObject.keySet().iterator().next();
		}

		Object source = vector.getSource();

		if (source instanceof float[]) {
			source = vector.toDoubleArray();
		}

		if (source instanceof double[] ds) {
			source = Arrays.stream(ds).boxed().collect(Collectors.toList());
		}

		$vectorSearch.append("path", path);
		$vectorSearch.append("queryVector", source);

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

	/**
	 * Builder helper to create a {@link VectorSearchOperation}.
	 */
	private static class VectorSearchBuilder implements PathContributor, VectorContributor, LimitContributor {

		@Nullable String index;
		@Nullable QueryPath<String> paths;
		@Nullable Vector vector;

		PathContributor index(String index) {
			this.index = index;
			return this;
		}

		@Override
		public VectorContributor path(String path) {

			this.paths = QueryPath.path(path);
			return this;
		}

		@Override
		public VectorSearchOperation limit(Limit limit) {

			Assert.notNull(index, "Index must be set first");
			Assert.notNull(paths, "Path must be set first");
			Assert.notNull(vector, "Vector must be set first");
			
			return new VectorSearchOperation(index, QueryPaths.of(paths), limit, vector);
		}

		@Override
		public LimitContributor vector(Vector vector) {
			this.vector = vector;
			return this;
		}
	}

	/**
	 * Search type, ANN as approximation or ENN for exact search.
	 */
	public enum SearchType {

		/** MongoDB Server default (value will be omitted) */
		DEFAULT,
		/** Approximate Nearest Neighbour */
		ANN,
		/** Exact Nearest Neighbour */
		ENN
	}

	/**
	 * Value object capturing query paths.
	 */
	public static class QueryPaths {

		private final Set<QueryPath<?>> paths;

		private QueryPaths(Set<QueryPath<?>> paths) {
			this.paths = paths;
		}

		/**
		 * Factory method to create {@link QueryPaths} from a single {@link QueryPath}.
		 *
		 * @param path
		 * @return a new {@link QueryPaths} instance.
		 */
		public static QueryPaths of(QueryPath<String> path) {
			return new QueryPaths(Set.of(path));
		}

		Object getPathObject() {

			if (paths.size() == 1) {
				return paths.iterator().next().value();
			}
			return paths.stream().map(QueryPath::value).collect(Collectors.toList());
		}
	}

	/**
	 * Interface describing a query path contract. Query paths might be simple field names, wildcard paths, or
	 * multi-paths. paths.
	 *
	 * @param <T>
	 */
	public interface QueryPath<T> {

		T value();

		static QueryPath<String> path(String field) {
			return new SimplePath(field);
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

	/**
	 * Fluent API to configure a path on the VectorSearchOperation builder.
	 */
	public interface PathContributor {

		/**
		 * Indexed vector type field to search.
		 *
		 * @param path name of the search path.
		 * @return
		 */
		@Contract("_ -> this")
		VectorContributor path(String path);
	}

	/**
	 * Fluent API to configure a vector on the VectorSearchOperation builder.
	 */
	public interface VectorContributor {

		/**
		 * Array of float numbers that represent the query vector. The number type must match the indexed field value type.
		 * Otherwise, Atlas Vector Search doesn't return any results or errors.
		 *
		 * @param vector the query vector.
		 * @return
		 */
		@Contract("_ -> this")
		default LimitContributor vector(float... vector) {
			return vector(Vector.of(vector));
		}

		/**
		 * Array of byte numbers that represent the query vector. The number type must match the indexed field value type.
		 * Otherwise, Atlas Vector Search doesn't return any results or errors.
		 *
		 * @param vector the query vector.
		 * @return
		 */
		@Contract("_ -> this")
		default LimitContributor vector(byte[] vector) {
			return vector(BinaryVector.int8Vector(vector));
		}

		/**
		 * Array of double numbers that represent the query vector. The number type must match the indexed field value type.
		 * Otherwise, Atlas Vector Search doesn't return any results or errors.
		 *
		 * @param vector the query vector.
		 * @return
		 */
		@Contract("_ -> this")
		default LimitContributor vector(double... vector) {
			return vector(Vector.of(vector));
		}

		/**
		 * Array of numbers that represent the query vector. The number type must match the indexed field value type.
		 * Otherwise, Atlas Vector Search doesn't return any results or errors.
		 *
		 * @param vector the query vector.
		 * @return
		 */
		@Contract("_ -> this")
		default LimitContributor vector(List<? extends Number> vector) {
			return vector(Vector.of(vector));
		}

		/**
		 * Binary vector (BSON BinData vector subtype float32, or BSON BinData vector subtype int1 or int8 type) that
		 * represent the query vector. The number type must match the indexed field value type. Otherwise, Atlas Vector
		 * Search doesn't return any results or errors.
		 *
		 * @param vector the query vector.
		 * @return
		 */
		@Contract("_ -> this")
		default LimitContributor vector(BinaryVector vector) {
			return vector(MongoVector.of(vector));
		}

		/**
		 * The query vector. The number type must match the indexed field value type. Otherwise, Atlas Vector Search doesn't
		 * return any results or errors.
		 *
		 * @param vector the query vector.
		 * @return
		 */
		@Contract("_ -> this")
		LimitContributor vector(Vector vector);
	}

	/**
	 * Fluent API to configure a limit on the VectorSearchOperation builder.
	 */
	public interface LimitContributor {

		/**
		 * Number (of type int only) of documents to return in the results. This value can't exceed the value of
		 * numCandidates if you specify numCandidates.
		 *
		 * @param limit
		 * @return
		 */
		@Contract("_ -> this")
		default VectorSearchOperation limit(int limit) {
			return limit(Limit.of(limit));
		}

		/**
		 * Number (of type int only) of documents to return in the results. This value can't exceed the value of
		 * numCandidates if you specify numCandidates.
		 *
		 * @param limit
		 * @return
		 */
		@Contract("_ -> this")
		VectorSearchOperation limit(Limit limit);
	}

}
