/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;

/**
 * Annotation to declare Vector Search queries directly on repository methods. Vector Search queries are used to search
 * for similar documents based on vector embeddings typically returning
 * {@link org.springframework.data.domain.SearchResults} and limited by either a
 * {@link org.springframework.data.domain.Score} (within) or a {@link org.springframework.data.domain.Range} of scores
 * (between).
 * <p>
 * Vector search must define an index name using the {@link #indexName()} attribute. The index must be created in the
 * MongoDB Atlas cluster before executing the query. Any misspelling of the index name will result in returning no
 * results.
 * <p>
 * When using pre-filters, you can either define {@link #filter()} or use query derivation to define the pre-filter.
 * {@link org.springframework.data.domain.Vector} and distance parameters are considered once these are present. Vector
 * search supports sorting and will consider {@link org.springframework.data.domain.Sort} parameters.
 *
 * @author Mark Paluch
 * @since 5.0
 * @see org.springframework.data.geo.Distance
 * @see org.springframework.data.domain.Vector
 * @see org.springframework.data.domain.SearchResults
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
@Query
@Hint
public @interface VectorSearch {

	/**
	 * Configuration whether to use ANN or ENN for the search. ANN is the default.
	 *
	 * @return the search type to use.
	 */
	VectorSearchOperation.SearchType searchType() default VectorSearchOperation.SearchType.ENN;

	/**
	 * Name of the Atlas Vector Search index to use. Atlas Vector Search doesn't return results if you misspell the index
	 * name or if the specified index doesn't already exist on the cluster.
	 *
	 * @return name of the Atlas Vector Search index to use.
	 */
	@AliasFor(annotation = Hint.class, value = "indexName")
	String indexName();

	/**
	 * Indexed vector type field to search. This is defaulted from the domain model using the first Vector property found.
	 *
	 * @return an empty String by default.
	 */
	String path() default "";

	/**
	 * Takes a MongoDB JSON (MQL) string defining the pre-filter against indexed fields. Alias for
	 * {@link VectorSearch#filter}.
	 *
	 * @return an empty String by default.
	 */
	@AliasFor(annotation = Query.class)
	String value() default "";

	/**
	 * Takes a MongoDB JSON (MQL) string defining the pre-filter against indexed fields. Alias for
	 * {@link VectorSearch#value}.
	 *
	 * @return an empty String by default.
	 */
	@AliasFor(annotation = Query.class, value = "value")
	String filter() default "";

	/**
	 * Number of documents to return in the results. This value can't exceed the value of {@link #numCandidates} if you
	 * specify {@link #numCandidates}. Limit accepts Value Expressions. A Vector Search method cannot define both,
	 * {@code limit()} and a {@link org.springframework.data.domain.Limit} parameter.
	 *
	 * @return number of documents to return in the results
	 */
	String limit() default "";

	/**
	 * Number of nearest neighbors to use during the search. Value must be less than or equal to ({@code <=})
	 * {@code 10000}. You can't specify a number less than the {@link #limit() number of documents to return}. We
	 * recommend that you specify a number at least {@code 20} times higher than the {@link #limit() number of documents
	 * to return} to increase accuracy. This over-request pattern is the recommended way to trade off latency and recall
	 * in your ANN searches, and we recommend tuning this parameter based on your specific dataset size and query
	 * requirements. Required if the query uses
	 * {@link org.springframework.data.mongodb.core.aggregation.VectorSearchOperation.SearchType#ANN}.
	 *
	 * @return number of documents to return in the results
	 */
	String numCandidates() default "";

}
