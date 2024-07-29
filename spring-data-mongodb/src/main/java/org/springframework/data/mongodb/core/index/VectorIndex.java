/*
 * Copyright 2024. the original author or authors.
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

/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;

/**
 * {@link IndexDefinition} for creating MongoDB
 * <a href="https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-overview/">Vector Index</a> required to
 * run {@code $vectorSearch} queries.
 *
 * @author Christoph Strobl
 */
public class VectorIndex implements IndexDefinition {

    private final String name;
    private String path;
    private int dimensions;
    private String similarity;
    private List<Filter> filters;

    /**
     * Create a new {@link VectorIndex} instance.
     *
     * @param name The name of the index.
     */
    public VectorIndex(String name) {
        this.name = name;
    }

    /**
     * Create a new {@link VectorIndex} instance using similarity based on the angle between vectors.
     *
     * @param name The name of the index.
     * @return new instance of {@link VectorIndex}.
     */
    public static VectorIndex cosine(String name) {

        VectorIndex idx = new VectorIndex(name);
        return idx.similarity(SimilarityFunction.COSINE);
    }

    /**
     * Create a new {@link VectorIndex} instance using similarity based the distance between vector ends.
     *
     * @param name The name of the index.
     * @return new instance of {@link VectorIndex}.
     */
    public static VectorIndex euclidean(String name) {

        VectorIndex idx = new VectorIndex(name);
        return idx.similarity(SimilarityFunction.EUCLIDEAN);
    }

    /**
     * Create a new {@link VectorIndex} instance using similarity based on based on both angle and magnitude of the
     * vectors.
     *
     * @param name The name of the index.
     * @return new instance of {@link VectorIndex}.
     */
    public static VectorIndex dotProduct(String name) {

        VectorIndex idx = new VectorIndex(name);
        return idx.similarity(SimilarityFunction.DOT_PRODUCT);
    }

    /**
     * The path to the field/property to index.
     *
     * @param path The path using dot notation.
     * @return this.
     */
    public VectorIndex path(String path) {

        this.path = path;
        return this;
    }

    /**
     * Number of vector dimensions enforced at index- & query-time.
     *
     * @param dimensions value between {@code 0} and {@code 4096}.
     * @return this.
     */
    public VectorIndex dimensions(int dimensions) {
        this.dimensions = dimensions;
        return this;
    }

    /**
     * Similarity function used.
     *
     * @param similarity should be one of {@literal euclidean | cosine | dotProduct}.
     * @return this.
     * @see SimilarityFunction
     * @see #similarity(SimilarityFunction)
     */
    public VectorIndex similarity(String similarity) {
        this.similarity = similarity;
        return this;
    }

    /**
     * Similarity function used.
     *
     * @param similarity must not be {@literal null}.
     * @return this.
     */
    public VectorIndex similarity(SimilarityFunction similarity) {
        return similarity(similarity.getFunctionName());
    }

    /**
     * Add a {@link Filter} that can be used to narrow search scope.
     *
     * @param filter must not be {@literal null}.
     * @return this.
     */
    public VectorIndex filter(Filter filter) {

        if (this.filters == null) {
            this.filters = new ArrayList<>(3);
        }

        this.filters.add(filter);
        return this;
    }

    /**
     * Add a field that can be used to pre filter data.
     *
     * @param path Dot notation to field/property used for filtering.
     * @return this.
     * @see #filter(Filter)
     */
    public VectorIndex filter(String path) {
        return filter(new Filter(path));
    }

    @Override
    public Document getIndexKeys() {

        // List<Document> fields = new ArrayList<>(filters.size()+1);
        // fields.

        // needs to be wrapped in new Document("definition", before sending to server
        // return new Document("fields", fields);
        return new Document();
    }

    @Override
    public Document getIndexOptions() {
        return new Document("name", name).append("type", "vectorSearch");
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public int getDimensions() {
        return dimensions;
    }

    public String getSimilarity() {
        return similarity;
    }

    public List<Filter> getFilters() {
        return filters == null ? Collections.emptyList() : filters;
    }

    public record Filter(String path) {

    }

    public enum SimilarityFunction {
        DOT_PRODUCT("dotProduct"), COSINE("cosine"), EUCLIDEAN("euclidean");

        String functionName;

        SimilarityFunction(String functionName) {
            this.functionName = functionName;
        }

        public String getFunctionName() {
            return functionName;
        }
    }
}
