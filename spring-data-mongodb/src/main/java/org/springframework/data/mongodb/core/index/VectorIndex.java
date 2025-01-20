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
import java.util.List;
import java.util.function.Consumer;

import org.bson.Document;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Contract;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link IndexDefinition} for creating MongoDB
 * <a href="https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-overview/">Vector Index</a> required to
 * run {@code $vectorSearch} queries.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.5
 */
public class VectorIndex implements SearchIndexDefinition {

	private final String name;
	private final List<Object> fields = new ArrayList<>();

	/**
	 * Create a new {@link VectorIndex} instance.
	 *
	 * @param name The name of the index.
	 */
	public VectorIndex(String name) {
		this.name = name;
	}

	/**
	 * Add a filter field.
	 *
	 * @param path dot notation to field/property used for filtering.
	 * @return this.
	 */
	@Contract("_ -> this")
	public VectorIndex addFilter(String path) {

		Assert.hasText(path, "Path must not be null or empty");

		fields.add(new VectorFilterField(path, "filter"));
		return this;
	}

	/**
	 * Add a vector field and accept a {@link VectorFieldBuilder} customizer.
	 *
	 * @param path dot notation to field/property used for filtering.
	 * @param customizer customizer function.
	 * @return this.
	 */
	@Contract("_, _ -> this")
	public VectorIndex addVector(String path, Consumer<VectorFieldBuilder> customizer) {

		Assert.hasText(path, "Path must not be null or empty");

		VectorFieldBuilder builder = new VectorFieldBuilder(path, "vector");
		customizer.accept(builder);

		fields.add(
				new VectorIndexField(builder.path, builder.type, builder.dimensions, builder.similarity, builder.quantization));

		return this;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getType() {
		return "vectorSearch";
	}

	@Override
	public Document getDefinition(@Nullable TypeInformation<?> entity,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		if (fields.isEmpty()) {
			throw new IllegalStateException("At least one vector or filter field must be added to the index");
		}

		MongoPersistentEntity<?> persistentEntity = entity != null ? mappingContext.getPersistentEntity(entity) : null;

		Document definition = new Document();
		List<Document> fields = new ArrayList<>();
		definition.put("fields", fields);

		for (Object field : this.fields) {

			if (field instanceof VectorFilterField vff) {

				Document filter = new Document("type", "filter");
				filter.put("path", resolvePath(vff.path(), persistentEntity, mappingContext));
				fields.add(filter);
			}

			if (field instanceof VectorIndexField vif) {

				Document filter = new Document("type", "vector");
				filter.put("path", resolvePath(vif.path(), persistentEntity, mappingContext));
				filter.put("numDimensions", vif.dimensions());
				filter.put("similarity", vif.similarity());
				filter.put("quantization", vif.quantization());
				fields.add(filter);
			}

		}

		return definition;
	}

	private String resolvePath(String path, @Nullable MongoPersistentEntity<?> persistentEntity,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		if (persistentEntity == null) {
			return path;
		}

		QueryMapper.MetadataBackedField mbf = new QueryMapper.MetadataBackedField(path, persistentEntity, mappingContext);

		return mbf.getMappedKey();
	}

	record VectorIndexField(String path, String type, int dimensions, String similarity, String quantization) {
	}

	record VectorFilterField(String path, String type) {
	}

	public static class VectorFieldBuilder {

		private final String path;
		private final String type;

		private int dimensions;
		private @Nullable String similarity;
		private String quantization = "none";

		VectorFieldBuilder(String path, String type) {
			this.path = path;
			this.type = type;
		}

		/**
		 * Number of vector dimensions enforced at index- & query-time.
		 *
		 * @param dimensions value between {@code 0} and {@code 4096}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public VectorFieldBuilder dimensions(int dimensions) {
			this.dimensions = dimensions;
			return this;
		}

		/**
		 * Use similarity based on the angle between vectors.
		 *
		 * @return new instance of {@link VectorIndex}.
		 */
		@Contract(" -> this")
		public VectorFieldBuilder cosine() {

			return similarity(SimilarityFunction.COSINE);
		}

		/**
		 * Use similarity based the distance between vector ends.
		 */
		@Contract(" -> this")
		public VectorFieldBuilder euclidean() {
			return similarity(SimilarityFunction.EUCLIDEAN);
		}

		/**
		 * Use similarity based on both angle and magnitude of the vectors.
		 *
		 * @param name The name of the index.
		 * @return new instance of {@link VectorIndex}.
		 */
		@Contract(" -> this")
		public VectorFieldBuilder dotProduct() {
			return similarity(SimilarityFunction.DOT_PRODUCT);
		}

		/**
		 * Similarity function used.
		 *
		 * @param similarity should be one of {@literal euclidean | cosine | dotProduct}.
		 * @return this.
		 * @see SimilarityFunction
		 * @see #similarity(SimilarityFunction)
		 */
		@Contract("_ -> this")
		public VectorFieldBuilder similarity(String similarity) {
			this.similarity = similarity;
			return this;
		}

		/**
		 * Similarity function used.
		 *
		 * @param similarity must not be {@literal null}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public VectorFieldBuilder similarity(SimilarityFunction similarity) {
			return similarity(similarity.getFunctionName());
		}

		/**
		 * Quantization used.
		 *
		 * @param quantization should be one of {@literal none | scalar | binary}.
		 * @return this.
		 * @see Quantization
		 * @see #quantization(Quantization)
		 */
		public VectorFieldBuilder quantization(String quantization) {
			this.quantization = quantization;
			return this;
		}

		/**
		 * Quntization used.
		 *
		 * @param quantization must not be {@literal null}.
		 * @return this.
		 */
		public VectorFieldBuilder quantization(Quantization quantization) {
			return quantization(quantization.getQuantizationName());
		}
	}

	public enum SimilarityFunction {
		DOT_PRODUCT("dotProduct"), COSINE("cosine"), EUCLIDEAN("euclidean");

		final String functionName;

		SimilarityFunction(String functionName) {
			this.functionName = functionName;
		}

		public String getFunctionName() {
			return functionName;
		}
	}

	public enum Quantization {
		NONE("none"), SCALAR("scalar"), BINARY("binary");

		final String quantizationName;

		Quantization(String quantizationName) {
			this.quantizationName = quantizationName;
		}

		public String getQuantizationName() {
			return quantizationName;
		}
	}
}
