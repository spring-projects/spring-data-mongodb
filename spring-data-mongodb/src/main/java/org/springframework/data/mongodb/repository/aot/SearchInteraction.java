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
package org.springframework.data.mongodb.repository.aot;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation.SearchType;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.query.MongoParameters;
import org.springframework.data.repository.aot.generate.QueryMetadata;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class SearchInteraction extends MongoInteraction implements QueryMetadata {

	private final Class<?> domainType;
	private final StringQuery filter;
	private final @Nullable VectorSearch vectorSearch;
	private final MongoParameters parameters;

	public SearchInteraction(Class<?> domainType, @Nullable VectorSearch vectorSearch, StringQuery filter,
			MongoParameters parameters) {

		this.domainType = domainType;
		this.vectorSearch = vectorSearch;

		this.filter = filter;
		this.parameters = parameters;
	}

	public StringQuery getFilter() {
		return filter;
	}

	@Nullable
	String getIndexName() {
		return vectorSearch != null ? vectorSearch.indexName() : null;
	}

	public MongoParameters getParameters() {
		return parameters;
	}

	@Override
	InteractionType getExecutionType() {
		return InteractionType.AGGREGATION;
	}

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = new LinkedHashMap<>();

		if (vectorSearch != null && StringUtils.hasText(vectorSearch.indexName())) {
			serialized.put("index", vectorSearch.indexName());
		}

		serialized.put("path", getSearchPath());

		if (vectorSearch.searchType().equals(SearchType.ENN)) {
			serialized.put("exact", true);
		}

		if (StringUtils.hasText(filter.getQueryString())) {
			serialized.put("filter", filter.getQueryString());
		}

		String limit = limitParameter();
		if (StringUtils.hasText(limit)) {
			serialized.put("limit", limit);
		}

		if (StringUtils.hasText(vectorSearch.numCandidates())) {
			serialized.put("numCandidates", vectorSearch.numCandidates());
		} else if (StringUtils.hasText(limit)) {
			serialized.put("numCandidates", limit + " * 20");
		}

		serialized.put("queryVector", "?" + parameters.getVectorIndex());

		return Map.of("pipeline", List.of(new Document("$vectorSearch", serialized)
				.toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()).replaceAll("\\\"", "'")));
	}

	private @Nullable String limitParameter() {

		if (parameters.hasLimitParameter()) {
			return "?" + parameters.getLimitIndex();
		} else if (StringUtils.hasText(vectorSearch.limit())) {
			return vectorSearch.limit();
		}
		return null;
	}

	public String getSearchPath() {

		if (vectorSearch != null && StringUtils.hasText(vectorSearch.path())) {
			return vectorSearch.path();
		}

		Field[] declaredFields = domainType.getDeclaredFields();
		for (Field field : declaredFields) {
			if (Vector.class.isAssignableFrom(field.getType())) {
				return field.getName();
			}
		}

		throw new IllegalArgumentException("No vector search path found for type %s".formatted(domainType));
	}
}
