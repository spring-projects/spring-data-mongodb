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
package org.springframework.data.mongodb.repository.aot;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.repository.aot.generate.QueryMetadata;
import org.springframework.util.StringUtils;

/**
 * An {@link MongoInteraction} to execute a query.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class QueryInteraction extends MongoInteraction implements QueryMetadata {

	private final AotStringQuery query;
	private final InteractionType interactionType;

	QueryInteraction(AotStringQuery query, boolean count, boolean delete, boolean exists) {

		this.query = query;
		if (count) {
			interactionType = InteractionType.COUNT;
		} else if (exists) {
			interactionType = InteractionType.EXISTS;
		} else if (delete) {
			interactionType = InteractionType.DELETE;
		} else {
			interactionType = InteractionType.QUERY;
		}
	}

	AotStringQuery getQuery() {
		return query;
	}

	QueryInteraction withSort(String sort) {
		query.sort(sort);
		return this;
	}

	QueryInteraction withFields(String fields) {
		query.fields(fields);
		return this;
	}

	@Override
	InteractionType getExecutionType() {
		return interactionType;
	}

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = new LinkedHashMap<>();

		serialized.put("filter", query.getQueryString());
		if (query.isSorted()) {
			serialized.put("sort", query.getSortString());
		}
		if (StringUtils.hasText(query.getFieldsString())) {
			serialized.put("fields", query.getFieldsString());
		}

		return serialized;
	}
}
