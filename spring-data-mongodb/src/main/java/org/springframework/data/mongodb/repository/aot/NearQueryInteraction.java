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

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.mongodb.repository.query.MongoParameters;
import org.springframework.data.repository.aot.generate.QueryMetadata;

/**
 * An {@link MongoInteraction} to execute a query.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class NearQueryInteraction extends MongoInteraction implements QueryMetadata {

	private final InteractionType interactionType;
	private final QueryInteraction query;
	private final MongoParameters parameters;

	NearQueryInteraction(QueryInteraction query, MongoParameters parameters) {
		interactionType = InteractionType.QUERY;
		this.query = query;
		this.parameters = parameters;
	}

	@Override
	InteractionType getExecutionType() {
		return interactionType;
	}

	public QueryInteraction getQuery() {
		return query;
	}

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = new LinkedHashMap<>();
		serialized.put("near", "?%s".formatted(parameters.getNearIndex()));
		if (parameters.getRangeIndex() != -1) {
			serialized.put("minDistance", "?%s".formatted(parameters.getRangeIndex()));
			serialized.put("maxDistance", "?%s".formatted(parameters.getRangeIndex()));
		} else if (parameters.getMaxDistanceIndex() != -1) {
			serialized.put("minDistance", "?%s".formatted(parameters.getMaxDistanceIndex()));
		}
		Object filter = query.serialize().get("filter"); // TODO: filter position index can be off due to bindable params
		if (filter != null) {
			serialized.put("filter", filter);
		}
		return serialized;
	}
}
