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

import java.util.Map;

/**
 * An {@link MongoInteraction} to execute an aggregation update.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class AggregationUpdateInteraction extends AggregationInteraction {

	private final QueryInteraction filter;

	AggregationUpdateInteraction(QueryInteraction filter, String[] raw) {

		super(raw);
		this.filter = filter;
	}

	QueryInteraction getFilter() {
		return filter;
	}

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = filter.serialize();
		serialized.putAll(super.serialize());
		return serialized;
	}

	@Override
	protected String pipelineSerializationKey() {
		return "update-" + super.pipelineSerializationKey();
	}
}
