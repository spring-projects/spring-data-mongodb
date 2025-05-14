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

import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.data.repository.aot.generate.QueryMetadata;
import org.springframework.util.Assert;

/**
 * An {@link MongoInteraction} to execute an update.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 5.0
 */
class UpdateInteraction extends MongoInteraction implements QueryMetadata {

	private final QueryInteraction filter;
	private final @Nullable StringUpdate update;
	private final @Nullable Integer updateDefinitionParameter;

	UpdateInteraction(QueryInteraction filter, @Nullable StringUpdate update,
			@Nullable Integer updateDefinitionParameter) {
		this.filter = filter;
		this.update = update;
		this.updateDefinitionParameter = updateDefinitionParameter;
	}

	public QueryInteraction getFilter() {
		return filter;
	}

	public @Nullable StringUpdate getUpdate() {
		return update;
	}

	public int getRequiredUpdateDefinitionParameter() {

		Assert.notNull(updateDefinitionParameter, "UpdateDefinitionParameter must not be null!");

		return updateDefinitionParameter;
	}

	public boolean hasUpdateDefinitionParameter() {
		return updateDefinitionParameter != null;
	}

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = filter.serialize();

		if (update != null) {
			serialized.put("filter", filter.getQuery().getQueryString());
			serialized.put("update", update.getUpdateString());
		}

		return serialized;
	}

	@Override
	InteractionType getExecutionType() {
		return InteractionType.UPDATE;
	}

}
