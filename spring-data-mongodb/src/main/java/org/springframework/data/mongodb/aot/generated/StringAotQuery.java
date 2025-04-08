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
package org.springframework.data.mongodb.aot.generated;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.data.repository.aot.generate.QueryMetadata;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2025/04
 */
public class StringAotQuery extends AotQuery implements QueryMetadata {

	StringQuery query;
	ExecutionType executionType;

	public StringAotQuery(StringQuery query, boolean count, boolean delete, boolean exists) {
		this.query = query;
		if(count) {
			executionType = ExecutionType.COUNT;
		} else if (exists) {
			executionType = ExecutionType.EXISTS;
		} else if (delete) {
			executionType = ExecutionType.DELETE;
		} else {
			executionType = ExecutionType.QUERY;
		}
	}

	public StringAotQuery(StringQuery query, ExecutionType executionType) {
		this.query = query;
		this.executionType = executionType;
	}

	StringAotQuery withSort(String sort) {
		query.sort(sort);
		return new StringAotQuery(query, executionType);
	}

	StringAotQuery withFields(String fields) {
		return new StringAotQuery(query.fields(fields), executionType);
	}

	@Override
	ExecutionType getExecutionType() {
		return executionType;
	}

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = new LinkedHashMap<>();

		serialized.put("filter", query.getQueryString());
		if(query.isSorted()) {
			serialized.put("sort", query.getSortString());
		}
		if(StringUtils.hasText(query.getFieldsString())) {
			serialized.put("fields", query.getFieldsString());
		}

		return serialized;

	}
}
