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

/**
 * @author Christoph Strobl
 * @since 2025/04
 */
public class StringAotQuery extends AotQuery {

	StringQuery query;
	boolean count, delete, exists;

	public StringAotQuery(StringQuery query, boolean count, boolean delete, boolean exists) {
		this.query = query;
		this.count = count;
		this.delete = delete;
		this.exists = exists;
	}

	StringAotQuery withSort(String sort) {
		query.sort(sort);
		return new StringAotQuery(query, count, delete, exists);
	}

	StringAotQuery withFields(String fields) {
		return new StringAotQuery(query.fields(fields), count, delete, exists);
	}

	@Override
	boolean isCountQuery() {
		return count;
	}

	@Override
	boolean isDeleteQuery() {
		return delete;
	}

	@Override
	boolean isExists() {
		return exists;
	}
}
