/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.bson.Document;

/**
 * Use {@link IndexFilter} to create the partial filter expression used when creating
 * <a href="https://docs.mongodb.com/manual/core/index-partial/">Partial Indexes</a>.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public interface IndexFilter {

	/**
	 * Get the raw (unmapped) filter expression.
	 *
	 * @return never {@literal null}.
	 */
	Document getFilterObject();

}
