/*
 * Copyright 2010-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import org.bson.Document;
import org.springframework.lang.Nullable;

/**
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface CriteriaDefinition {

	/**
	 * Get {@link Document} representation.
	 *
	 * @return never {@literal null}.
	 */
	Document getCriteriaObject();

	/**
	 * Get the identifying {@literal key}.
	 *
	 * @return can be {@literal null}.
	 * @since 1.6
	 */
	@Nullable
	String getKey();

}
