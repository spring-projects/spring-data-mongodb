/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.util.json;

import org.springframework.lang.Nullable;

/**
 * A value provider to retrieve bindable values by their parameter index.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
@FunctionalInterface
public interface ValueProvider {

	/**
	 * @param index parameter index to use.
	 * @return can be {@literal null}.
	 * @throws RuntimeException if the requested element does not exist.
	 */
	@Nullable
	Object getBindableValue(int index);
}
