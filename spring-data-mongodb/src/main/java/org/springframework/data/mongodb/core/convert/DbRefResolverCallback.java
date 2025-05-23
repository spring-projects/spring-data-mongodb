/*
 * Copyright 2013-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Callback interface to be used in conjunction with {@link DbRefResolver}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 */
public interface DbRefResolverCallback {

	/**
	 * Resolve the final object for the given {@link MongoPersistentProperty}.
	 *
	 * @param property will never be {@literal null}.
	 * @return
	 */
	@Nullable Object resolve(MongoPersistentProperty property);
}
