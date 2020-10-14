/*
 * Copyright 2020. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mapping.model;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public interface AccessorFunctionProvider<S> {

	default boolean hasSetFunctionFor(String fieldName) {
		return getSetFunctionFor(fieldName) != null;
	}

	default boolean hasGetFunctionFor(String fieldName) {
		return getGetFunctionFor(fieldName) != null;
	}

	BiFunction<S, Object, S> getSetFunctionFor(String fieldName);

	Function<S, Object> getGetFunctionFor(String fieldName);
}
