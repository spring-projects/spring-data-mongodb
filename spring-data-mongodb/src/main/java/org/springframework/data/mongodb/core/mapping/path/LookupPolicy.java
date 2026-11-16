/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.path;

/**
 * Policy applied when a path segment cannot be resolved against its {@link Scope}.
 *
 * @author Christoph Strobl
 * @since 5.3
 * @see org.springframework.data.mongodb.core.aggregation.FieldLookupPolicy
 */
public enum LookupPolicy {

	/**
	 * Fail on the first unresolvable or ambiguous segment. Paths into computed fields of an exposing {@link Scope} are
	 * not validated beyond the field itself. Strict binding verifies a path against the mapping only and is no
	 * authorization check.
	 */
	STRICT,

	/**
	 * Pass unresolvable segments through as-is.
	 */
	RELAXED
}
