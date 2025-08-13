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
package org.springframework.data.mongodb.core.query;

import org.jspecify.annotations.Nullable;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public enum DiskUse {

	DEFAULT, ALLOW, DENY;

	public static DiskUse of(@Nullable Boolean value) {
		return value != null ? (value ? ALLOW : DENY) : DEFAULT;
	}

	public static DiskUse of(String value) {
		if (!StringUtils.hasText(value)) {
			return DEFAULT;
		}
		if (value.toLowerCase().equalsIgnoreCase("true")) {
			return ALLOW;
		}
		return valueOf(value.toUpperCase());
	}
}
