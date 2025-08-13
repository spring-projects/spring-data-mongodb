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
 * Disk use indicates if the MongoDB server is allowed to write temporary files to disk during query/aggregation
 * execution. MongoDB 6.0 server (and later) default for {@literal allowDiskUseByDefault} is {@literal true} on server
 * side.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
public enum DiskUse {

	/**
	 * Go with the server default value and do not specify any override.
	 */
	DEFAULT,

	/**
	 * Override server default value and explicitly allow disk writes.
	 */
	ALLOW,

	/**
	 * Override server default value and explicitly deny disk writes.
	 */
	DENY;

	/**
	 * Obtain the {@link DiskUse} corresponding to the given Boolean flag. {@literal null} is considered {@link #DEFAULT},
	 * {@literal true} as {@link #ALLOW}, {@literal false} as {@link #DENY}.
	 *
	 * @param value can be {@literal null}.
	 * @return the {@link DiskUse} corresponding to the given value.
	 */
	public static DiskUse of(@Nullable Boolean value) {
		return value != null ? (value ? ALLOW : DENY) : DEFAULT;
	}

	/**
	 * Obtain the {@link DiskUse} referred to by the given value. Considers {@literal null} or empty Strings as
	 * {@link #DEFAULT}, {@literal true} as {@link #ALLOW}, {@literal false} as {@link #DENY} and delegates other values
	 * to {@link #valueOf(String)}.
	 *
	 * @param value can be {@literal null}.
	 * @return the {@link DiskUse} corresponding to the given value.
	 * @throws IllegalArgumentException if not matching {@link DiskUse} found.
	 */
	public static DiskUse of(@Nullable String value) {

		if (!StringUtils.hasText(value)) {
			return DEFAULT;
		}

		return switch (value) {
			case "true" -> ALLOW;
			case "false" -> DENY;
			default -> valueOf(value.toUpperCase());
		};
	}
}
