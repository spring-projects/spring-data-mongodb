/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.util;

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Value object representing a dot path.
 *
 * @author Mark Paluch
 * @since 3.2
 */
public class DotPath {

	private static final DotPath EMPTY = new DotPath("");

	private final String path;

	private DotPath(String path) {
		this.path = path;
	}

	/**
	 * Creates a new {@link DotPath} from {@code dotPath}.
	 *
	 * @param dotPath the dot path, can be empty or {@literal null}.
	 * @return the {@link DotPath} representing {@code dotPath}.
	 */
	public static DotPath from(@Nullable String dotPath) {

		if (StringUtils.hasLength(dotPath)) {
			return new DotPath(dotPath);
		}

		return EMPTY;
	}

	/**
	 * Returns an empty dotpath.
	 *
	 * @return an empty dotpath.
	 */
	public static DotPath empty() {
		return EMPTY;
	}

	/**
	 * Append a segment to the dotpath. If the dotpath is not empty, then segments are separated with a dot.
	 *
	 * @param segment the segment to append.
	 * @return
	 */
	public DotPath append(String segment) {

		if (isEmpty()) {
			return new DotPath(segment);
		}

		return new DotPath(path + "." + segment);
	}

	/**
	 * Returns whether this dotpath is empty.
	 *
	 * @return whether this dotpath is empty.
	 */
	public boolean isEmpty() {
		return !StringUtils.hasLength(path);
	}

	@Override
	public String toString() {
		return path;
	}
}
