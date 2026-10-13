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
package org.springframework.data.mongodb.repository.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility to materialize repository method {@link Iterable} arguments.
 * <p>
 * Methods return the source itself if it already has the requested type and copy it otherwise. Callers must not mutate
 * the result as it may be the caller-provided argument.
 *
 * @author Mark Paluch
 * @since 5.2
 */
class IterableUtils {

	/**
	 * Return {@code source} as {@link List}, copying it unless it already is a {@link List}.
	 */
	public static <E> List<E> toList(Iterable<E> source) {
		return source instanceof List<E> list ? list : new ArrayList<>(toCollection(source));
	}

	/**
	 * Return {@code source} as {@link Collection}, copying it unless it already is a {@link Collection}.
	 */
	public static <E> Collection<E> toCollection(Iterable<E> source) {

		if (source instanceof Collection<E> collection) {
			return collection;
		}

		List<E> list = new ArrayList<>();
		source.forEach(list::add);

		return list;
	}

}
