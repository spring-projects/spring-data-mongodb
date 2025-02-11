/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.util;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Utility methods for {@link Slice} handling.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.5
 */
public class SliceUtils {

	/**
	 * Creates a {@link Slice} given {@link Pageable} and {@link List} of results.
	 *
	 * @param <T> the element type.
	 * @param resultList the source list holding the result of the request. If the result list contains more elements
	 *          (indicating a next slice is available) it is trimmed to the {@link Pageable#getPageSize() page size}.
	 * @param pageable the source pageable.
	 * @return new instance of {@link Slice}.
	 */
	public static <T> Slice<T> sliceResult(List<T> resultList, Pageable pageable) {

		boolean hasNext = resultList.size() > pageable.getPageSize();

		if (hasNext) {
			resultList = resultList.subList(0, pageable.getPageSize());
		}

		return new SliceImpl<>(resultList, pageable, hasNext);
	}

	/**
	 * Customize query for {@link #sliceResult sliced result} retrieval. If {@link Pageable#isPaged() paged} the
	 * {@link Query#limit(int) limit} is set to {@code pagesize + 1} in order to determine if more data is available.
	 *
	 * @param query the source query
	 * @param pageable paging to apply.
	 * @return new instance of {@link Query} if either {@link Pageable#isPaged() paged}, the source query otherwise.
	 */
	public static Query limitResult(Query query, Pageable pageable) {

		if (pageable.isUnpaged()) {
			return query;
		}

		Query target = Query.of(query);
		target.skip(pageable.getOffset());
		target.limit(pageable.getPageSize() + 1);

		return target;
	}
}
