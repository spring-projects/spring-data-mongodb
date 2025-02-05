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
package org.springframework.data.mongodb.repository.support;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Utility methods for {@link Slice} handling.
 *
 * @author Mark Paluch
 * @since 4.5
 */
class SliceUtils {

	/**
	 * Creates a {@link Slice} given {@link Pageable} and {@link List} of results.
	 *
	 * @param <T>
	 * @param resultList
	 * @param pageable
	 * @return
	 */
	public static <T> Slice<T> getSlice(List<T> resultList, Pageable pageable) {

		boolean hasNext = resultList.size() > pageable.getPageSize();

		if (hasNext) {
			resultList = resultList.subList(0, pageable.getPageSize());
		}

		return new SliceImpl<>(resultList, pageable, hasNext);
	}

	/**
	 * Customize query for slice retrieval.
	 *
	 * @param query
	 * @param pageable
	 * @return
	 */
	public static Query getQuery(Query query, Pageable pageable) {

		query.with(pageable);

		return pageable.isPaged() ? query.limit(pageable.getPageSize() + 1) : query;
	}
}
