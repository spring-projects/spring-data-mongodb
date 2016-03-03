/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Reactive {@code Page} implementation.
 *
 * @param <T> the type of which the page consists.
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactivePageImpl<T> extends ReactiveChunk<T> implements Page<T> {

	private static final long serialVersionUID = 867755909294344406L;

	private final MonoProcessor<Long> totalMono;
	private volatile Long totalValueCache;
	private final Pageable pageable;

	/**
	 * Constructor of {@code PageImpl}.
	 *
	 * @param content the content of this page, must not be {@literal null}.
	 * @param pageable the paging information, can be {@literal null}.
	 * @param totalMono the total amount of items available. The total might be adapted considering the length of the
	 *          content given, if it is going to be the content of the last page. This is in place to mitigate
	 *          inconsistencies
	 */
	public ReactivePageImpl(Flux<? extends T> content, Pageable pageable, Mono<Long> totalMono) {

		super(content, pageable);

		this.pageable = pageable;
		this.totalMono = totalMono.subscribe();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Page#getTotalPages()
	 */
	@Override
	public int getTotalPages() {
		return getSize() == 0 ? 1 : (int) Math.ceil((double) getTotal0() / (double) getSize());
	}

	private long getTotal0() {

		if (totalValueCache == null) {
			long total = totalMono.block();
			List<T> content = getContent();
			this.totalValueCache = !content.isEmpty() && pageable != null
					&& pageable.getOffset() + pageable.getPageSize() > total ? pageable.getOffset() + content.size() : total;

		}

		return totalValueCache;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Page#getTotalElements()
	 */
	@Override
	public long getTotalElements() {
		return getTotal0();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return getNumber() + 1 < getTotalPages();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#isLast()
	 */
	@Override
	public boolean isLast() {
		return !hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#transform(org.springframework.core.convert.converter.Converter)
	 */
	@Override
	public <S> Page<S> map(Converter<? super T, ? extends S> converter) {
		return new ReactivePageImpl<S>(Flux.fromIterable(getConvertedContent(converter)), pageable, Mono.just(getTotal0()));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		String contentType = "UNKNOWN";
		List<T> content = getContent();

		if (content.size() > 0) {
			contentType = content.get(0).getClass().getName();
		}

		return String.format("Page %s of %d containing %s instances", getNumber() + 1, getTotalPages(), contentType);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof ReactivePageImpl<?>)) {
			return false;
		}

		ReactivePageImpl<?> that = (ReactivePageImpl<?>) obj;

		return getTotal0() == that.getTotal0() && super.equals(obj);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += 31 * (int) (getTotal0() ^ getTotal0() >>> 32);
		result += 31 * super.hashCode();

		return result;
	}
}
