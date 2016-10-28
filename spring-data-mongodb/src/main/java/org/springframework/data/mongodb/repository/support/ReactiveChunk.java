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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoProcessor;

/**
 * A reactive chunk of data restricted by the configured {@link Pageable}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
//TODO: should that one move to SD Commons
abstract class ReactiveChunk<T> implements Slice<T>, Serializable {

	private static final long serialVersionUID = 867755909294344406L;

	private final Flux<T> content;
	private final MonoProcessor<List<T>> processor;
	private volatile List<T> contentCache;
	private final Pageable pageable;

	/**
	 * Creates a new {@link ReactiveChunk} with the given content and the given governing {@link Pageable}.
	 *
	 * @param content must not be {@literal null}.
	 * @param pageable can be {@literal null}.
	 */
	public ReactiveChunk(Flux<? extends T> content, Pageable pageable) {

		Assert.notNull(content, "Content must not be null!");

		this.content = (Flux) content;
		this.pageable = pageable;
		this.processor = this.content.collectList().doOnSuccess(list -> {

			if (list.size() > pageable.getPageSize()) {
				contentCache = list.subList(0, pageable.getPageSize());
			} else {
				contentCache = list;
			}
		}).subscribe();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#getNumber()
	 */
	public int getNumber() {
		return pageable == null ? 0 : pageable.getPageNumber();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#getSize()
	 */
	public int getSize() {
		return pageable == null ? 0 : pageable.getPageSize();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#getNumberOfElements()
	 */
	public int getNumberOfElements() {
		return getContent().size();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#hasPrevious()
	 */
	public boolean hasPrevious() {
		return getNumber() > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#isFirst()
	 */
	public boolean isFirst() {
		return !hasPrevious();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#isLast()
	 */
	public boolean isLast() {
		return !hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#nextPageable()
	 */
	public Pageable nextPageable() {
		return hasNext() ? pageable.next() : null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#previousPageable()
	 */
	public Pageable previousPageable() {

		if (hasPrevious()) {
			return pageable.previousOrFirst();
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#hasContent()
	 */
	public boolean hasContent() {
		return !getContent().isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#getContent()
	 */
	public List<T> getContent() {

		if (contentCache != null) {
			return Collections.unmodifiableList(contentCache);
		}

		List<T> list = processor.block();

		if (list.size() > pageable.getPageSize()) {
			return list.subList(0, pageable.getPageSize());
		}

		return Collections.unmodifiableList(list);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Slice#getSort()
	 */
	public Sort getSort() {
		return pageable == null ? null : pageable.getSort();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<T> iterator() {
		return getContent().iterator();
	}

	/**
	 * Applies the given {@link Converter} to the content of the {@link ReactiveChunk}.
	 *
	 * @param converter must not be {@literal null}.
	 * @return
	 */
	protected <S> List<S> getConvertedContent(Converter<? super T, ? extends S> converter) {

		Assert.notNull(converter, "Converter must not be null!");

		List<S> result = new ArrayList<S>(getContent().size());

		for (T element : this) {
			result.add(converter.convert(element));
		}

		return result;
	}

	/**
	 * Returns whether the returned list contains more elements than specified by {@link Pageable#getPageSize()}.
	 * 
	 * @return
	 */
	protected boolean containsMore() {

		List<T> list = processor.block();

		return list.size() > pageable.getPageSize();
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

		if (!(obj instanceof ReactiveChunk<?>)) {
			return false;
		}

		ReactiveChunk<?> that = (ReactiveChunk<?>) obj;

		boolean pageableEqual = this.pageable == null ? that.pageable == null : this.pageable.equals(that.pageable);

		return pageableEqual;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += 31 * (pageable == null ? 0 : pageable.hashCode());

		return result;
	}

}
