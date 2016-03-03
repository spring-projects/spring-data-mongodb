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
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;

import reactor.core.publisher.Flux;

/**
 * Reactive {@code Page} implementation.
 *
 * @param <T> the type of which the page consists.
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactiveSliceImpl<T> extends ReactiveChunk<T> {

	private static final long serialVersionUID = 867755909294344406L;

	private final Pageable pageable;

	public ReactiveSliceImpl(Flux<T> content, Pageable pageable) {

		super(content, pageable);

		this.pageable = pageable;
	}

	public boolean hasNext() {
		return containsMore();
	}

	public <S> Slice<S> map(Converter<? super T, ? extends S> converter) {
		return new SliceImpl<>(this.getConvertedContent(converter), pageable, this.hasNext());
	}

	public String toString() {

		String contentType = "UNKNOWN";
		List content = this.getContent();
		if (content.size() > 0) {
			contentType = content.get(0).getClass().getName();
		}

		return String.format("Slice %d containing %s instances",
				this.getNumber(), contentType);
	}
}
