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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit test for {@link SliceUtils}.
 *
 * @author Christoph Strobl
 */
class SliceUtilsUnitTests {

	@ParameterizedTest // GH-4889
	@MethodSource("paged")
	void pagedPageableModifiesQuery(Pageable page) {

		Query source = new BasicQuery(Document.parse("{ 'spring' : 'data' }"));

		Query target = SliceUtils.limitResult(source, page);

		assertThat(target.getQueryObject()).isEqualTo(source.getQueryObject());
		assertThat(target).isNotSameAs(source);
		assertThat(target.isLimited()).isTrue();
		assertThat(target.getSkip()).isEqualTo(page.getOffset());
		assertThat(target.getLimit()).isEqualTo(page.toLimit().max() + 1);
		assertThat(target.getSortObject()).isEqualTo(source.getSortObject());
	}

	@ParameterizedTest // GH-4889
	@MethodSource("unpaged")
	void unpagedPageableDoesNotModifyQuery(Pageable page) {

		Query source = spy(new BasicQuery(Document.parse("{ 'spring' : 'data' }")));

		Query target = SliceUtils.limitResult(source, page);

		verifyNoInteractions(source);

		assertThat(target).isSameAs(source);
		assertThat(target.isLimited()).isFalse();
	}

	public static Stream<Arguments> paged() {
		return Stream.of(Arguments.of(Pageable.ofSize(1)), Arguments.of(PageRequest.of(0, 10)),
				Arguments.of(PageRequest.of(0, 10, Direction.ASC, "name")));
	}

	public static Stream<Arguments> unpaged() {
		return Stream.of(Arguments.of(Pageable.unpaged()), Arguments.of(Pageable.unpaged(Sort.by("name"))));
	}
}
