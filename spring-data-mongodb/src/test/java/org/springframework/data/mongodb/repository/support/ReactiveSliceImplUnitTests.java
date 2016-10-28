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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;

import reactor.core.publisher.Flux;

/**
 * Unit tests for {@link ReactiveSliceImpl}.
 *
 * @author Mark Paluch
 */
public class ReactiveSliceImplUnitTests {

	/**
	 * @see DATAMONGO-1444
	 */
	@Test(expected = IllegalArgumentException.class)
	public void preventsNullContentForAdvancedSetup() throws Exception {
		new ReactiveSliceImpl<Object>(null, null);
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void returnsNextPageable() {

		Slice<Object> page = new ReactiveSliceImpl<>(Flux.just(new Object(), new Object()), new PageRequest(0, 1));

		assertThat(page.isFirst(), is(true));
		assertThat(page.hasPrevious(), is(false));
		assertThat(page.previousPageable(), is(nullValue()));

		assertThat(page.isLast(), is(false));
		assertThat(page.hasNext(), is(true));
		assertThat(page.nextPageable(), is(new PageRequest(1, 1)));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void returnsPreviousPageable() {

		Slice<Object> page = new ReactiveSliceImpl<>(Flux.just(new Object()), new PageRequest(1, 1));

		assertThat(page.isFirst(), is(false));
		assertThat(page.hasPrevious(), is(true));
		assertThat(page.previousPageable(), is(new PageRequest(0, 1)));

		assertThat(page.isLast(), is(true));
		assertThat(page.hasNext(), is(false));
		assertThat(page.nextPageable(), is(nullValue()));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void transformsPageCorrectly() {

		Slice<Integer> transformed = new ReactiveSliceImpl<>(Flux.just("foo", "bar"), new PageRequest(0, 2))
				.map(String::length);

		assertThat(transformed.getContent(), hasSize(2));
		assertThat(transformed.getContent(), contains(3, 3));
	}
}
