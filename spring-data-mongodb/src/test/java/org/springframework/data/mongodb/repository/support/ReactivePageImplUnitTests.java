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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link ReactivePageImpl}.
 *
 * @author Mark Paluch
 */
public class ReactivePageImplUnitTests {

	/**
	 * @see DATAMONGO-1444
	 */
	@Test(expected = IllegalArgumentException.class)
	public void preventsNullContentForAdvancedSetup() throws Exception {
		new ReactivePageImpl<Object>(null, null, Mono.just(0L));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void returnsNextPageable() {

		Page<Object> page = new ReactivePageImpl<>(Flux.just(new Object()), new PageRequest(0, 1), Mono.just(10L));

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
	public void returnsContentBoundedByPageSize() {

		Page<Object> page = new ReactivePageImpl<>(Flux.just(new Object(), new Object()), new PageRequest(0, 1),
				Mono.just(10L));

		assertThat(page.getContent(), hasSize(1));
		assertThat(page.hasNext(), is(true));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void returnsPreviousPageable() {

		Page<Object> page = new ReactivePageImpl<>(Flux.just(new Object()), new PageRequest(1, 1), Mono.just(2L));

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

		Page<Integer> transformed = new ReactivePageImpl<>(Flux.just("foo", "bar"), new PageRequest(0, 2), Mono.just(10L))
				.map(String::length);

		assertThat(transformed.getContent(), hasSize(2));
		assertThat(transformed.getContent(), contains(3, 3));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void adaptsTotalForLastPageOnIntermediateDeletion() {
		assertThat(new ReactivePageImpl<>(Flux.just("foo", "bar"), new PageRequest(0, 5), Mono.just(3L)).getTotalElements(),
				is(2L));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void adaptsTotalForLastPageOnIntermediateInsertion() {
		assertThat(new ReactivePageImpl<>(Flux.just("foo", "bar"), new PageRequest(0, 5), Mono.just(1L)).getTotalElements(),
				is(2L));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void adaptsTotalForLastPageOnIntermediateDeletionOnLastPate() {
		assertThat(
				new ReactivePageImpl<>(Flux.just("foo", "bar"), new PageRequest(1, 10), Mono.just(13L)).getTotalElements(),
				is(12L));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void adaptsTotalForLastPageOnIntermediateInsertionOnLastPate() {
		assertThat(
				new ReactivePageImpl<>(Flux.just("foo", "bar"), new PageRequest(1, 10), Mono.just(11L)).getTotalElements(),
				is(12L));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void doesNotAdapttotalIfPageIsEmpty() {

		assertThat(new ReactivePageImpl<String>(Flux.empty(), new PageRequest(1, 10), Mono.just(0L)).getTotalElements(),
				is(0L));
	}
}
