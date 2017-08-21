/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;

/**
 * {@link Example} implementation for query by example (QBE). Unlike plain {@link Example} this untyped counterpart does
 * not enforce a strict type match when executing the query. This allows to use totally unrelated example documents as
 * references for querying collections as long as the used field/property names match.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public class UntypedExample implements Example<Object> {

	private final @NonNull Object probe;
	private final @NonNull ExampleMatcher matcher;

	/**
	 * Create a new {@literal untyped} {@link Example} including all non-null properties by default.
	 *
	 * @param probe must not be {@literal null}.
	 * @return new instance of {@link Example}.
	 */
	public static Example<Object> of(Object probe) {
		return new UntypedExample(probe, ExampleMatcher.matching());
	}

	/**
	 * Create a new {@literal untyped} {@link Example} using the given {@link ExampleMatcher}.
	 *
	 * @param probe must not be {@literal null}.
	 * @param matcher must not be {@literal null}.
	 * @return new instance of {@link Example}.
	 */
	public static Example<Object> of(Object probe, ExampleMatcher matcher) {
		return new UntypedExample(probe, matcher);
	}
}
