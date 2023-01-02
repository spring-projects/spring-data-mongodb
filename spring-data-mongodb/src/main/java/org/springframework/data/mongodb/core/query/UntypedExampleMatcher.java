/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Set;

import org.springframework.data.domain.ExampleMatcher;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * {@link ExampleMatcher} implementation for query by example (QBE). Unlike plain {@link ExampleMatcher} this untyped
 * counterpart does not enforce type matching when executing the query. This allows to query unrelated example documents
 * as references for querying collections as long as the used field/property names match.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class UntypedExampleMatcher implements ExampleMatcher {

	private final ExampleMatcher delegate;

	private UntypedExampleMatcher(ExampleMatcher delegate) {
		this.delegate = delegate;
	}

	public static UntypedExampleMatcher matching() {
		return new UntypedExampleMatcher(ExampleMatcher.matching());
	}

	public static UntypedExampleMatcher matchingAny() {
		return new UntypedExampleMatcher(ExampleMatcher.matchingAny());
	}

	public static UntypedExampleMatcher matchingAll() {
		return new UntypedExampleMatcher(ExampleMatcher.matchingAll());
	}

	public UntypedExampleMatcher withIgnorePaths(String... ignoredPaths) {
		return new UntypedExampleMatcher(delegate.withIgnorePaths(ignoredPaths));
	}

	public UntypedExampleMatcher withStringMatcher(StringMatcher defaultStringMatcher) {
		return new UntypedExampleMatcher(delegate.withStringMatcher(defaultStringMatcher));
	}

	public UntypedExampleMatcher withIgnoreCase() {
		return new UntypedExampleMatcher(delegate.withIgnoreCase());
	}

	public UntypedExampleMatcher withIgnoreCase(boolean defaultIgnoreCase) {
		return new UntypedExampleMatcher(delegate.withIgnoreCase(defaultIgnoreCase));
	}

	public UntypedExampleMatcher withMatcher(String propertyPath,
			MatcherConfigurer<GenericPropertyMatcher> matcherConfigurer) {
		return new UntypedExampleMatcher(delegate.withMatcher(propertyPath, matcherConfigurer));
	}

	public UntypedExampleMatcher withMatcher(String propertyPath, GenericPropertyMatcher genericPropertyMatcher) {
		return new UntypedExampleMatcher(delegate.withMatcher(propertyPath, genericPropertyMatcher));
	}

	public UntypedExampleMatcher withTransformer(String propertyPath, PropertyValueTransformer propertyValueTransformer) {
		return new UntypedExampleMatcher(delegate.withTransformer(propertyPath, propertyValueTransformer));
	}

	public UntypedExampleMatcher withIgnoreCase(String... propertyPaths) {
		return new UntypedExampleMatcher(delegate.withIgnoreCase(propertyPaths));
	}

	public UntypedExampleMatcher withIncludeNullValues() {
		return new UntypedExampleMatcher(delegate.withIncludeNullValues());
	}

	public UntypedExampleMatcher withIgnoreNullValues() {
		return new UntypedExampleMatcher(delegate.withIgnoreNullValues());
	}

	public UntypedExampleMatcher withNullHandler(NullHandler nullHandler) {
		return new UntypedExampleMatcher(delegate.withNullHandler(nullHandler));
	}

	public NullHandler getNullHandler() {
		return delegate.getNullHandler();
	}

	public StringMatcher getDefaultStringMatcher() {
		return delegate.getDefaultStringMatcher();
	}

	public boolean isIgnoreCaseEnabled() {
		return delegate.isIgnoreCaseEnabled();
	}

	public boolean isIgnoredPath(String path) {
		return delegate.isIgnoredPath(path);
	}

	public Set<String> getIgnoredPaths() {
		return delegate.getIgnoredPaths();
	}

	public PropertySpecifiers getPropertySpecifiers() {
		return delegate.getPropertySpecifiers();
	}

	public boolean isAllMatching() {
		return delegate.isAllMatching();
	}

	public boolean isAnyMatching() {
		return delegate.isAnyMatching();
	}

	public MatchMode getMatchMode() {
		return delegate.getMatchMode();
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		UntypedExampleMatcher that = (UntypedExampleMatcher) o;

		return ObjectUtils.nullSafeEquals(delegate, that.delegate);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(delegate);
	}
}
