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

import lombok.EqualsAndHashCode;

import java.util.Set;

import org.springframework.data.domain.ExampleMatcher;
import org.springframework.util.Assert;

/**
 * {@link ExampleMatcher} implementation for query by example (QBE). Unlike plain {@link ExampleMatcher} this untyped
 * counterpart does not enforce a strict type match when executing the query. This allows to use totally unrelated
 * example documents as references for querying collections as long as the used field/property names match.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
@EqualsAndHashCode
public class UntypedExampleMatcher implements ExampleMatcher {

	private final ExampleMatcher delegate;

	/**
	 * Creates new {@link UntypedExampleMatcher}.
	 *
	 * @param delegate must not be {@literal null}.
	 */
	private UntypedExampleMatcher(ExampleMatcher delegate) {

		Assert.notNull(delegate, "Delegate must not be null!");
		this.delegate = delegate;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#matching()
	 */
	public static UntypedExampleMatcher matching() {
		return new UntypedExampleMatcher(ExampleMatcher.matching());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#matchingAny()
	 */
	public static UntypedExampleMatcher matchingAny() {
		return new UntypedExampleMatcher(ExampleMatcher.matchingAny());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#matchingAll()
	 */
	public static UntypedExampleMatcher matchingAll() {
		return new UntypedExampleMatcher(ExampleMatcher.matchingAll());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withIgnorePaths(java.lang.String...)
	 */
	public UntypedExampleMatcher withIgnorePaths(String... ignoredPaths) {
		return new UntypedExampleMatcher(delegate.withIgnorePaths(ignoredPaths));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withStringMatcher(java.lang.String)
	 */
	public UntypedExampleMatcher withStringMatcher(StringMatcher defaultStringMatcher) {
		return new UntypedExampleMatcher(delegate.withStringMatcher(defaultStringMatcher));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withIgnoreCase()
	 */
	public UntypedExampleMatcher withIgnoreCase() {
		return new UntypedExampleMatcher(delegate.withIgnoreCase());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withIgnoreCase(boolean)
	 */
	public UntypedExampleMatcher withIgnoreCase(boolean defaultIgnoreCase) {
		return new UntypedExampleMatcher(delegate.withIgnoreCase(defaultIgnoreCase));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withMatcher(java.lang.String, org.springframework.data.domain.ExampleMatcher.MatcherConfigurer)
	 */
	public UntypedExampleMatcher withMatcher(String propertyPath,
			MatcherConfigurer<GenericPropertyMatcher> matcherConfigurer) {
		return new UntypedExampleMatcher(delegate.withMatcher(propertyPath, matcherConfigurer));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withMatcher(java.lang.String, org.springframework.data.domain.ExampleMatcher.GenericPropertyMatcher)
	 */
	public UntypedExampleMatcher withMatcher(String propertyPath, GenericPropertyMatcher genericPropertyMatcher) {
		return new UntypedExampleMatcher(delegate.withMatcher(propertyPath, genericPropertyMatcher));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withTransformer(java.lang.String, org.springframework.data.domain.ExampleMatcher.PropertyValueTransformer)
	 */
	public UntypedExampleMatcher withTransformer(String propertyPath, PropertyValueTransformer propertyValueTransformer) {
		return new UntypedExampleMatcher(delegate.withTransformer(propertyPath, propertyValueTransformer));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withIgnoreCase(java.lang.String...)
	 */
	public UntypedExampleMatcher withIgnoreCase(String... propertyPaths) {
		return new UntypedExampleMatcher(delegate.withIgnoreCase(propertyPaths));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withIncludeNullValues()
	 */
	public UntypedExampleMatcher withIncludeNullValues() {
		return new UntypedExampleMatcher(delegate.withIncludeNullValues());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withIgnoreNullValues()
	 */
	public UntypedExampleMatcher withIgnoreNullValues() {
		return new UntypedExampleMatcher(delegate.withIgnoreNullValues());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#withNullHandler(org.springframework.data.domain.ExampleMatcher.NullHandler)
	 */
	public UntypedExampleMatcher withNullHandler(NullHandler nullHandler) {
		return new UntypedExampleMatcher(delegate.withNullHandler(nullHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#getNullHandler()
	 */
	public NullHandler getNullHandler() {
		return delegate.getNullHandler();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#getDefaultStringMatcher()
	 */
	public StringMatcher getDefaultStringMatcher() {
		return delegate.getDefaultStringMatcher();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#isIgnoreCaseEnabled()
	 */
	public boolean isIgnoreCaseEnabled() {
		return delegate.isIgnoreCaseEnabled();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#isIgnoredPath()
	 */
	public boolean isIgnoredPath(String path) {
		return delegate.isIgnoredPath(path);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#getIgnoredPaths()
	 */
	public Set<String> getIgnoredPaths() {
		return delegate.getIgnoredPaths();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#getPropertySpecifiers()
	 */
	public PropertySpecifiers getPropertySpecifiers() {
		return delegate.getPropertySpecifiers();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#isAllMatching()
	 */
	public boolean isAllMatching() {
		return delegate.isAllMatching();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#isAnyMatching()
	 */
	public boolean isAnyMatching() {
		return delegate.isAnyMatching();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.ExampleMatcher#getMatchMode()
	 */
	public MatchMode getMatchMode() {
		return delegate.getMatchMode();
	}

}
