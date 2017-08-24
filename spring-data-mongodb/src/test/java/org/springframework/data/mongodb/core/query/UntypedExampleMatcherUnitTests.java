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

import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.Matchers.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.ExampleMatcher.NullHandler;
import org.springframework.data.domain.ExampleMatcher.StringMatcher;

/**
 * @author Christoph Strobl
 */
public class UntypedExampleMatcherUnitTests {

	ExampleMatcher matcher;

	@Before
	public void setUp() throws Exception {
		matcher = UntypedExampleMatcher.matching();
	}

	@Test // DATAMONGO-1768
	public void defaultStringMatcherShouldReturnDefault() {
		assertThat(matcher.getDefaultStringMatcher()).isEqualTo(StringMatcher.DEFAULT);
	}

	@Test // DATAMONGO-1768
	public void ignoreCaseShouldReturnFalseByDefault() {
		assertThat(matcher.isIgnoreCaseEnabled()).isFalse();
	}

	@Test // DATAMONGO-1768
	public void ignoredPathsIsEmptyByDefault() {
		assertThat(matcher.getIgnoredPaths()).isEmpty();
	}

	@Test // DATAMONGO-1768
	public void nullHandlerShouldReturnIgnoreByDefault() {
		assertThat(matcher.getNullHandler()).isEqualTo(NullHandler.IGNORE);
	}

	@Test(expected = UnsupportedOperationException.class) // DATAMONGO-1768
	public void ignoredPathsIsNotModifiable() throws Exception {
		matcher.getIgnoredPaths().add("¯\\_(ツ)_/¯");
	}

	@Test // DATAMONGO-1768
	public void ignoreCaseShouldReturnTrueWhenIgnoreCaseEnabled() {

		matcher = UntypedExampleMatcher.matching().withIgnoreCase();

		assertThat(matcher.isIgnoreCaseEnabled()).isTrue();
	}

	@Test // DATAMONGO-1768
	public void ignoreCaseShouldReturnTrueWhenIgnoreCaseSet() {

		matcher = UntypedExampleMatcher.matching().withIgnoreCase(true);

		assertThat(matcher.isIgnoreCaseEnabled()).isTrue();
	}

	@Test // DATAMONGO-1768
	public void nullHandlerShouldReturnInclude() throws Exception {

		matcher = UntypedExampleMatcher.matching().withIncludeNullValues();

		assertThat(matcher.getNullHandler()).isEqualTo(NullHandler.INCLUDE);
	}

	@Test // DATAMONGO-1768
	public void nullHandlerShouldReturnIgnore() {

		matcher = UntypedExampleMatcher.matching().withIgnoreNullValues();

		assertThat(matcher.getNullHandler()).isEqualTo(NullHandler.IGNORE);
	}

	@Test // DATAMONGO-1768
	public void nullHandlerShouldReturnConfiguredValue() {

		matcher = UntypedExampleMatcher.matching().withNullHandler(NullHandler.INCLUDE);

		assertThat(matcher.getNullHandler()).isEqualTo(NullHandler.INCLUDE);
	}

	@Test // DATAMONGO-1768
	public void ignoredPathsShouldReturnCorrectProperties() {

		matcher = UntypedExampleMatcher.matching().withIgnorePaths("foo", "bar", "baz");

		assertThat(matcher.getIgnoredPaths()).contains("foo", "bar", "baz");
		assertThat(matcher.getIgnoredPaths()).hasSize(3);
	}

	@Test // DATAMONGO-1768
	public void ignoredPathsShouldReturnUniqueProperties() {

		matcher = UntypedExampleMatcher.matching().withIgnorePaths("foo", "bar", "foo");

		assertThat(matcher.getIgnoredPaths()).contains("foo", "bar");
		assertThat(matcher.getIgnoredPaths()).hasSize(2);
	}

	@Test // DATAMONGO-1768
	public void withCreatesNewInstance() {

		matcher = UntypedExampleMatcher.matching().withIgnorePaths("foo", "bar", "foo");
		ExampleMatcher configuredExampleSpec = matcher.withIgnoreCase();

		assertThat(matcher).isNotEqualTo(sameInstance(configuredExampleSpec));
		assertThat(matcher.getIgnoredPaths()).hasSize(2);
		assertThat(matcher.isIgnoreCaseEnabled()).isFalse();

		assertThat(configuredExampleSpec.getIgnoredPaths()).hasSize(2);
		assertThat(configuredExampleSpec.isIgnoreCaseEnabled()).isTrue();
	}

	@Test // DATAMONGO-1768
	public void defaultMatcherRequiresAllMatching() {

		assertThat(UntypedExampleMatcher.matching().isAllMatching()).isTrue();
		assertThat(UntypedExampleMatcher.matching().isAnyMatching()).isFalse();
	}

	@Test // DATAMONGO-1768
	public void allMatcherRequiresAllMatching() {

		assertThat(UntypedExampleMatcher.matchingAll().isAllMatching()).isTrue();
		assertThat(UntypedExampleMatcher.matchingAll().isAnyMatching()).isFalse();
	}

	@Test // DATAMONGO-1768
	public void anyMatcherYieldsAnyMatching() {

		assertThat(UntypedExampleMatcher.matchingAny().isAnyMatching()).isTrue();
		assertThat(UntypedExampleMatcher.matchingAny().isAllMatching()).isFalse();
	}

	@Test // DATAMONGO-1768
	public void shouldCompareUsingHashCodeAndEquals() {

		matcher = UntypedExampleMatcher.matching() //
				.withIgnorePaths("foo", "bar", "baz") //
				.withNullHandler(NullHandler.IGNORE) //
				.withIgnoreCase("ignored-case") //
				.withMatcher("hello", ExampleMatcher.GenericPropertyMatchers.contains().caseSensitive()) //
				.withMatcher("world", matcher -> matcher.endsWith());

		ExampleMatcher sameAsMatcher = UntypedExampleMatcher.matching() //
				.withIgnorePaths("foo", "bar", "baz") //
				.withNullHandler(NullHandler.IGNORE) //
				.withIgnoreCase("ignored-case") //
				.withMatcher("hello", ExampleMatcher.GenericPropertyMatchers.contains().caseSensitive()) //
				.withMatcher("world", matcher -> matcher.endsWith());

		ExampleMatcher different = UntypedExampleMatcher.matching() //
				.withIgnorePaths("foo", "bar", "baz") //
				.withNullHandler(NullHandler.IGNORE) //
				.withMatcher("hello", ExampleMatcher.GenericPropertyMatchers.contains().ignoreCase());

		assertThat(matcher.hashCode()).isEqualTo(sameAsMatcher.hashCode());
		assertThat(matcher.hashCode()).isNotEqualTo(different.hashCode());
		assertThat(matcher).isEqualTo(sameAsMatcher);
		assertThat(matcher).isNotEqualTo(different);
	}
}
