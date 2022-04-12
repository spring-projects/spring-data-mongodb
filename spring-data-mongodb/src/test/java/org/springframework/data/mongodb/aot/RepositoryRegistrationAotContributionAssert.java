/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.mongodb.aot;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.assertj.core.api.AbstractAssert;
import org.springframework.aot.generate.ClassNameGenerator;
import org.springframework.aot.generate.DefaultGenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.aot.BeanRegistrationCode;
import org.springframework.data.aot.RepositoryRegistrationAotContribution;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryFragment;
import org.springframework.lang.NonNull;

/**
 * @author Christoph Strobl
 * @since 2022/04
 */
public class RepositoryRegistrationAotContributionAssert
		extends AbstractAssert<RepositoryRegistrationAotContributionAssert, RepositoryRegistrationAotContribution>  {

	@NonNull
	public static RepositoryRegistrationAotContributionAssert assertThatContribution(
			@NonNull RepositoryRegistrationAotContribution actual) {

		return new RepositoryRegistrationAotContributionAssert(actual);
	}

	public RepositoryRegistrationAotContributionAssert(@NonNull RepositoryRegistrationAotContribution actual) {
		super(actual, RepositoryRegistrationAotContributionAssert.class);
	}

	public RepositoryRegistrationAotContributionAssert targetRepositoryTypeIs(Class<?> expected) {

		assertThat(getRepositoryInformation().getRepositoryInterface()).isEqualTo(expected);

		return this.myself;
	}

	public RepositoryRegistrationAotContributionAssert hasNoFragments() {

		assertThat(getRepositoryInformation().getFragments()).isEmpty();

		return this;
	}

	public RepositoryRegistrationAotContributionAssert hasFragments() {

		assertThat(getRepositoryInformation().getFragments()).isNotEmpty();

		return this;
	}

	public RepositoryRegistrationAotContributionAssert verifyFragments(Consumer<Set<RepositoryFragment<?>>> consumer) {

		assertThat(getRepositoryInformation().getFragments())
				.satisfies(it -> consumer.accept(new LinkedHashSet<>(it)));

		return this;
	}

	public RepositoryRegistrationAotContributionAssert codeContributionSatisfies(
			Consumer<CodeContributionAssert> assertWith) {

		BeanRegistrationCode mockBeanRegistrationCode = mock(BeanRegistrationCode.class);

		DefaultGenerationContext generationContext =
				new DefaultGenerationContext(new ClassNameGenerator(), new InMemoryGeneratedFiles(), new RuntimeHints());

		this.actual.applyTo(generationContext, mockBeanRegistrationCode);

		assertWith.accept(new CodeContributionAssert(generationContext));

		return this;
	}

	private RepositoryInformation getRepositoryInformation() {

		assertThat(this.actual)
				.describedAs("No repository interface found on null bean contribution")
				.isNotNull();

		assertThat(this.actual.getRepositoryInformation())
				.describedAs("No repository interface found on null repository information")
				.isNotNull();

		return this.actual.getRepositoryInformation();
	}
}
