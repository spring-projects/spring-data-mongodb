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

import java.util.Arrays;
import java.util.stream.Stream;

import org.assertj.core.api.AbstractAssert;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.hint.ClassProxyHint;
import org.springframework.aot.hint.JdkProxyHint;
import org.springframework.aot.hint.RuntimeHintsPredicates;

/**
 * @author Christoph Strobl
 * @since 2022/04
 */
public class CodeContributionAssert extends AbstractAssert<CodeContributionAssert, GenerationContext> {

	public CodeContributionAssert(GenerationContext contribution) {
		super(contribution, CodeContributionAssert.class);
	}

	public CodeContributionAssert contributesReflectionFor(Class<?>... types) {

		for (Class<?> type : types) {
			assertThat(this.actual.getRuntimeHints())
					.describedAs("No reflection entry found for [%s]", type)
					.matches(RuntimeHintsPredicates.reflection().onType(type));
		}

		return this;
	}

	public CodeContributionAssert doesNotContributeReflectionFor(Class<?>... types) {

		for (Class<?> type : types) {
			assertThat(this.actual.getRuntimeHints())
					.describedAs("Reflection entry found for [%s]", type)
					.matches(RuntimeHintsPredicates.reflection().onType(type).negate());
		}

		return this;
	}

	public CodeContributionAssert contributesJdkProxyFor(Class<?> entryPoint) {

		assertThat(jdkProxiesFor(entryPoint).findFirst())
				.describedAs("No JDK proxy found for [%s]", entryPoint)
				.isPresent();

		return this;
	}

	public CodeContributionAssert doesNotContributeJdkProxyFor(Class<?> entryPoint) {

		assertThat(jdkProxiesFor(entryPoint).findFirst())
				.describedAs("Found JDK proxy matching [%s] though it should not be present", entryPoint)
				.isNotPresent();

		return this;
	}

	public CodeContributionAssert contributesJdkProxy(Class<?>... proxyInterfaces) {

		assertThat(jdkProxiesFor(proxyInterfaces[0]))
				.describedAs("Unable to find JDK proxy matching [%s]", Arrays.asList(proxyInterfaces))
				.anySatisfy(it -> new JdkProxyAssert(it).matches(proxyInterfaces));

		return this;
	}

	public CodeContributionAssert doesNotContributeJdkProxy(Class<?>... proxyInterfaces) {

		assertThat(jdkProxiesFor(proxyInterfaces[0]))
				.describedAs("Found JDK proxy matching [%s] though it should not be present",
						Arrays.asList(proxyInterfaces))
				.noneSatisfy(it -> new JdkProxyAssert(it).matches(proxyInterfaces));

		return this;
	}

	private Stream<JdkProxyHint> jdkProxiesFor(Class<?> entryPoint) {

		return this.actual.getRuntimeHints().proxies().jdkProxies()
				.filter(jdkProxyHint -> jdkProxyHint.getProxiedInterfaces().get(0).getCanonicalName()
						.equals(entryPoint.getCanonicalName()));
	}

	public CodeContributionAssert contributesClassProxy(Class<?>... proxyInterfaces) {

		assertThat(classProxiesFor(proxyInterfaces[0]))
				.describedAs("Unable to find JDK proxy matching [%s]", Arrays.asList(proxyInterfaces))
				.anySatisfy(it -> new ClassProxyAssert(it).matches(proxyInterfaces));

		return this;
	}

	private Stream<ClassProxyHint> classProxiesFor(Class<?> entryPoint) {

		return this.actual.getRuntimeHints().proxies().classProxies()
				.filter(jdkProxyHint -> jdkProxyHint.getProxiedInterfaces().get(0).getCanonicalName()
						.equals(entryPoint.getCanonicalName()));
	}
}
