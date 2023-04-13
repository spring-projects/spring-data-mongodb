/*
 * Copyright 2023 the original author or authors.
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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.ClassNameGenerator;
import org.springframework.aot.generate.DefaultGenerationContext;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.javapoet.ClassName;

/**
 * Unit tests for {@link LazyLoadingProxyAotProcessor}.
 *
 * @author Christoph Strobl
 */
class LazyLoadingProxyAotProcessorUnitTests {

	@Test // GH-4351
	void registersProxyForLazyDbRefCorrectlyWhenTypeIsCollectionInterface() {

		GenerationContext ctx = new DefaultGenerationContext(new ClassNameGenerator(ClassName.get(this.getClass())),
				new InMemoryGeneratedFiles());

		new LazyLoadingProxyAotProcessor().registerLazyLoadingProxyIfNeeded(A.class, ctx);

		assertThat(ctx.getRuntimeHints())
				.satisfies(RuntimeHintsPredicates.proxies().forInterfaces(java.util.Collection.class,
						org.springframework.data.mongodb.core.convert.LazyLoadingProxy.class, java.util.List.class,
						org.springframework.aop.SpringProxy.class, org.springframework.aop.framework.Advised.class,
						org.springframework.core.DecoratingProxy.class)::test);
	}

	static class A {

		String id;

		@DBRef(lazy = true) //
		List<B> listRef;
	}

	static class B {
		String id;
	}
}
