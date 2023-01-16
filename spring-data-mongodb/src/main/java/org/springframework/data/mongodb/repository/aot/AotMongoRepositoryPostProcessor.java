/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import org.springframework.aot.generate.GenerationContext;
import org.springframework.data.mongodb.aot.LazyLoadingProxyAotProcessor;
import org.springframework.data.mongodb.aot.MongoAotPredicates;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.RepositoryRegistrationAotProcessor;
import org.springframework.data.util.TypeContributor;
import org.springframework.data.util.TypeUtils;

/**
 * @author Christoph Strobl
 */
public class AotMongoRepositoryPostProcessor extends RepositoryRegistrationAotProcessor {

	private final LazyLoadingProxyAotProcessor lazyLoadingProxyAotProcessor = new LazyLoadingProxyAotProcessor();

	@Override
	protected void contribute(AotRepositoryContext repositoryContext, GenerationContext generationContext) {
		// do some custom type registration here
		super.contribute(repositoryContext, generationContext);

		repositoryContext.getResolvedTypes().stream().filter(MongoAotPredicates.IS_SIMPLE_TYPE.negate()).forEach(type -> {
			TypeContributor.contribute(type, it -> true, generationContext);
			lazyLoadingProxyAotProcessor.registerLazyLoadingProxyIfNeeded(type, generationContext);
		});
	}

	@Override
	protected void contributeType(Class<?> type, GenerationContext generationContext) {

		if (TypeUtils.type(type).isPartOf("org.springframework.data.mongodb", "com.mongodb")) {
			return;
		}
		super.contributeType(type, generationContext);
	}
}
