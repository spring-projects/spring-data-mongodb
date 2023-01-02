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
package org.springframework.data.mongodb.aot;

import org.springframework.aot.generate.GenerationContext;
import org.springframework.core.ResolvableType;
import org.springframework.data.aot.ManagedTypesBeanRegistrationAotProcessor;
import org.springframework.data.mongodb.MongoManagedTypes;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @since 2022/06
 */
class MongoManagedTypesBeanRegistrationAotProcessor extends ManagedTypesBeanRegistrationAotProcessor {

	private final LazyLoadingProxyAotProcessor lazyLoadingProxyAotProcessor = new LazyLoadingProxyAotProcessor();

	public MongoManagedTypesBeanRegistrationAotProcessor() {
		setModuleIdentifier("mongo");
	}

	@Override
	protected boolean isMatch(@Nullable Class<?> beanType, @Nullable String beanName) {
		return isMongoManagedTypes(beanType) || super.isMatch(beanType, beanName);
	}

	protected boolean isMongoManagedTypes(@Nullable Class<?> beanType) {
		return beanType != null && ClassUtils.isAssignable(MongoManagedTypes.class, beanType);
	}

	@Override
	protected void contributeType(ResolvableType type, GenerationContext generationContext) {

		if (MongoAotPredicates.IS_SIMPLE_TYPE.test(type.toClass())) {
			return;
		}

		super.contributeType(type, generationContext);
		lazyLoadingProxyAotProcessor.registerLazyLoadingProxyIfNeeded(type.toClass(), generationContext);
	}
}
