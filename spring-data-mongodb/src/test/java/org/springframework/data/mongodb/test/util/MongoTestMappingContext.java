/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.MethodInvocationRecorder;

/**
 * @author Christoph Strobl
 */
public class MongoTestMappingContext extends MongoMappingContext {

	private MappingContextConfigurer contextConfigurer;
	private MongoConverterConfigurer converterConfigurer;

	public static MongoTestMappingContext newTestContext() {
		return new MongoTestMappingContext(conig -> {}).init();
	}

	public MongoTestMappingContext(MappingContextConfigurer contextConfig) {

		this.contextConfigurer = contextConfig;
		this.converterConfigurer = new MongoConverterConfigurer();
	}

	public MongoTestMappingContext(Consumer<MappingContextConfigurer> contextConfig) {

		this(new MappingContextConfigurer());
		contextConfig.accept(contextConfigurer);
	}

	public <T> MongoPersistentProperty getPersistentPropertyFor(Class<T> type, Function<T, ?> property) {

		MongoPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(type);
		return persistentEntity.getPersistentProperty(MethodInvocationRecorder.forProxyOf(type).record(property).getPropertyPath().get());
	}

	public MongoTestMappingContext customConversions(MongoConverterConfigurer converterConfig) {

		this.converterConfigurer = converterConfig;
		return this;
	}

	public MongoTestMappingContext customConversions(Consumer<MongoConverterConfigurer> converterConfig) {

		converterConfig.accept(converterConfigurer);
		return this;
	}

	public MongoTestMappingContext init() {

		setInitialEntitySet(contextConfigurer.initialEntitySet());
		setAutoIndexCreation(contextConfigurer.autocreateIndex);
		if (converterConfigurer.customConversions != null) {
			setSimpleTypeHolder(converterConfigurer.customConversions.getSimpleTypeHolder());
		} else {
			setSimpleTypeHolder(new MongoCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
		}

		super.afterPropertiesSet();
		return this;
	}

	@Override
	public void afterPropertiesSet() {
		init();
	}


}
