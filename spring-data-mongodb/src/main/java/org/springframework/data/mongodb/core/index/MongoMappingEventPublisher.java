/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.util.Assert;

/**
 * An implementation of ApplicationEventPublisher that will only fire {@link MappingContextEvent}s for use by the index
 * creator when MongoTemplate is used 'stand-alone', that is not declared inside a Spring {@link ApplicationContext}.
 * Declare {@link MongoTemplate} inside an {@link ApplicationContext} to enable the publishing of all persistence events
 * such as {@link AfterLoadEvent}, {@link AfterSaveEvent}, etc.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class MongoMappingEventPublisher implements ApplicationEventPublisher {

	private final ApplicationListener<MappingContextEvent<?, ?>> indexCreator;

	/**
	 * Creates a new {@link MongoMappingEventPublisher} for the given {@link ApplicationListener}.
	 *
	 * @param indexCreator must not be {@literal null}.
	 * @since 2.1
	 */
	public MongoMappingEventPublisher(ApplicationListener<MappingContextEvent<?, ?>> indexCreator) {

		Assert.notNull(indexCreator, "ApplicationListener must not be null");

		this.indexCreator = indexCreator;
	}

	/**
	 * Creates a new {@link MongoMappingEventPublisher} for the given {@link MongoPersistentEntityIndexCreator}.
	 *
	 * @param indexCreator must not be {@literal null}.
	 */
	public MongoMappingEventPublisher(MongoPersistentEntityIndexCreator indexCreator) {

		Assert.notNull(indexCreator, "MongoPersistentEntityIndexCreator must not be null");

		this.indexCreator = indexCreator;
	}

	@SuppressWarnings("unchecked")
	public void publishEvent(ApplicationEvent event) {
		if (event instanceof MappingContextEvent<?,?> mappingContextEvent) {
			indexCreator.onApplicationEvent(mappingContextEvent);
		}
	}

	public void publishEvent(Object event) {}
}
