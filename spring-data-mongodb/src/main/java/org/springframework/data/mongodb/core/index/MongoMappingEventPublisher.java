/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.mongodb.core.index;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mapping.event.MappingContextEvent;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * An implementation of ApplicationEventPublisher that will only fire MappingContextEvents for use by the index creator when
 * MongoTemplate is used 'stand-alone', that is not declared inside a Spring ApplicationContext.
 * 
 * Declare MongoTemplate inside an ApplicationContext to enable the publishing of all persistence events such as 
 * {@link AfterLoadEvent}, {@link AfterSaveEvent}, etc. 
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingEventPublisher implements ApplicationEventPublisher {

	private MongoPersistentEntityIndexCreator indexCreator;

	public MongoMappingEventPublisher(MongoPersistentEntityIndexCreator indexCreator) {
		this.indexCreator = indexCreator;
	}

	@SuppressWarnings("unchecked")
	public void publishEvent(ApplicationEvent event) {
		if (event instanceof MappingContextEvent) {
			indexCreator
					.onApplicationEvent((MappingContextEvent<MongoPersistentEntity<MongoPersistentProperty>, MongoPersistentProperty>) event);
		}
	}
}
