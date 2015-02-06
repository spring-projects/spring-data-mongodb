/*
 * Copyright 2012-2015 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;

/**
 * Integration test for the auditing support.
 * 
 * @author Oliver Gierke
 */
public class AuditingIntegrationTests {

	/**
	 * @see DATAMONGO-577, DATAMONGO-800, DATAMONGO-883
	 */
	@Test
	public void enablesAuditingAndSetsPropertiesAccordingly() throws Exception {

		AbstractApplicationContext context = new ClassPathXmlApplicationContext("auditing.xml", getClass());

		MongoMappingContext mappingContext = context.getBean(MongoMappingContext.class);
		mappingContext.getPersistentEntity(Entity.class);

		Entity entity = new Entity();
		BeforeConvertEvent<Entity> event = new BeforeConvertEvent<Entity>(entity);
		context.publishEvent(event);

		assertThat(entity.created, is(notNullValue()));
		assertThat(entity.modified, is(entity.created));

		Thread.sleep(10);
		entity.id = 1L;
		event = new BeforeConvertEvent<Entity>(entity);
		context.publishEvent(event);

		assertThat(entity.created, is(notNullValue()));
		assertThat(entity.modified, is(not(entity.created)));
		context.close();
	}

	class Entity {

		@Id Long id;
		@CreatedDate DateTime created;
		DateTime modified;

		@LastModifiedDate
		public DateTime getModified() {
			return modified;
		}
	}
}
