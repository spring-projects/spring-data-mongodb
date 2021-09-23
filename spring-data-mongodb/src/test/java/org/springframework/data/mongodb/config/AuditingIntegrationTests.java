/*
 * Copyright 2012-2021 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;

import org.junit.jupiter.api.Test;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;

/**
 * Integration test for the auditing support.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class AuditingIntegrationTests {

	@Test // DATAMONGO-577, DATAMONGO-800, DATAMONGO-883, DATAMONGO-2261
	public void enablesAuditingAndSetsPropertiesAccordingly() throws Exception {

		AbstractApplicationContext context = new ClassPathXmlApplicationContext("auditing.xml", getClass());

		MongoMappingContext mappingContext = context.getBean(MongoMappingContext.class);
		mappingContext.getPersistentEntity(Entity.class);

		EntityCallbacks callbacks = EntityCallbacks.create(context);

		Entity entity = new Entity();
		entity = callbacks.callback(BeforeConvertCallback.class, entity, "collection-1");

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isEqualTo(entity.created);

		Thread.sleep(10);
		entity.id = 1L;

		entity = callbacks.callback(BeforeConvertCallback.class, entity, "collection-1");

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isNotEqualTo(entity.created);
		context.close();
	}

	class Entity {

		@Id Long id;
		@CreatedDate Date created;
		Date modified;

		@LastModifiedDate
		public Date getModified() {
			return modified;
		}
	}
}
