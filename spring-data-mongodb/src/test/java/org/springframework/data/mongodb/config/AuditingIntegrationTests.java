/*
 * Copyright 2012-2025 the original author or authors.
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

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test for the auditing support.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@ContextConfiguration(locations = "auditing.xml")
@ExtendWith(SpringExtension.class)
class AuditingIntegrationTests {

	@Autowired MongoMappingContext mappingContext;
	@Autowired ApplicationContext context;
	@Autowired MongoTemplate template;

	@Test // DATAMONGO-577, DATAMONGO-800, DATAMONGO-883, DATAMONGO-2261
	void enablesAuditingAndSetsPropertiesAccordingly() throws Exception {

		mappingContext.getPersistentEntity(Entity.class);

		EntityCallbacks callbacks = EntityCallbacks.create(context);

		Entity entity = new Entity();
		entity = callbacks.callback(BeforeConvertCallback.class, entity, "collection-1");

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isEqualTo(entity.created);

		Thread.sleep(10);
		entity.id = "foo";

		entity = callbacks.callback(BeforeConvertCallback.class, entity, "collection-1");

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isNotEqualTo(entity.created);
	}

	@Test // GH-5031
	void handlesDocumentReferenceAuditingCorrectly() throws InterruptedException {

		template.remove(TopDocumentReferenceLevelEntity.class).all();

		Entity entity = new Entity();
		template.insert(entity);

		TopDocumentReferenceLevelEntity tle = new TopDocumentReferenceLevelEntity();
		tle.entity = entity;
		template.insert(tle);

		Thread.sleep(200);

		TopDocumentReferenceLevelEntity loadAndModify = template.findById(tle.id, TopDocumentReferenceLevelEntity.class);
		Date created = loadAndModify.entity.getCreated();
		Date modified = loadAndModify.entity.getModified();
		template.save(loadAndModify.entity);

		TopDocumentReferenceLevelEntity loaded = template.findById(tle.id, TopDocumentReferenceLevelEntity.class);

		assertThat(loaded.entity.getCreated()).isEqualTo(created);
		assertThat(loaded.entity.getModified()).isNotEqualTo(modified);
	}

	@Test // GH-5031
	void handlesDbRefAuditingCorrectly() throws InterruptedException {

		template.remove(TopDbRefLevelEntity.class).all();

		Entity entity = new Entity();
		template.insert(entity);

		TopDbRefLevelEntity tle = new TopDbRefLevelEntity();
		tle.entity = entity;
		template.insert(tle);

		Thread.sleep(200);

		TopDbRefLevelEntity loadAndModify = template.findById(tle.id, TopDbRefLevelEntity.class);
		Date created = loadAndModify.entity.getCreated();
		Date modified = loadAndModify.entity.getModified();
		template.save(loadAndModify.entity);

		TopDbRefLevelEntity loaded = template.findById(tle.id, TopDbRefLevelEntity.class);

		assertThat(loaded.entity.getCreated()).isEqualTo(created);
		assertThat(loaded.entity.getModified()).isNotEqualTo(modified);
	}

	static class Entity {

		@Id String id;
		@CreatedDate Date created;
		Date modified;

		@LastModifiedDate
		public Date getModified() {
			return modified;
		}

		public Date getCreated() {
			return created;
		}

		public void setCreated(Date created) {
			this.created = created;
		}

		public void setModified(Date modified) {
			this.modified = modified;
		}
	}

	static class TopDocumentReferenceLevelEntity {

		@Id ObjectId id;
		@DocumentReference(lazy = true) Entity entity;

	}

	static class TopDbRefLevelEntity {

		@Id ObjectId id;
		@DBRef(lazy = true) Entity entity;

	}

}
