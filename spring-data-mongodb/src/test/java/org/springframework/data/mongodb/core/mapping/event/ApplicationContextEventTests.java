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
package org.springframework.data.mongodb.core.mapping.event;

import static org.junit.Assert.*;

import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * Integration test for Mapping Events.
 * 
 * @author Mark Pollack
 */
public class ApplicationContextEventTests {

	private final String[] collectionsToDrop = new String[] { "personPojoStringId" };

	private ApplicationContext applicationContext;
	private MongoTemplate template;

	@Before
	public void setUp() throws Exception {
		cleanDb();
		applicationContext = new AnnotationConfigApplicationContext(ApplicationContextEventTestsAppConfig.class);
		template = applicationContext.getBean(MongoTemplate.class);
		template.setWriteConcern(WriteConcern.FSYNC_SAFE);
	}

	@After
	public void cleanUp() throws Exception {
		cleanDb();
	}

	private void cleanDb() throws UnknownHostException {
		Mongo mongo = new MongoClient();
		DB db = mongo.getDB("database");
		for (String coll : collectionsToDrop) {
			db.getCollection(coll).drop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void beforeSaveEvent() {
		PersonBeforeSaveListener personBeforeSaveListener = applicationContext.getBean(PersonBeforeSaveListener.class);
		AfterSaveListener afterSaveListener = applicationContext.getBean(AfterSaveListener.class);
		SimpleMappingEventListener simpleMappingEventListener = applicationContext
				.getBean(SimpleMappingEventListener.class);

		assertEquals(0, personBeforeSaveListener.seenEvents.size());
		assertEquals(0, afterSaveListener.seenEvents.size());

		assertEquals(0, simpleMappingEventListener.onBeforeSaveEvents.size());
		assertEquals(0, simpleMappingEventListener.onAfterSaveEvents.size());

		PersonPojoStringId p = new PersonPojoStringId("1", "Text");
		template.insert(p);

		assertEquals(1, personBeforeSaveListener.seenEvents.size());
		assertEquals(1, afterSaveListener.seenEvents.size());

		assertEquals(1, simpleMappingEventListener.onBeforeSaveEvents.size());
		assertEquals(1, simpleMappingEventListener.onAfterSaveEvents.size());

		Assert.assertTrue(personBeforeSaveListener.seenEvents.get(0) instanceof BeforeSaveEvent<?>);
		Assert.assertTrue(afterSaveListener.seenEvents.get(0) instanceof AfterSaveEvent<?>);

		BeforeSaveEvent<PersonPojoStringId> beforeSaveEvent = (BeforeSaveEvent<PersonPojoStringId>) personBeforeSaveListener.seenEvents
				.get(0);
		PersonPojoStringId p2 = beforeSaveEvent.getSource();
		DBObject dbo = beforeSaveEvent.getDBObject();

		comparePersonAndDbo(p, p2, dbo);

		AfterSaveEvent<Object> afterSaveEvent = (AfterSaveEvent<Object>) afterSaveListener.seenEvents.get(0);
		Assert.assertTrue(afterSaveEvent.getSource() instanceof PersonPojoStringId);
		p2 = (PersonPojoStringId) afterSaveEvent.getSource();
		dbo = beforeSaveEvent.getDBObject();

		comparePersonAndDbo(p, p2, dbo);

	}

	private void comparePersonAndDbo(PersonPojoStringId p, PersonPojoStringId p2, DBObject dbo) {
		assertEquals(p.getId(), p2.getId());
		assertEquals(p.getText(), p2.getText());

		assertEquals("org.springframework.data.mongodb.core.mapping.PersonPojoStringId", dbo.get("_class"));
		assertEquals("1", dbo.get("_id"));
		assertEquals("Text", dbo.get("text"));
	}
}
