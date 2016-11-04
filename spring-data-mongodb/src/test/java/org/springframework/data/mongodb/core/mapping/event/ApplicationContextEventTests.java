/*
 * Copyright (c) 2011-2016 by the original author(s).
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

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

import lombok.Data;

/**
 * Integration test for Mapping Events.
 * 
 * @author Mark Pollack
 * @author Christoph Strobl
 * @author Jordi Llach
 */
public class ApplicationContextEventTests {

	private static final String COLLECTION_NAME = "personPojoStringId";
	private static final String ROOT_COLLECTION_NAME = "root";
	private static final String RELATED_COLLECTION_NAME = "related";

	private final String[] collectionsToDrop = new String[] { COLLECTION_NAME, ROOT_COLLECTION_NAME,
			RELATED_COLLECTION_NAME };

	private ApplicationContext applicationContext;
	private MongoTemplate template;
	private SimpleMappingEventListener simpleMappingEventListener;

	@Before
	public void setUp() throws Exception {
		cleanDb();
		applicationContext = new AnnotationConfigApplicationContext(ApplicationContextEventTestsAppConfig.class);
		template = applicationContext.getBean(MongoTemplate.class);
		template.setWriteConcern(WriteConcern.FSYNC_SAFE);
		simpleMappingEventListener = applicationContext.getBean(SimpleMappingEventListener.class);
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

		assertEquals(COLLECTION_NAME, simpleMappingEventListener.onBeforeSaveEvents.get(0).getCollectionName());
		assertEquals(COLLECTION_NAME, simpleMappingEventListener.onAfterSaveEvents.get(0).getCollectionName());

		Assert.assertTrue(personBeforeSaveListener.seenEvents.get(0) instanceof BeforeSaveEvent<?>);
		Assert.assertTrue(afterSaveListener.seenEvents.get(0) instanceof AfterSaveEvent<?>);

		BeforeSaveEvent<PersonPojoStringId> beforeSaveEvent = (BeforeSaveEvent<PersonPojoStringId>) personBeforeSaveListener.seenEvents
				.get(0);
		PersonPojoStringId p2 = beforeSaveEvent.getSource();
		org.bson.Document document = beforeSaveEvent.getDocument();

		comparePersonAndDocument(p, p2, document);

		AfterSaveEvent<Object> afterSaveEvent = (AfterSaveEvent<Object>) afterSaveListener.seenEvents.get(0);
		Assert.assertTrue(afterSaveEvent.getSource() instanceof PersonPojoStringId);
		p2 = (PersonPojoStringId) afterSaveEvent.getSource();
		document = beforeSaveEvent.getDocument();

		comparePersonAndDocument(p, p2, document);
	}

	/**
	 * @see DATAMONGO-1256
	 */
	@Test
	public void loadAndConvertEvents() {

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.findOne(query(where("id").is(entity.getId())), PersonPojoStringId.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(), is(COLLECTION_NAME));

		assertThat(simpleMappingEventListener.onBeforeConvertEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onBeforeConvertEvents.get(0).getCollectionName(), is(COLLECTION_NAME));

		assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(), is(COLLECTION_NAME));
	}

	/**
	 * @see DATAMONGO-1256
	 */
	@Test
	public void loadEventsOnAggregation() {

		template.insert(new PersonPojoStringId("1", "Text"));

		template.aggregate(Aggregation.newAggregation(Aggregation.project("text")), PersonPojoStringId.class,
				PersonPojoStringId.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(), is(COLLECTION_NAME));

		assertThat(simpleMappingEventListener.onBeforeConvertEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onBeforeConvertEvents.get(0).getCollectionName(), is(COLLECTION_NAME));

		assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(), is(COLLECTION_NAME));
	}

	/**
	 * @see DATAMONGO-1256
	 */
	@Test
	public void deleteEvents() {

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.remove(entity);

		assertThat(simpleMappingEventListener.onBeforeDeleteEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onBeforeDeleteEvents.get(0).getCollectionName(), is(COLLECTION_NAME));

		assertThat(simpleMappingEventListener.onAfterDeleteEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onAfterDeleteEvents.get(0).getCollectionName(), is(COLLECTION_NAME));
	}

	/**
	 * @see DATAMONGO-1271
	 */
	@Test
	public void publishesAfterLoadAndAfterConvertEventsForDBRef() throws Exception {

		Related ref1 = new Related(2L, "related desc1");

		template.insert(ref1);

		Root source = new Root();
		source.id = 1L;
		source.reference = ref1;

		template.insert(source);

		template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(2));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(2));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
	}

	/**
	 * @see DATAMONGO-1271
	 */
	@Test
	public void publishesAfterLoadAndAfterConvertEventsForLazyLoadingDBRef() throws Exception {

		Related ref1 = new Related(2L, "related desc1");

		template.insert(ref1);

		Root source = new Root();
		source.id = 1L;
		source.lazyReference = ref1;

		template.insert(source);

		Root target = template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(1));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(1));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));

		target.getLazyReference().getDescription();

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(2));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(2));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
	}

	/**
	 * @see DATAMONGO-1271
	 */
	@Test
	public void publishesAfterLoadAndAfterConvertEventsForListOfDBRef() throws Exception {

		List<Related> references = Arrays.asList(new Related(20L, "ref 1"), new Related(30L, "ref 2"));

		template.insert(references, Related.class);

		Root source = new Root();
		source.id = 1L;
		source.listOfReferences = references;

		template.insert(source);

		template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
	}

	/**
	 * @see DATAMONGO-1271
	 */
	@Test
	public void publishesAfterLoadAndAfterConvertEventsForLazyLoadingListOfDBRef() throws Exception {

		List<Related> references = Arrays.asList(new Related(20L, "ref 1"), new Related(30L, "ref 2"));

		template.insert(references, Related.class);

		Root source = new Root();
		source.id = 1L;
		source.lazyListOfReferences = references;

		template.insert(source);

		Root target = template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(1));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(1));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));

		target.getLazyListOfReferences().size();

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(3));

		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
	}

	/**
	 * @see DATAMONGO-1271
	 */
	@Test
	public void publishesAfterLoadAndAfterConvertEventsForMapOfDBRef() throws Exception {

		Map<String, Related> references = new LinkedHashMap<String, Related>();
		references.put("ref-1", new Related(20L, "ref 1"));
		references.put("ref-2", new Related(30L, "ref 2"));

		template.insert(references.values(), Related.class);

		Root source = new Root();
		source.id = 1L;
		source.mapOfReferences = references;

		template.insert(source);

		template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));
	}

	/**
	 * @see DATAMONGO-1271
	 */
	@Test
	public void publishesAfterLoadAndAfterConvertEventsForLazyLoadingMapOfDBRef() throws Exception {

		Map<String, Related> references = new LinkedHashMap<String, Related>();
		references.put("ref-1", new Related(20L, "ref 1"));
		references.put("ref-2", new Related(30L, "ref 2"));

		template.insert(references.values(), Related.class);

		Root source = new Root();
		source.id = 1L;
		source.lazyMapOfReferences = references;

		template.insert(source);

		Root target = template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(1));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(1));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(),
				is(equalTo(ROOT_COLLECTION_NAME)));

		target.getLazyMapOfReferences().size();

		assertThat(simpleMappingEventListener.onAfterLoadEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));

		assertThat(simpleMappingEventListener.onAfterConvertEvents, hasSize(3));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(),
				is(equalTo(RELATED_COLLECTION_NAME)));
	}

	private void comparePersonAndDocument(PersonPojoStringId p, PersonPojoStringId p2, org.bson.Document document) {

		assertEquals(p.getId(), p2.getId());
		assertEquals(p.getText(), p2.getText());

		assertEquals("org.springframework.data.mongodb.core.mapping.PersonPojoStringId", document.get("_class"));
		assertEquals("1", document.get("_id"));
		assertEquals("Text", document.get("text"));
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document
	public static class Root {

		@Id Long id;

		@DBRef Related reference;
		@DBRef(lazy = true) Related lazyReference;

		@DBRef List<Related> listOfReferences;
		@DBRef(lazy = true) List<Related> lazyListOfReferences;

		@DBRef Map<String, Related> mapOfReferences;
		@DBRef(lazy = true) Map<String, Related> lazyMapOfReferences;
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document
	public static class Related {

		final @Id Long id;
		final String description;
	}
}
