/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.QuerydslMongoPredicateExecutor;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.annotation.DirtiesContext;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Integration test for Mapping Events.
 *
 * @author Mark Pollack
 * @author Christoph Strobl
 * @author Jordi Llach
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class })
public class ApplicationContextEventTests {

	private static final String COLLECTION_NAME = "personPojoStringId";
	private static final String ROOT_COLLECTION_NAME = "root";
	private static final String RELATED_COLLECTION_NAME = "related";

	private final String[] collectionsToDrop = new String[] { COLLECTION_NAME, ROOT_COLLECTION_NAME,
			RELATED_COLLECTION_NAME };

	static @Client MongoClient mongoClient;

	private ConfigurableApplicationContext applicationContext;
	private MongoTemplate template;
	private SimpleMappingEventListener listener;

	@BeforeEach
	public void setUp() {

		cleanDb();

		applicationContext = new AnnotationConfigApplicationContext(ApplicationContextEventTestsAppConfig.class);
		template = applicationContext.getBean(MongoTemplate.class);
		template.setWriteConcern(WriteConcern.JOURNALED);
		listener = applicationContext.getBean(SimpleMappingEventListener.class);
	}

	@AfterEach
	public void cleanUp() {

		cleanDb();
		applicationContext.close();
	}

	private void cleanDb() {

		MongoDatabase db = mongoClient.getDatabase("database");
		for (String coll : collectionsToDrop) {
			db.getCollection(coll).drop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void beforeSaveEvent() {

		PersonBeforeSaveListener personBeforeSaveListener = applicationContext.getBean(PersonBeforeSaveListener.class);
		AfterSaveListener afterSaveListener = applicationContext.getBean(AfterSaveListener.class);

		assertThat(personBeforeSaveListener.seenEvents).isEmpty();
		assertThat(afterSaveListener.seenEvents).isEmpty();

		assertThat(listener.onBeforeSaveEvents).isEmpty();
		assertThat(listener.onAfterSaveEvents).isEmpty();

		PersonPojoStringId p = new PersonPojoStringId("1", "Text");
		template.insert(p);

		assertThat(personBeforeSaveListener.seenEvents).hasSize(1);
		assertThat(afterSaveListener.seenEvents).hasSize(1);

		assertThat(listener.onBeforeSaveEvents).hasSize(1);
		assertThat(listener.onAfterSaveEvents).hasSize(1);

		assertThat(listener.onBeforeSaveEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);
		assertThat(listener.onAfterSaveEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(personBeforeSaveListener.seenEvents.get(0) instanceof BeforeSaveEvent<?>).isTrue();
		assertThat(afterSaveListener.seenEvents.get(0) instanceof AfterSaveEvent<?>).isTrue();

		BeforeSaveEvent<PersonPojoStringId> beforeSaveEvent = (BeforeSaveEvent<PersonPojoStringId>) personBeforeSaveListener.seenEvents
				.get(0);
		PersonPojoStringId p2 = beforeSaveEvent.getSource();
		org.bson.Document document = beforeSaveEvent.getDocument();

		comparePersonAndDocument(p, p2, document);

		AfterSaveEvent<Object> afterSaveEvent = (AfterSaveEvent<Object>) afterSaveListener.seenEvents.get(0);
		assertThat(afterSaveEvent.getSource() instanceof PersonPojoStringId).isTrue();
		p2 = (PersonPojoStringId) afterSaveEvent.getSource();
		document = beforeSaveEvent.getDocument();

		comparePersonAndDocument(p, p2, document);
	}

	@Test // DATAMONGO-1256
	public void loadAndConvertEvents() {

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.findOne(query(where("id").is(entity.getId())), PersonPojoStringId.class);

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onBeforeConvertEvents).hasSize(1);
		assertThat(listener.onBeforeConvertEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);
	}

	@Test // GH-4107
	@DirtiesContext
	public void configurationShouldDisableLifecycleEvents() {

		template.setEntityLifecycleEventsEnabled(false);

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.findOne(query(where("id").is(entity.getId())), PersonPojoStringId.class);

		assertThat(listener.onAfterLoadEvents).isEmpty();
		assertThat(listener.onBeforeConvertEvents).isEmpty();
		assertThat(listener.onAfterConvertEvents).isEmpty();
	}

	@Test // DATAMONGO-1256
	public void loadEventsOnAggregation() {

		template.insert(new PersonPojoStringId("1", "Text"));

		template.aggregate(Aggregation.newAggregation(Aggregation.project("text")), PersonPojoStringId.class,
				PersonPojoStringId.class);

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onBeforeConvertEvents).hasSize(1);
		assertThat(listener.onBeforeConvertEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);
	}

	@Test // DATAMONGO-1256
	public void deleteEvents() {

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.remove(entity);

		assertThat(listener.onBeforeDeleteEvents).hasSize(1);
		assertThat(listener.onBeforeDeleteEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onAfterDeleteEvents).hasSize(1);
		assertThat(listener.onAfterDeleteEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);
	}

	@Test // DATAMONGO-1271
	public void publishesAfterLoadAndAfterConvertEventsForDBRef() {

		Related ref1 = new Related(2L, "related desc1");

		template.insert(ref1);

		Root source = new Root();
		source.id = 1L;
		source.reference = ref1;

		template.insert(source);

		template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(listener.onAfterLoadEvents).hasSize(2);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(2);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(1).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
	}

	@Test // DATAMONGO-1271
	public void publishesAfterLoadAndAfterConvertEventsForLazyLoadingDBRef() {

		Related ref1 = new Related(2L, "related desc1");

		template.insert(ref1);

		Root source = new Root();
		source.id = 1L;
		source.lazyReference = ref1;

		template.insert(source);

		Root target = template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);

		target.getLazyReference().getDescription();

		assertThat(listener.onAfterLoadEvents).hasSize(2);
		assertThat(listener.onAfterLoadEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(2);
		assertThat(listener.onAfterConvertEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
	}

	@Test // DATAMONGO-1271
	public void publishesAfterLoadAndAfterConvertEventsForListOfDBRef() {

		List<Related> references = Arrays.asList(new Related(20L, "ref 1"), new Related(30L, "ref 2"));

		template.insert(references, Related.class);

		Root source = new Root();
		source.id = 1L;
		source.listOfReferences = references;

		template.insert(source);

		template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(listener.onAfterLoadEvents).hasSize(3);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(2).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(3);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(2).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
	}

	@Test // DATAMONGO-1271
	public void publishesAfterLoadAndAfterConvertEventsForLazyLoadingListOfDBRef() {

		List<Related> references = Arrays.asList(new Related(20L, "ref 1"), new Related(30L, "ref 2"));

		template.insert(references, Related.class);

		Root source = new Root();
		source.id = 1L;
		source.lazyListOfReferences = references;

		template.insert(source);

		Root target = template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);

		target.getLazyListOfReferences().size();

		assertThat(listener.onAfterLoadEvents).hasSize(3);
		assertThat(listener.onAfterConvertEvents).hasSize(3);

		assertThat(listener.onAfterLoadEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(2).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(2).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
	}

	@Test // DATAMONGO-1271
	public void publishesAfterLoadAndAfterConvertEventsForMapOfDBRef() {

		Map<String, Related> references = new LinkedHashMap<String, Related>();
		references.put("ref-1", new Related(20L, "ref 1"));
		references.put("ref-2", new Related(30L, "ref 2"));

		template.insert(references.values(), Related.class);

		Root source = new Root();
		source.id = 1L;
		source.mapOfReferences = references;

		template.insert(source);

		template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(listener.onAfterLoadEvents).hasSize(3);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(2).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(3);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(2).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);
	}

	@Test // DATAMONGO-1271
	public void publishesAfterLoadAndAfterConvertEventsForLazyLoadingMapOfDBRef() {

		Map<String, Related> references = new LinkedHashMap<String, Related>();
		references.put("ref-1", new Related(20L, "ref 1"));
		references.put("ref-2", new Related(30L, "ref 2"));

		template.insert(references.values(), Related.class);

		Root source = new Root();
		source.id = 1L;
		source.lazyMapOfReferences = references;

		template.insert(source);

		Root target = template.findOne(query(where("id").is(source.getId())), Root.class);

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(ROOT_COLLECTION_NAME);

		target.getLazyMapOfReferences().size();

		assertThat(listener.onAfterLoadEvents).hasSize(3);
		assertThat(listener.onAfterLoadEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterLoadEvents.get(2).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(3);
		assertThat(listener.onAfterConvertEvents.get(1).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
		assertThat(listener.onAfterConvertEvents.get(2).getCollectionName()).isEqualTo(RELATED_COLLECTION_NAME);
	}

	@Test // DATAMONGO-1823
	public void publishesAfterConvertEventForFindQueriesUsingProjections() {

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.query(PersonPojoStringId.class).matching(query(where("id").is(entity.getId()))).all();

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onBeforeConvertEvents).hasSize(1);
		assertThat(listener.onBeforeConvertEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);

		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo(COLLECTION_NAME);
	}

	@Test // DATAMONGO-700, DATAMONGO-1185, DATAMONGO-1848
	public void publishesEventsForQuerydslFindQueries() {

		template.dropCollection(Person.class);

		template.save(new Person("Boba", "Fett", 40));

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		QuerydslMongoPredicateExecutor executor = new QuerydslMongoPredicateExecutor<>(entityInformation, template);

		executor.findOne(QPerson.person.lastname.startsWith("Fe"));

		assertThat(listener.onAfterLoadEvents).hasSize(1);
		assertThat(listener.onAfterLoadEvents.get(0).getCollectionName()).isEqualTo("person");

		assertThat(listener.onBeforeConvertEvents).hasSize(1);
		assertThat(listener.onBeforeConvertEvents.get(0).getCollectionName()).isEqualTo("person");

		assertThat(listener.onAfterConvertEvents).hasSize(1);
		assertThat(listener.onAfterConvertEvents.get(0).getCollectionName()).isEqualTo("person");
	}

	private void comparePersonAndDocument(PersonPojoStringId p, PersonPojoStringId p2, org.bson.Document document) {

		assertThat(p2.getId()).isEqualTo(p.getId());
		assertThat(p2.getText()).isEqualTo(p.getText());

		assertThat(document.get("_id")).isEqualTo("1");
		assertThat(document.get("text")).isEqualTo("Text");
		assertTypeHint(document, PersonPojoStringId.class);
	}

	@org.springframework.data.mongodb.core.mapping.Document
	public static class Root {

		@Id Long id;

		@DBRef Related reference;
		@DBRef(lazy = true) Related lazyReference;

		@DBRef List<Related> listOfReferences;
		@DBRef(lazy = true) List<Related> lazyListOfReferences;

		@DBRef Map<String, Related> mapOfReferences;
		@DBRef(lazy = true) Map<String, Related> lazyMapOfReferences;

		public Long getId() {
			return this.id;
		}

		public Related getReference() {
			return this.reference;
		}

		public Related getLazyReference() {
			return this.lazyReference;
		}

		public List<Related> getListOfReferences() {
			return this.listOfReferences;
		}

		public List<Related> getLazyListOfReferences() {
			return this.lazyListOfReferences;
		}

		public Map<String, Related> getMapOfReferences() {
			return this.mapOfReferences;
		}

		public Map<String, Related> getLazyMapOfReferences() {
			return this.lazyMapOfReferences;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setReference(Related reference) {
			this.reference = reference;
		}

		public void setLazyReference(Related lazyReference) {
			this.lazyReference = lazyReference;
		}

		public void setListOfReferences(List<Related> listOfReferences) {
			this.listOfReferences = listOfReferences;
		}

		public void setLazyListOfReferences(List<Related> lazyListOfReferences) {
			this.lazyListOfReferences = lazyListOfReferences;
		}

		public void setMapOfReferences(Map<String, Related> mapOfReferences) {
			this.mapOfReferences = mapOfReferences;
		}

		public void setLazyMapOfReferences(Map<String, Related> lazyMapOfReferences) {
			this.lazyMapOfReferences = lazyMapOfReferences;
		}

		public String toString() {
			return "ApplicationContextEventTests.Root(id=" + this.getId() + ", reference=" + this.getReference()
					+ ", lazyReference=" + this.getLazyReference() + ", listOfReferences=" + this.getListOfReferences()
					+ ", lazyListOfReferences=" + this.getLazyListOfReferences() + ", mapOfReferences="
					+ this.getMapOfReferences() + ", lazyMapOfReferences=" + this.getLazyMapOfReferences() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document
	public static class Related {

		@Id Long id;
		String description;

		public Related(Long id, String description) {
			this.id = id;
			this.description = description;
		}

		public Long getId() {
			return this.id;
		}

		public String getDescription() {
			return this.description;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public String toString() {
			return "ApplicationContextEventTests.Related(id=" + this.getId() + ", description=" + this.getDescription() + ")";
		}
	}
}
