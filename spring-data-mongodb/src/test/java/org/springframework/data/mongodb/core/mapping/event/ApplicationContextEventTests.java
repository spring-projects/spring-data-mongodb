/*
 * Copyright (c) 2011-2015 by the original author(s).
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

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.core.Is.is;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Integration test for Mapping Events.
 * 
 * @author Mark Pollack
 * @author Christoph Strobl
 * @author Jordi Llach
 */
public class ApplicationContextEventTests {

	private static final String COLLECTION_NAME         = "personPojoStringId";
    private static final String ROOT_COLLECTION_NAME    = "root";
    private static final String RELATED_COLLECTION_NAME = "related";

	private final String[] collectionsToDrop = new String[] { COLLECTION_NAME, ROOT_COLLECTION_NAME, RELATED_COLLECTION_NAME };

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

		assertEquals(COLLECTION_NAME, simpleMappingEventListener.onBeforeSaveEvents.get(0).getCollectionName());
		assertEquals(COLLECTION_NAME, simpleMappingEventListener.onAfterSaveEvents.get(0).getCollectionName());

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

	/**
	 * @see DATAMONGO-1256
	 */
	@Test
	public void loadAndConvertEvents() {

		SimpleMappingEventListener simpleMappingEventListener = applicationContext
				.getBean(SimpleMappingEventListener.class);

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

		SimpleMappingEventListener simpleMappingEventListener = applicationContext
				.getBean(SimpleMappingEventListener.class);

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

		SimpleMappingEventListener simpleMappingEventListener = applicationContext
				.getBean(SimpleMappingEventListener.class);

		PersonPojoStringId entity = new PersonPojoStringId("1", "Text");
		template.insert(entity);

		template.remove(entity);

		assertThat(simpleMappingEventListener.onBeforeDeleteEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onBeforeDeleteEvents.get(0).getCollectionName(), is(COLLECTION_NAME));

		assertThat(simpleMappingEventListener.onAfterDeleteEvents.size(), is(1));
		assertThat(simpleMappingEventListener.onAfterDeleteEvents.get(0).getCollectionName(), is(COLLECTION_NAME));
	}
    
    /**
     * DATAMONGO-1271 DATAMONGO-1287
     */
    @Test
    public void loadAndConvertEventsInInnerSimpleDBRef () throws Exception {
        ParentMappingEventListener simpleMappingEventListener = applicationContext.getBean(ParentMappingEventListener.class);
        Related embed = new Related(1L, "embed desc");
        Related ref1  = new Related(2L, "related desc1");
        Related ref2  = new Related(3L, "related desc2");
        template.insert(embed);
        template.insert(ref1);
        template.insert(ref2);

        Root root = new Root(1L, embed, ref1, ref2, null, null, null, null);
        template.insert(root);
        
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(0));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(0));

        // initially fetching ROOT document and also eagerly fetching 1 DBRef
        Root rootR = template.findOne(query(where("id").is(root.getId())), Root.class);
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(2));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(2));
            
            // checking that no event is fired because those documents were previously eagerly fetched 
            rootR.getRef().getDescription();
            rootR.getEmbed().getDescription();
            assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(2));
            assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(2));
            //  checking that accessing lazy DBRef fires 1 more event of each type
            rootR.getLazyRef().getDescription();
            assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(3));
            assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(3));
            
        // checking collectionNames fired
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(), is(ROOT_COLLECTION_NAME));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(), is(ROOT_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(), is(RELATED_COLLECTION_NAME));
    }
    
    /**
     * DATAMONGO-1271 DATAMONGO-1287
     */
    @Test
    public void loadAndConvertEventsInInnerListDBRef() throws Exception {
        ParentMappingEventListener simpleMappingEventListener = applicationContext.getBean(ParentMappingEventListener.class);
        Related embed = new Related(1L, "embed desc");
        Related ref1  = new Related(2L, "related desc1");
        Related ref2  = new Related(3L, "related desc2");
        template.insert(embed);
        template.insert(ref1);
        template.insert(ref2);
        
        Root root = new Root(1L, embed, null, null, Arrays.asList(ref1, ref2), Arrays.asList(ref1, ref2), null, null);
        template.insert(root);
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(0));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(0));

        // initially fetching ROOT document and also eagerly fetching 2 DBRef
        Root rootR = template.findOne(query(where("id").is(root.getId())), Root.class);
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(3));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(3));
            
            // checking that no event is fired because those documents were previously eagerly fetched
            rootR.getListRef().get(0).getDescription();
            rootR.getListRef().get(1).getDescription();
            assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(3));
            assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(3));
        
            // fetching lazily dbref
            rootR.getListLazy().get(0).getDescription();
            rootR.getListLazy().get(1).getDescription();
            assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(5));
            assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(5));

        // checking collectionNames fired        
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(), is(ROOT_COLLECTION_NAME));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(), is(ROOT_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(3).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(3).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(4).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(4).getCollectionName(), is(RELATED_COLLECTION_NAME));
    }
    
    /**
     * DATAMONGO-1271 DATAMONGO-1287
     */
    @Test
    public void loadAndConvertEventsInInnerMapDBRef() throws Exception {
        ParentMappingEventListener simpleMappingEventListener = applicationContext.getBean(ParentMappingEventListener.class);
        Related embed = new Related(1L, "embed desc");
        Related ref1  = new Related(2L, "related desc1");
        Related ref2  = new Related(3L, "related desc2");
        template.insert(embed);
        template.insert(ref1);
        template.insert(ref2);
        
        Map<String,Related>  mapRef   = new HashMap();
        mapRef.put("1", ref1);
        mapRef.put("2", ref2);
        Map<String,Related>  mapLazy  = new HashMap();
        mapLazy.put("1", ref1);
        mapLazy.put("2", ref2);

        Root root = new Root(1L, embed, null, null, null, null, mapRef, mapLazy);
        template.insert(root);
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(0));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(0));

        // initially fetching ROOT document and also eagerly fetching 2 DBRef (eager map)
        Root rootR = template.findOne(query(where("id").is(root.getId())), Root.class);
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(3));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(3));
        
        // checking that accessing eagerly fetched map does not fire any new event
        Assert.assertEquals(0, rootR.getMapRef().keySet().stream().filter(key -> rootR.getMapRef().get(key).getDescription() == null).count());
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(3));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(3));
        // accessing lazy map of dbref
        Assert.assertEquals(0, rootR.getMapLazy().keySet().stream().filter(key -> rootR.getMapLazy().get(key).getDescription() == null).count());
        assertThat(simpleMappingEventListener.onAfterLoadEvents.size(), is(5));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.size(), is(5));
        
        // checking collectionNames fired        
		assertThat(simpleMappingEventListener.onAfterLoadEvents.get(0).getCollectionName(), is(ROOT_COLLECTION_NAME));
		assertThat(simpleMappingEventListener.onAfterConvertEvents.get(0).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(1).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(1).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(2).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(2).getCollectionName(), is(ROOT_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(3).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(3).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterLoadEvents.get(4).getCollectionName(), is(RELATED_COLLECTION_NAME));
        assertThat(simpleMappingEventListener.onAfterConvertEvents.get(4).getCollectionName(), is(RELATED_COLLECTION_NAME));
    }

	private void comparePersonAndDbo(PersonPojoStringId p, PersonPojoStringId p2, DBObject dbo) {
		assertEquals(p.getId(), p2.getId());
		assertEquals(p.getText(), p2.getText());

		assertEquals("org.springframework.data.mongodb.core.mapping.PersonPojoStringId", dbo.get("_class"));
		assertEquals("1", dbo.get("_id"));
		assertEquals("Text", dbo.get("text"));
	}
}
