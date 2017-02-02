/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

/**
 * Unit tests for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoTemplateUnitTests extends MongoOperationsUnitTests {

	MongoTemplate template;

	@Mock MongoDbFactory factory;
	@Mock Mongo mongo;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;
	@Mock MongoCursor<Document> cursor;
	@Mock FindIterable<Document> findIterable;

	MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		when(findIterable.iterator()).thenReturn(cursor);
		when(factory.getDb()).thenReturn(db);
		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(db.getCollection(Mockito.any(String.class), eq(Document.class))).thenReturn(collection);
		when(collection.find(Mockito.any(org.bson.Document.class))).thenReturn(findIterable);
		when(findIterable.sort(Mockito.any(org.bson.Document.class))).thenReturn(findIterable);
		when(findIterable.modifiers(Mockito.any(org.bson.Document.class))).thenReturn(findIterable);

		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext);
		this.template = new MongoTemplate(factory, converter);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullDatabaseName() throws Exception {
		new MongoTemplate(mongo, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullMongo() throws Exception {
		new MongoTemplate(null, "database");
	}

	@Test(expected = DataAccessException.class)
	public void removeHandlesMongoExceptionProperly() throws Exception {

		MongoTemplate template = mockOutGetDb();

		template.remove(null, "collection");
	}

	@Test
	public void defaultsConverterToMappingMongoConverter() throws Exception {
		MongoTemplate template = new MongoTemplate(mongo, "database");
		assertTrue(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void rejectsNotFoundMapReduceResource() {

		GenericApplicationContext ctx = new GenericApplicationContext();
		ctx.refresh();
		template.setApplicationContext(ctx);
		template.mapReduce("foo", "classpath:doesNotExist.js", "function() {}", Person.class);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-322
	public void rejectsEntityWithNullIdIfNotSupportedIdType() {

		Object entity = new NotAutogenerateableId();
		template.save(entity);
	}

	@Test // DATAMONGO-322
	public void storesEntityWithSetIdAlthoughNotAutogenerateable() {

		NotAutogenerateableId entity = new NotAutogenerateableId();
		entity.id = 1;

		template.save(entity);
	}

	@Test // DATAMONGO-322
	public void autogeneratesIdForEntityWithAutogeneratableId() {

		this.converter.afterPropertiesSet();

		MongoTemplate template = spy(this.template);
		doReturn(new ObjectId()).when(template).saveDocument(Mockito.any(String.class), Mockito.any(Document.class),
				Mockito.any(Class.class));

		AutogenerateableId entity = new AutogenerateableId();
		template.save(entity);

		assertThat(entity.id, is(notNullValue()));
	}

	@Test // DATAMONGO-374
	public void convertsUpdateConstraintsUsingConverters() {

		CustomConversions conversions = new CustomConversions(Collections.singletonList(MyConverter.INSTANCE));
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();

		Query query = new Query();
		Update update = new Update().set("foo", new AutogenerateableId());

		template.updateFirst(query, update, Wrapper.class);

		QueryMapper queryMapper = new QueryMapper(converter);
		Document reference = queryMapper.getMappedObject(update.getUpdateObject(), Optional.empty());

		verify(collection, times(1)).updateOne(Mockito.any(org.bson.Document.class), eq(reference),
				Mockito.any(UpdateOptions.class)); // .update(Mockito.any(Document.class), eq(reference), anyBoolean(),
																						// anyBoolean());
	}

	@Test // DATAMONGO-474
	public void setsUnpopulatedIdField() {

		NotAutogenerateableId entity = new NotAutogenerateableId();

		template.populateIdIfNecessary(entity, 5);
		assertThat(entity.id, is(5));
	}

	@Test // DATAMONGO-474
	public void doesNotSetAlreadyPopulatedId() {

		NotAutogenerateableId entity = new NotAutogenerateableId();
		entity.id = 5;

		template.populateIdIfNecessary(entity, 7);
		assertThat(entity.id, is(5));
	}

	@Test // DATAMONGO-868
	public void findAndModifyShouldBumpVersionByOneWhenVersionFieldNotIncludedInUpdate() {

		VersionedEntity v = new VersionedEntity();
		v.id = 1;
		v.version = 0;

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.findAndModify(new Query(), new Update().set("id", "10"), VersionedEntity.class);
		// verify(collection, times(1)).findAndModify(Matchers.any(Document.class),
		// org.mockito.Matchers.isNull(Document.class), org.mockito.Matchers.isNull(Document.class), eq(false),
		// captor.capture(), eq(false), eq(false));

		verify(collection, times(1)).findOneAndUpdate(Matchers.any(org.bson.Document.class), captor.capture(),
				Matchers.any(FindOneAndUpdateOptions.class));
		Assert.assertThat(captor.getValue().get("$inc"), Is.<Object> is(new org.bson.Document("version", 1L)));
	}

	@Test // DATAMONGO-868
	public void findAndModifyShouldNotBumpVersionByOneWhenVersionFieldAlreadyIncludedInUpdate() {

		VersionedEntity v = new VersionedEntity();
		v.id = 1;
		v.version = 0;

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.findAndModify(new Query(), new Update().set("version", 100), VersionedEntity.class);

		verify(collection, times(1)).findOneAndUpdate(Matchers.any(org.bson.Document.class), captor.capture(),
				Matchers.any(FindOneAndUpdateOptions.class));

		// verify(collection, times(1)).findAndModify(Matchers.any(Document.class), isNull(Document.class),
		// isNull(Document.class), eq(false), captor.capture(), eq(false), eq(false));

		Assert.assertThat(captor.getValue().get("$set"), Is.<Object> is(new org.bson.Document("version", 100)));
		Assert.assertThat(captor.getValue().get("$inc"), nullValue());
	}

	@Test // DATAMONGO-533
	public void registersDefaultEntityIndexCreatorIfApplicationContextHasOneForDifferentMappingContext() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo",
				new MongoPersistentEntityIndexCreator(new MongoMappingContext(), template));
		applicationContext.refresh();

		GenericApplicationContext spy = spy(applicationContext);

		MongoTemplate mongoTemplate = new MongoTemplate(factory, converter);
		mongoTemplate.setApplicationContext(spy);

		verify(spy, times(1)).addApplicationListener(argThat(new ArgumentMatcher<MongoPersistentEntityIndexCreator>() {

			@Override
			public boolean matches(MongoPersistentEntityIndexCreator argument) {
				return argument.isIndexCreatorFor(mappingContext);
			}
		}));
	}

	@Test // DATAMONGO-566
	public void findAllAndRemoveShouldRetrieveMatchingDocumentsPriorToRemoval() {

		BasicQuery query = new BasicQuery("{'foo':'bar'}");
		template.findAllAndRemove(query, VersionedEntity.class);
		verify(collection, times(1)).find(Matchers.eq(query.getQueryObject()));
	}

	@Test // DATAMONGO-566
	public void findAllAndRemoveShouldRemoveDocumentsReturedByFindQuery() {

		Mockito.when(cursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		Mockito.when(cursor.next()).thenReturn(new org.bson.Document("_id", Integer.valueOf(0)))
				.thenReturn(new org.bson.Document("_id", Integer.valueOf(1)));

		ArgumentCaptor<org.bson.Document> queryCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		BasicQuery query = new BasicQuery("{'foo':'bar'}");
		template.findAllAndRemove(query, VersionedEntity.class);

		verify(collection, times(1)).deleteMany(queryCaptor.capture());

		Document idField = DocumentTestUtils.getAsDocument(queryCaptor.getValue(), "_id");
		assertThat((List<Object>) idField.get("$in"),
				IsIterableContainingInOrder.<Object> contains(Integer.valueOf(0), Integer.valueOf(1)));
	}

	@Test // DATAMONGO-566
	public void findAllAndRemoveShouldNotTriggerRemoveIfFindResultIsEmpty() {

		template.findAllAndRemove(new BasicQuery("{'foo':'bar'}"), VersionedEntity.class);
		verify(collection, never()).deleteMany(Mockito.any(org.bson.Document.class));
	}

	@Test // DATAMONGO-948
	public void sortShouldBeTakenAsIsWhenExecutingQueryWithoutSpecificTypeInformation() {

		Query query = Query.query(Criteria.where("foo").is("bar")).with(new Sort("foo"));
		template.executeQuery(query, "collection1", new DocumentCallbackHandler() {

			@Override
			public void processDocument(Document document) throws MongoException, DataAccessException {
				// nothing to do - just a test
			}
		});

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		verify(findIterable, times(1)).sort(captor.capture());
		assertThat(captor.getValue(), equalTo(new org.bson.Document("foo", 1)));
	}

	@Test // DATAMONGO-1166
	public void aggregateShouldHonorReadPreferenceWhenSet() {

		when(db.runCommand(Mockito.any(org.bson.Document.class), Mockito.any(ReadPreference.class), eq(Document.class)))
				.thenReturn(mock(Document.class));
		template.setReadPreference(ReadPreference.secondary());

		template.aggregate(Aggregation.newAggregation(Aggregation.unwind("foo")), "collection-1", Wrapper.class);

		verify(this.db, times(1)).runCommand(Mockito.any(org.bson.Document.class), eq(ReadPreference.secondary()),
				eq(Document.class));
	}

	@Test // DATAMONGO-1166
	public void aggregateShouldIgnoreReadPreferenceWhenNotSet() {

		when(db.runCommand(Mockito.any(org.bson.Document.class), eq(org.bson.Document.class)))
				.thenReturn(mock(Document.class));

		template.aggregate(Aggregation.newAggregation(Aggregation.unwind("foo")), "collection-1", Wrapper.class);

		verify(this.db, times(1)).runCommand(Mockito.any(org.bson.Document.class), eq(org.bson.Document.class));
	}

	@Test // DATAMONGO-1166
	public void geoNearShouldHonorReadPreferenceWhenSet() {

		when(db.runCommand(Mockito.any(org.bson.Document.class), Mockito.any(ReadPreference.class), eq(Document.class)))
				.thenReturn(mock(Document.class));
		template.setReadPreference(ReadPreference.secondary());

		NearQuery query = NearQuery.near(new Point(1, 1));
		template.geoNear(query, Wrapper.class);

		verify(this.db, times(1)).runCommand(Mockito.any(org.bson.Document.class), eq(ReadPreference.secondary()),
				eq(Document.class));
	}

	@Test // DATAMONGO-1166
	public void geoNearShouldIgnoreReadPreferenceWhenNotSet() {

		when(db.runCommand(Mockito.any(Document.class), eq(Document.class))).thenReturn(mock(Document.class));

		NearQuery query = NearQuery.near(new Point(1, 1));
		template.geoNear(query, Wrapper.class);

		verify(this.db, times(1)).runCommand(Mockito.any(Document.class), eq(Document.class));
	}

	@Test // DATAMONGO-1334
	@Ignore("TODO: mongo3 - a bit hard to tests with the immutable object stuff")
	public void mapReduceShouldUseZeroAsDefaultLimit() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(Mockito.any(Document.class))).thenReturn(output);
		when(output.filter(Mockito.any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString())).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}", Wrapper.class);

		verify(output, times(1)).limit(1);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromQuery() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(Mockito.any(Document.class))).thenReturn(output);
		when(output.filter(Mockito.any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString())).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");
		query.limit(100);

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}", Wrapper.class);

		verify(output, times(1)).limit(100);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromOptions() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(Mockito.any(Document.class))).thenReturn(output);
		when(output.filter(Mockito.any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString())).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}",
				new MapReduceOptions().limit(1000), Wrapper.class);

		verify(output, times(1)).limit(1000);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromOptionsWhenQueryIsNotPresent() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString())).thenReturn(output);

		template.mapReduce("collection", "function(){}", "function(key,values){}", new MapReduceOptions().limit(1000),
				Wrapper.class);

		verify(output, times(1)).limit(1000);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromOptionsEvenWhenQueryDefinesItDifferently() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(Mockito.any(Document.class))).thenReturn(output);
		when(output.filter(Mockito.any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString())).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");
		query.limit(100);

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}",
				new MapReduceOptions().limit(1000), Wrapper.class);

		verify(output, times(1)).limit(1000);
	}

	@Test // DATAMONGO-1639
	public void beforeConvertEventForUpdateSeesNextVersion() {

		final VersionedEntity entity = new VersionedEntity();
		entity.id = 1;
		entity.version = 0;

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		context.addApplicationListener(new AbstractMongoEventListener<VersionedEntity>() {

			@Override
			public void onBeforeConvert(BeforeConvertEvent<VersionedEntity> event) {
				assertThat(event.getSource().version, is(1));
			}
		});

		template.setApplicationContext(context);

		MongoTemplate spy = Mockito.spy(template);

		UpdateResult result = mock(UpdateResult.class);
		doReturn(1L).when(result).getModifiedCount();

		doReturn(result).when(spy).doUpdate(anyString(), Mockito.any(Query.class), Mockito.any(Update.class),
				Mockito.any(Class.class), anyBoolean(), anyBoolean());

		spy.save(entity);
	}

	class AutogenerateableId {

		@Id BigInteger id;
	}

	class NotAutogenerateableId {

		@Id Integer id;

		public Pattern getId() {
			return Pattern.compile(".");
		}
	}

	static class VersionedEntity {

		@Id Integer id;
		@Version Integer version;
	}

	enum MyConverter implements Converter<AutogenerateableId, String> {

		INSTANCE;

		public String convert(AutogenerateableId source) {
			return source.toString();
		}
	}

	class Wrapper {

		AutogenerateableId foo;
	}

	/**
	 * Mocks out the {@link MongoTemplate#getDb()} method to return the {@link DB} mock instead of executing the actual
	 * behaviour.
	 *
	 * @return
	 */
	private MongoTemplate mockOutGetDb() {

		MongoTemplate template = spy(this.template);
		when(template.getDb()).thenReturn(db);
		return template;
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.mongodb.core.core.MongoOperationsUnitTests#getOperations()
	  */
	@Override
	protected MongoOperations getOperationsForExceptionHandling() {
		MongoTemplate template = spy(this.template);
		when(template.getDb()).thenThrow(new MongoException("Error!"));
		return template;
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.mongodb.core.core.MongoOperationsUnitTests#getOperations()
	  */
	@Override
	protected MongoOperations getOperations() {
		return this.template;
	}
}
