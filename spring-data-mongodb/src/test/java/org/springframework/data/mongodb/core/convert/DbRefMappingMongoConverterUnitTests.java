/*
 * Copyright 2013-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.convert.LazyLoadingTestUtils.*;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.AccessType;
import org.springframework.data.annotation.AccessType.Type;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.convert.MappingMongoConverterUnitTests.Person;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.SerializationUtils;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link DbRefMappingMongoConverter}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DbRefMappingMongoConverterUnitTests {

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Mock MongoDbFactory dbFactory;
	DefaultDbRefResolver dbRefResolver;

	@Before
	public void setUp() {

		when(dbFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

		this.dbRefResolver = spy(new DefaultDbRefResolver(dbFactory));
		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(dbRefResolver, mappingContext);
	}

	@Test // DATAMONGO-347
	public void createsSimpleDBRefCorrectly() {

		Person person = new Person();
		person.id = "foo";

		DBRef dbRef = converter.toDBRef(person, null);
		assertThat(dbRef.getId(), is((Object) "foo"));
		assertThat(dbRef.getCollectionName(), is("person"));
	}

	@Test // DATAMONGO-657
	public void convertDocumentWithMapDBRef() {

		Document mapValDocument = new Document();
		mapValDocument.put("_id", BigInteger.ONE);

		DBRef dbRef = mock(DBRef.class);
		when(dbRef.getId()).thenReturn(BigInteger.ONE);
		when(dbRef.getCollectionName()).thenReturn("collection-1");

		if (MongoClientVersion.isMongo3Driver()) {
			MongoDatabase dbMock = mock(MongoDatabase.class);
			MongoCollection collectionMock = mock(MongoCollection.class);
			when(dbFactory.getDb()).thenReturn(dbMock);
			when(dbMock.getCollection(anyString(), eq(Document.class))).thenReturn(collectionMock);

			FindIterable fi = mock(FindIterable.class);
			when(fi.first()).thenReturn(mapValDocument);
			when(collectionMock.find(Mockito.any(Bson.class))).thenReturn(fi);
		} else {
			when(dbRefResolver.fetch(dbRef)).thenReturn(mapValDocument);
		}

		MapDBRef mapDBRef = new MapDBRef();

		MapDBRefVal val = new MapDBRefVal();
		val.id = BigInteger.ONE;

		Map<String, MapDBRefVal> mapVal = new HashMap<String, MapDBRefVal>();
		mapVal.put("test", val);

		mapDBRef.map = mapVal;

		Document document = new Document();
		converter.write(mapDBRef, document);

		Document map = (Document) document.get("map");

		assertThat(map.get("test"), instanceOf(DBRef.class));

		((Document) document.get("map")).put("test", dbRef);

		MapDBRef read = converter.read(MapDBRef.class, document);

		assertThat(read.map.get("test").id, is(BigInteger.ONE));
	}

	@Test // DATAMONGO-347
	public void createsDBRefWithClientSpecCorrectly() {

		PropertyPath path = PropertyPath.from("person", PersonClient.class);
		MongoPersistentProperty property = mappingContext.getPersistentPropertyPath(path).getLeafProperty();

		Person person = new Person();
		person.id = "foo";

		DBRef dbRef = converter.toDBRef(person, property);
		assertThat(dbRef.getId(), is((Object) "foo"));
		assertThat(dbRef.getCollectionName(), is("person"));
	}

	@Test // DATAMONGO-348
	public void lazyLoadingProxyForLazyDbRefOnInterface() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToInterface = new LinkedList<LazyDbRefTarget>(Arrays.asList(new LazyDbRefTarget("1")));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToInterface, false);
		assertThat(result.dbRefToInterface.get(0).getId(), is(id));
		assertProxyIsResolved(result.dbRefToInterface, true);
		assertThat(result.dbRefToInterface.get(0).getValue(), is(value));
	}

	@Test // DATAMONGO-348
	public void lazyLoadingProxyForLazyDbRefOnConcreteCollection() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteCollection = new ArrayList<LazyDbRefTarget>(
				Arrays.asList(new LazyDbRefTarget(id, value)));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteCollection, false);
		assertThat(result.dbRefToConcreteCollection.get(0).getId(), is(id));
		assertProxyIsResolved(result.dbRefToConcreteCollection, true);
		assertThat(result.dbRefToConcreteCollection.get(0).getValue(), is(value));
	}

	@Test // DATAMONGO-348
	public void lazyLoadingProxyForLazyDbRefOnConcreteType() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteType = new LazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteType, false);
		assertThat(result.dbRefToConcreteType.getId(), is(id));
		assertProxyIsResolved(result.dbRefToConcreteType, true);
		assertThat(result.dbRefToConcreteType.getValue(), is(value));
	}

	@Test // DATAMONGO-348
	public void lazyLoadingProxyForLazyDbRefOnConcreteTypeWithPersistenceConstructor() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteTypeWithPersistenceConstructor = new LazyDbRefTargetWithPeristenceConstructor((Object) id,
				(Object) value);
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructor, false);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructor.getId(), is(id));
		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructor, true);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructor.getValue(), is(value));
	}

	@Test // DATAMONGO-348
	public void lazyLoadingProxyForLazyDbRefOnConcreteTypeWithPersistenceConstructorButWithoutDefaultConstructor() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor = new LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor(
				(Object) id, (Object) value);
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor, false);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor.getId(), is(id));
		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor, true);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor.getValue(), is(value));
	}

	@Test // DATAMONGO-348
	public void lazyLoadingProxyForSerializableLazyDbRefOnConcreteType() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		SerializableClassWithLazyDbRefs lazyDbRefs = new SerializableClassWithLazyDbRefs();
		lazyDbRefs.dbRefToSerializableTarget = new SerializableLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		SerializableClassWithLazyDbRefs result = converterSpy.read(SerializableClassWithLazyDbRefs.class, document);

		SerializableClassWithLazyDbRefs deserializedResult = (SerializableClassWithLazyDbRefs) transport(result);

		assertThat(deserializedResult.dbRefToSerializableTarget.getId(), is(id));
		assertProxyIsResolved(deserializedResult.dbRefToSerializableTarget, true);
		assertThat(deserializedResult.dbRefToSerializableTarget.getValue(), is(value));
	}

	@Test // DATAMONGO-884
	public void lazyLoadingProxyForToStringObjectMethodOverridingDbref() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToToStringObjectMethodOverride = new ToStringObjectMethodOverrideLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToToStringObjectMethodOverride, is(notNullValue()));
		assertProxyIsResolved(result.dbRefToToStringObjectMethodOverride, false);
		assertThat(result.dbRefToToStringObjectMethodOverride.toString(), is(id + ":" + value));
		assertProxyIsResolved(result.dbRefToToStringObjectMethodOverride, true);
	}

	@Test // DATAMONGO-884
	public void callingToStringObjectMethodOnLazyLoadingDbrefShouldNotInitializeProxy() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToPlainObject = new LazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToPlainObject, is(notNullValue()));
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		// calling Object#toString does not initialize the proxy.
		String proxyString = result.dbRefToPlainObject.toString();
		assertThat(proxyString, is("lazyDbRefTarget" + ":" + id + "$LazyLoadingProxy"));
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		// calling another method not declared on object triggers proxy initialization.
		assertThat(result.dbRefToPlainObject.getValue(), is(value));
		assertProxyIsResolved(result.dbRefToPlainObject, true);
	}

	@Test // DATAMONGO-884
	public void equalsObjectMethodOnLazyLoadingDbrefShouldNotInitializeProxy() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToPlainObject = new LazyDbRefTarget(id, value);
		lazyDbRefs.dbRefToToStringObjectMethodOverride = new ToStringObjectMethodOverrideLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToPlainObject, is(notNullValue()));
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		assertThat(result.dbRefToPlainObject, is(equalTo(result.dbRefToPlainObject)));
		assertThat(result.dbRefToPlainObject, is(not(equalTo(null))));
		assertThat(result.dbRefToPlainObject, is(not(equalTo((Object) lazyDbRefs.dbRefToToStringObjectMethodOverride))));

		assertProxyIsResolved(result.dbRefToPlainObject, false);
	}

	@Test // DATAMONGO-884
	public void hashcodeObjectMethodOnLazyLoadingDbrefShouldNotInitializeProxy() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToPlainObject = new LazyDbRefTarget(id, value);
		lazyDbRefs.dbRefToToStringObjectMethodOverride = new ToStringObjectMethodOverrideLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToPlainObject, is(notNullValue()));
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		assertThat(result.dbRefToPlainObject.hashCode(), is(311365444));

		assertProxyIsResolved(result.dbRefToPlainObject, false);
	}

	@Test // DATAMONGO-884
	public void lazyLoadingProxyForEqualsAndHashcodeObjectMethodOverridingDbref() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef((DBRef) any());

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefEqualsAndHashcodeObjectMethodOverride1 = new EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget(
				id, value);
		lazyDbRefs.dbRefEqualsAndHashcodeObjectMethodOverride2 = new EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget(
				id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride1, false);
		assertThat(result.dbRefEqualsAndHashcodeObjectMethodOverride1, is(notNullValue()));
		result.dbRefEqualsAndHashcodeObjectMethodOverride1.equals(null);
		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride1, true);

		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride2, false);
		assertThat(result.dbRefEqualsAndHashcodeObjectMethodOverride2, is(notNullValue()));
		result.dbRefEqualsAndHashcodeObjectMethodOverride2.hashCode();
		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride2, true);
	}

	@Test // DATAMONGO-987
	public void shouldNotGenerateLazyLoadingProxyForNullValues() {

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.id = "42";
		converter.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converter.read(ClassWithLazyDbRefs.class, document);

		assertThat(result.id, is(lazyDbRefs.id));
		assertThat(result.dbRefToInterface, is(nullValue()));
		assertThat(result.dbRefToConcreteCollection, is(nullValue()));
		assertThat(result.dbRefToConcreteType, is(nullValue()));
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructor, is(nullValue()));
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor, is(nullValue()));
	}

	@Test // DATAMONGO-1005
	public void shouldBeAbleToStoreDirectReferencesToSelf() {

		Document document = new Document();

		ClassWithDbRefField o = new ClassWithDbRefField();
		o.id = "123";
		o.reference = o;
		converter.write(o, document);

		ClassWithDbRefField found = converter.read(ClassWithDbRefField.class, document);

		assertThat(found, is(notNullValue()));
		assertThat(found.reference, is(found));
	}

	@Test // DATAMONGO-1005
	public void shouldBeAbleToStoreNestedReferencesToSelf() {

		Document document = new Document();

		ClassWithNestedDbRefField o = new ClassWithNestedDbRefField();
		o.id = "123";
		o.nested = new NestedReferenceHolder();
		o.nested.reference = o;

		converter.write(o, document);

		ClassWithNestedDbRefField found = converter.read(ClassWithNestedDbRefField.class, document);

		assertThat(found, is(notNullValue()));
		assertThat(found.nested, is(notNullValue()));
		assertThat(found.nested.reference, is(found));
	}

	@Test // DATAMONGO-1012
	public void shouldEagerlyResolveIdPropertyWithFieldAccess() {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(ClassWithLazyDbRefs.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("dbRefToConcreteType");
		MongoPersistentEntity<?> propertyEntity = mappingContext.getRequiredPersistentEntity(property);

		String idValue = new ObjectId().toString();
		DBRef dbRef = converter.toDBRef(new LazyDbRefTarget(idValue), property);

		Document object = new Document("dbRefToConcreteType", dbRef);

		ClassWithLazyDbRefs result = converter.read(ClassWithLazyDbRefs.class, object);

		PersistentPropertyAccessor accessor = propertyEntity.getPropertyAccessor(result.dbRefToConcreteType);
		MongoPersistentProperty idProperty = mappingContext.getRequiredPersistentEntity(LazyDbRefTarget.class).getIdProperty().get();

		assertThat(accessor.getProperty(idProperty), is(notNullValue()));
		assertProxyIsResolved(result.dbRefToConcreteType, false);
	}

	@Test // DATAMONGO-1012
	public void shouldNotEagerlyResolveIdPropertyWithPropertyAccess() {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(ClassWithLazyDbRefs.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("dbRefToConcreteTypeWithPropertyAccess");

		String idValue = new ObjectId().toString();
		DBRef dbRef = converter.toDBRef(new LazyDbRefTargetPropertyAccess(idValue), property);

		Document object = new Document("dbRefToConcreteTypeWithPropertyAccess", dbRef);

		ClassWithLazyDbRefs result = converter.read(ClassWithLazyDbRefs.class, object);

		LazyDbRefTargetPropertyAccess proxy = result.dbRefToConcreteTypeWithPropertyAccess;
		assertThat(ReflectionTestUtils.getField(proxy, "id"), is(nullValue()));
		assertProxyIsResolved(proxy, false);
	}

	@Test // DATAMONGO-1076
	public void shouldNotTriggerResolvingOfLazyLoadedProxyWhenFinalizeMethodIsInvoked() throws Exception {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(WithObjectMethodOverrideLazyDbRefs.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("dbRefToPlainObject");

		String idValue = new ObjectId().toString();
		DBRef dbRef = converter.toDBRef(new LazyDbRefTargetPropertyAccess(idValue), property);

		WithObjectMethodOverrideLazyDbRefs result = converter.read(WithObjectMethodOverrideLazyDbRefs.class,
				new Document("dbRefToPlainObject", dbRef));

		ReflectionTestUtils.invokeMethod(result.dbRefToPlainObject, "finalize");

		assertProxyIsResolved(result.dbRefToPlainObject, false);
	}

	@Test // DATAMONGO-1194
	public void shouldBulkFetchListOfReferences() {

		String id1 = "1";
		String id2 = "2";
		String value = "val";

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(Arrays.asList(new Document("_id", id1).append("value", value),
				new Document("_id", id2).append("value", value))).when(converterSpy).bulkReadRefs(anyListOf(DBRef.class));

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteCollection = new ArrayList<LazyDbRefTarget>(
				Arrays.asList(new LazyDbRefTarget(id1, value), new LazyDbRefTarget(id2, value)));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteCollection, false);
		assertThat(result.dbRefToConcreteCollection.get(0).getId(), is(id1));
		assertProxyIsResolved(result.dbRefToConcreteCollection, true);
		assertThat(result.dbRefToConcreteCollection.get(1).getId(), is(id2));

		verify(converterSpy, never()).readRef(Mockito.any(DBRef.class));
	}

	@Test // DATAMONGO-1194
	public void shouldFallbackToOneByOneFetchingWhenElementsInListOfReferencesPointToDifferentCollections() {

		String id1 = "1";
		String id2 = "2";
		String value = "val";

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id1).append("value", value))
				.doReturn(new Document("_id", id2).append("value", value)).when(converterSpy)
				.readRef(Mockito.any(DBRef.class));

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteCollection = new ArrayList<LazyDbRefTarget>(
				Arrays.asList(new LazyDbRefTarget(id1, value), new SerializableLazyDbRefTarget(id2, value)));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteCollection, false);
		assertThat(result.dbRefToConcreteCollection.get(0).getId(), is(id1));
		assertProxyIsResolved(result.dbRefToConcreteCollection, true);
		assertThat(result.dbRefToConcreteCollection.get(1).getId(), is(id2));

		verify(converterSpy, times(2)).readRef(Mockito.any(DBRef.class));
		verify(converterSpy, never()).bulkReadRefs(anyListOf(DBRef.class));
	}

	@Test // DATAMONGO-1194
	public void shouldBulkFetchMapOfReferences() {

		MapDBRefVal val1 = new MapDBRefVal();
		val1.id = BigInteger.ONE;

		MapDBRefVal val2 = new MapDBRefVal();
		val2.id = BigInteger.ZERO;

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(Arrays.asList(new Document("_id", val1.id), new Document("_id", val2.id))).when(converterSpy)
				.bulkReadRefs(anyListOf(DBRef.class));

		Document document = new Document();
		MapDBRef mapDBRef = new MapDBRef();
		mapDBRef.map = new LinkedHashMap<String, MapDBRefVal>();
		mapDBRef.map.put("one", val1);
		mapDBRef.map.put("two", val2);

		converterSpy.write(mapDBRef, document);

		MapDBRef result = converterSpy.read(MapDBRef.class, document);

		// assertProxyIsResolved(result.map, false);
		assertThat(result.map.get("one").id, is(val1.id));
		// assertProxyIsResolved(result.map, true);
		assertThat(result.map.get("two").id, is(val2.id));

		verify(converterSpy, times(1)).bulkReadRefs(anyListOf(DBRef.class));
		verify(converterSpy, never()).readRef(Mockito.any(DBRef.class));
	}

	@Test // DATAMONGO-1194
	public void shouldBulkFetchLazyMapOfReferences() {

		MapDBRefVal val1 = new MapDBRefVal();
		val1.id = BigInteger.ONE;

		MapDBRefVal val2 = new MapDBRefVal();
		val2.id = BigInteger.ZERO;

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(Arrays.asList(new Document("_id", val1.id), new Document("_id", val2.id))).when(converterSpy)
				.bulkReadRefs(anyListOf(DBRef.class));

		Document document = new Document();
		MapDBRef mapDBRef = new MapDBRef();
		mapDBRef.lazyMap = new LinkedHashMap<String, MapDBRefVal>();
		mapDBRef.lazyMap.put("one", val1);
		mapDBRef.lazyMap.put("two", val2);

		converterSpy.write(mapDBRef, document);

		MapDBRef result = converterSpy.read(MapDBRef.class, document);

		assertProxyIsResolved(result.lazyMap, false);
		assertThat(result.lazyMap.get("one").id, is(val1.id));
		assertProxyIsResolved(result.lazyMap, true);
		assertThat(result.lazyMap.get("two").id, is(val2.id));

		verify(converterSpy, times(1)).bulkReadRefs(anyListOf(DBRef.class));
		verify(converterSpy, never()).readRef(Mockito.any(DBRef.class));
	}

	private Object transport(Object result) {
		return SerializationUtils.deserialize(SerializationUtils.serialize(result));
	}

	class MapDBRef {
		@org.springframework.data.mongodb.core.mapping.DBRef Map<String, MapDBRefVal> map;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) Map<String, MapDBRefVal> lazyMap;
	}

	class MapDBRefVal {
		BigInteger id;
	}

	class PersonClient {
		@org.springframework.data.mongodb.core.mapping.DBRef Person person;
	}

	static class ClassWithLazyDbRefs {

		@Id String id;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) List<LazyDbRefTarget> dbRefToInterface;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) ArrayList<LazyDbRefTarget> dbRefToConcreteCollection;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) LazyDbRefTarget dbRefToConcreteType;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) LazyDbRefTargetPropertyAccess dbRefToConcreteTypeWithPropertyAccess;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) LazyDbRefTargetWithPeristenceConstructor dbRefToConcreteTypeWithPersistenceConstructor;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor;
	}

	static class SerializableClassWithLazyDbRefs implements Serializable {

		private static final long serialVersionUID = 1L;

		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) SerializableLazyDbRefTarget dbRefToSerializableTarget;
	}

	static class LazyDbRefTarget implements Serializable {

		private static final long serialVersionUID = 1L;

		@Id String id;
		String value;

		public LazyDbRefTarget() {
			this(null);
		}

		public LazyDbRefTarget(String id) {
			this(id, null);
		}

		public LazyDbRefTarget(String id, String value) {
			this.id = id;
			this.value = value;
		}

		public String getId() {
			return id;
		}

		public String getValue() {
			return value;
		}
	}

	static class LazyDbRefTargetPropertyAccess implements Serializable {

		private static final long serialVersionUID = 1L;

		@Id @AccessType(Type.PROPERTY) String id;

		public LazyDbRefTargetPropertyAccess(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}
	}

	@SuppressWarnings("serial")
	static class LazyDbRefTargetWithPeristenceConstructor extends LazyDbRefTarget {

		boolean persistenceConstructorCalled;

		public LazyDbRefTargetWithPeristenceConstructor() {}

		@PersistenceConstructor
		public LazyDbRefTargetWithPeristenceConstructor(String id, String value) {
			super(id, value);
			this.persistenceConstructorCalled = true;
		}

		public LazyDbRefTargetWithPeristenceConstructor(Object id, Object value) {
			super(id.toString(), value.toString());
		}
	}

	@SuppressWarnings("serial")
	static class LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor extends LazyDbRefTarget {

		boolean persistenceConstructorCalled;

		@PersistenceConstructor
		public LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor(String id, String value) {
			super(id, value);
			this.persistenceConstructorCalled = true;
		}

		public LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor(Object id, Object value) {
			super(id.toString(), value.toString());
		}
	}

	static class SerializableLazyDbRefTarget extends LazyDbRefTarget implements Serializable {

		public SerializableLazyDbRefTarget() {}

		public SerializableLazyDbRefTarget(String id, String value) {
			super(id, value);
		}

		private static final long serialVersionUID = 1L;
	}

	static class ToStringObjectMethodOverrideLazyDbRefTarget extends LazyDbRefTarget {

		private static final long serialVersionUID = 1L;

		public ToStringObjectMethodOverrideLazyDbRefTarget() {}

		public ToStringObjectMethodOverrideLazyDbRefTarget(String id, String value) {
			super(id, value);
		}

		/* 
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return this.id + ":" + this.value;
		}
	}

	static class EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget extends LazyDbRefTarget {

		private static final long serialVersionUID = 1L;

		public EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget() {}

		public EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget(String id, String value) {
			super(id, value);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget other = (EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}
	}

	static class WithObjectMethodOverrideLazyDbRefs {

		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) LazyDbRefTarget dbRefToPlainObject;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) ToStringObjectMethodOverrideLazyDbRefTarget dbRefToToStringObjectMethodOverride;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget dbRefEqualsAndHashcodeObjectMethodOverride2;
		@org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget dbRefEqualsAndHashcodeObjectMethodOverride1;
	}

	class ClassWithDbRefField {

		String id;
		@org.springframework.data.mongodb.core.mapping.DBRef ClassWithDbRefField reference;
	}

	static class NestedReferenceHolder {

		String id;
		@org.springframework.data.mongodb.core.mapping.DBRef ClassWithNestedDbRefField reference;
	}

	static class ClassWithNestedDbRefField {

		String id;
		NestedReferenceHolder nested;
	}
}
