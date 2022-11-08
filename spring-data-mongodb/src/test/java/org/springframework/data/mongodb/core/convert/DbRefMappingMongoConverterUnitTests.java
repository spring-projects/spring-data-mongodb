/*
 * Copyright 2013-2022 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.convert.LazyLoadingTestUtils.*;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.annotation.AccessType;
import org.springframework.data.annotation.AccessType.Type;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.convert.MappingMongoConverterUnitTests.Person;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.SerializationUtils;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link MappingMongoConverter}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class DbRefMappingMongoConverterUnitTests {

	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;

	@Mock MongoDatabaseFactory dbFactory;
	private DefaultDbRefResolver dbRefResolver;

	@BeforeEach
	void setUp() {

		when(dbFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

		this.dbRefResolver = spy(new DefaultDbRefResolver(dbFactory));
		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setSimpleTypeHolder(new MongoCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
		this.converter = new MappingMongoConverter(dbRefResolver, mappingContext);
	}

	@Test // DATAMONGO-347
	void createsSimpleDBRefCorrectly() {

		Person person = new Person();
		person.id = "foo";

		DBRef dbRef = converter.toDBRef(person, null);
		assertThat(dbRef.getId()).isEqualTo("foo");
		assertThat(dbRef.getCollectionName()).isEqualTo("person");
	}

	@Test // DATAMONGO-657
	void convertDocumentWithMapDBRef() {

		Document mapValDocument = new Document();
		mapValDocument.put("_id", BigInteger.ONE);

		DBRef dbRef = mock(DBRef.class);
		when(dbRef.getId()).thenReturn(BigInteger.ONE);
		when(dbRef.getCollectionName()).thenReturn("collection-1");

		MongoDatabase dbMock = mock(MongoDatabase.class);
		MongoCollection collectionMock = mock(MongoCollection.class);
		when(dbFactory.getMongoDatabase()).thenReturn(dbMock);
		when(dbMock.getCollection(anyString(), eq(Document.class))).thenReturn(collectionMock);

		FindIterable fi = mock(FindIterable.class);
		when(fi.limit(anyInt())).thenReturn(fi);
		when(fi.sort(any())).thenReturn(fi);
		when(fi.first()).thenReturn(mapValDocument);
		when(collectionMock.find(Mockito.any(Bson.class))).thenReturn(fi);

		MapDBRef mapDBRef = new MapDBRef();

		MapDBRefVal val = new MapDBRefVal();
		val.id = BigInteger.ONE;

		Map<String, MapDBRefVal> mapVal = new HashMap<>();
		mapVal.put("test", val);

		mapDBRef.map = mapVal;

		Document document = new Document();
		converter.write(mapDBRef, document);

		Document map = (Document) document.get("map");

		assertThat(map.get("test")).isInstanceOf(DBRef.class);

		((Document) document.get("map")).put("test", dbRef);

		MapDBRef read = converter.read(MapDBRef.class, document);

		assertThat(read.map.get("test").id).isEqualTo(BigInteger.ONE);
	}

	@Test // DATAMONGO-347
	void createsDBRefWithClientSpecCorrectly() {

		PropertyPath path = PropertyPath.from("person", PersonClient.class);
		MongoPersistentProperty property = mappingContext.getPersistentPropertyPath(path).getLeafProperty();

		Person person = new Person();
		person.id = "foo";

		DBRef dbRef = converter.toDBRef(person, property);
		assertThat(dbRef.getId()).isEqualTo("foo");
		assertThat(dbRef.getCollectionName()).isEqualTo("person");
	}

	@Test // DATAMONGO-348
	void lazyLoadingProxyForLazyDbRefOnInterface() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToInterface = new LinkedList<>(Collections.singletonList(new LazyDbRefTarget("1")));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToInterface, false);
		assertThat(result.dbRefToInterface.get(0).getId()).isEqualTo(id);
		assertProxyIsResolved(result.dbRefToInterface, true);
		assertThat(result.dbRefToInterface.get(0).getValue()).isEqualTo(value);
	}

	@Test // DATAMONGO-348
	@DisabledForJreRange(min = JRE.JAVA_16, disabledReason = "Class Proxies for eg; ArrayList require to open java.util.")
	void lazyLoadingProxyForLazyDbRefOnConcreteCollection() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteCollection = new ArrayList<>(Collections.singletonList(new LazyDbRefTarget(id, value)));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteCollection, false);
		assertThat(result.dbRefToConcreteCollection.get(0).getId()).isEqualTo(id);
		assertProxyIsResolved(result.dbRefToConcreteCollection, true);
		assertThat(result.dbRefToConcreteCollection.get(0).getValue()).isEqualTo(value);
	}

	@Test // DATAMONGO-348
	void lazyLoadingProxyForLazyDbRefOnConcreteType() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteType = new LazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteType, false);
		assertThat(result.dbRefToConcreteType.getId()).isEqualTo(id);
		assertProxyIsResolved(result.dbRefToConcreteType, true);
		assertThat(result.dbRefToConcreteType.getValue()).isEqualTo(value);
	}

	@Test // DATAMONGO-348
	void lazyLoadingProxyForLazyDbRefOnConcreteTypeWithPersistenceConstructor() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteTypeWithPersistenceConstructor = new LazyDbRefTargetWithPeristenceConstructor(id, value);
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructor, false);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructor.getId()).isEqualTo(id);
		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructor, true);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructor.getValue()).isEqualTo(value);
	}

	@Test // DATAMONGO-348
	void lazyLoadingProxyForLazyDbRefOnConcreteTypeWithPersistenceConstructorButWithoutDefaultConstructor() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor = new LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor(
				id, value);
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor, false);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor.getId()).isEqualTo(id);
		assertProxyIsResolved(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor, true);
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor.getValue())
				.isEqualTo(value);
	}

	@Test // DATAMONGO-348
	void lazyLoadingProxyForSerializableLazyDbRefOnConcreteType() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		SerializableClassWithLazyDbRefs lazyDbRefs = new SerializableClassWithLazyDbRefs();
		lazyDbRefs.dbRefToSerializableTarget = new SerializableLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		SerializableClassWithLazyDbRefs result = converterSpy.read(SerializableClassWithLazyDbRefs.class, document);

		SerializableClassWithLazyDbRefs deserializedResult = (SerializableClassWithLazyDbRefs) transport(result);

		assertThat(deserializedResult.dbRefToSerializableTarget.getId()).isEqualTo(id);
		assertProxyIsResolved(deserializedResult.dbRefToSerializableTarget, true);
		assertThat(deserializedResult.dbRefToSerializableTarget.getValue()).isEqualTo(value);
	}

	@Test // DATAMONGO-884
	void lazyLoadingProxyForToStringObjectMethodOverridingDbref() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToToStringObjectMethodOverride = new ToStringObjectMethodOverrideLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToToStringObjectMethodOverride).isNotNull();
		assertProxyIsResolved(result.dbRefToToStringObjectMethodOverride, false);
		assertThat(result.dbRefToToStringObjectMethodOverride.toString()).isEqualTo(id + ":" + value);
		assertProxyIsResolved(result.dbRefToToStringObjectMethodOverride, true);
	}

	@Test // DATAMONGO-884
	void callingToStringObjectMethodOnLazyLoadingDbrefShouldNotInitializeProxy() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToPlainObject = new LazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToPlainObject).isNotNull();
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		// calling Object#toString does not initialize the proxy.
		String proxyString = result.dbRefToPlainObject.toString();
		assertThat(proxyString).isEqualTo("lazyDbRefTarget" + ":" + id + "$LazyLoadingProxy");
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		// calling another method not declared on object triggers proxy initialization.
		assertThat(result.dbRefToPlainObject.getValue()).isEqualTo(value);
		assertProxyIsResolved(result.dbRefToPlainObject, true);
	}

	@Test // DATAMONGO-884
	void equalsObjectMethodOnLazyLoadingDbrefShouldNotInitializeProxy() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToPlainObject = new LazyDbRefTarget(id, value);
		lazyDbRefs.dbRefToToStringObjectMethodOverride = new ToStringObjectMethodOverrideLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToPlainObject).isNotNull();
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		assertThat(result.dbRefToPlainObject).isEqualTo(result.dbRefToPlainObject);
		assertThat(result.dbRefToPlainObject).isNotEqualTo(null);
		assertThat(result.dbRefToPlainObject).isNotEqualTo((Object) lazyDbRefs.dbRefToToStringObjectMethodOverride);

		assertProxyIsResolved(result.dbRefToPlainObject, false);
	}

	@Test // DATAMONGO-884
	void hashcodeObjectMethodOnLazyLoadingDbrefShouldNotInitializeProxy() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefToPlainObject = new LazyDbRefTarget(id, value);
		lazyDbRefs.dbRefToToStringObjectMethodOverride = new ToStringObjectMethodOverrideLazyDbRefTarget(id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertThat(result.dbRefToPlainObject).isNotNull();
		assertProxyIsResolved(result.dbRefToPlainObject, false);

		assertThat(result.dbRefToPlainObject.hashCode()).isEqualTo(311365444);

		assertProxyIsResolved(result.dbRefToPlainObject, false);
	}

	@Test // DATAMONGO-884
	void lazyLoadingProxyForEqualsAndHashcodeObjectMethodOverridingDbref() {

		String id = "42";
		String value = "bubu";
		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id).append("value", value)).when(converterSpy).readRef(any());

		Document document = new Document();
		WithObjectMethodOverrideLazyDbRefs lazyDbRefs = new WithObjectMethodOverrideLazyDbRefs();
		lazyDbRefs.dbRefEqualsAndHashcodeObjectMethodOverride1 = new EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget(
				id, value);
		lazyDbRefs.dbRefEqualsAndHashcodeObjectMethodOverride2 = new EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget(
				id, value);
		converterSpy.write(lazyDbRefs, document);

		WithObjectMethodOverrideLazyDbRefs result = converterSpy.read(WithObjectMethodOverrideLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride1, false);
		assertThat(result.dbRefEqualsAndHashcodeObjectMethodOverride1).isNotNull();
		result.dbRefEqualsAndHashcodeObjectMethodOverride1.equals(null);
		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride1, true);

		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride2, false);
		assertThat(result.dbRefEqualsAndHashcodeObjectMethodOverride2).isNotNull();
		result.dbRefEqualsAndHashcodeObjectMethodOverride2.hashCode();
		assertProxyIsResolved(result.dbRefEqualsAndHashcodeObjectMethodOverride2, true);
	}

	@Test // DATAMONGO-987
	void shouldNotGenerateLazyLoadingProxyForNullValues() {

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.id = "42";
		converter.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converter.read(ClassWithLazyDbRefs.class, document);

		assertThat(result.id).isEqualTo(lazyDbRefs.id);
		assertThat(result.dbRefToInterface).isNull();
		assertThat(result.dbRefToConcreteCollection).isNull();
		assertThat(result.dbRefToConcreteType).isNull();
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructor).isNull();
		assertThat(result.dbRefToConcreteTypeWithPersistenceConstructorWithoutDefaultConstructor).isNull();
	}

	@Test // DATAMONGO-1005
	void shouldBeAbleToStoreDirectReferencesToSelf() {

		Document document = new Document();

		ClassWithDbRefField o = new ClassWithDbRefField();
		o.id = "123";
		o.reference = o;
		converter.write(o, document);

		ClassWithDbRefField found = converter.read(ClassWithDbRefField.class, document);

		assertThat(found).isNotNull();
		assertThat(found.reference).isEqualTo(found);
	}

	@Test // DATAMONGO-1005
	void shouldBeAbleToStoreNestedReferencesToSelf() {

		Document document = new Document();

		ClassWithNestedDbRefField o = new ClassWithNestedDbRefField();
		o.id = "123";
		o.nested = new NestedReferenceHolder();
		o.nested.reference = o;

		converter.write(o, document);

		ClassWithNestedDbRefField found = converter.read(ClassWithNestedDbRefField.class, document);

		assertThat(found).isNotNull();
		assertThat(found.nested).isNotNull();
		assertThat(found.nested.reference).isEqualTo(found);
	}

	@Test // DATAMONGO-1012
	void shouldEagerlyResolveIdPropertyWithFieldAccess() {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(ClassWithLazyDbRefs.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("dbRefToConcreteType");
		MongoPersistentEntity<?> propertyEntity = mappingContext.getRequiredPersistentEntity(property);

		String idValue = new ObjectId().toString();
		DBRef dbRef = converter.toDBRef(new LazyDbRefTarget(idValue), property);

		Document object = new Document("dbRefToConcreteType", dbRef);

		ClassWithLazyDbRefs result = converter.read(ClassWithLazyDbRefs.class, object);

		PersistentPropertyAccessor accessor = propertyEntity.getPropertyAccessor(result.dbRefToConcreteType);
		MongoPersistentProperty idProperty = mappingContext.getRequiredPersistentEntity(LazyDbRefTarget.class)
				.getIdProperty();

		assertThat(accessor.getProperty(idProperty)).isNotNull();
		assertProxyIsResolved(result.dbRefToConcreteType, false);
	}

	@Test // DATAMONGO-1012
	void shouldNotEagerlyResolveIdPropertyWithPropertyAccess() {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(ClassWithLazyDbRefs.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("dbRefToConcreteTypeWithPropertyAccess");

		String idValue = new ObjectId().toString();
		DBRef dbRef = converter.toDBRef(new LazyDbRefTargetPropertyAccess(idValue), property);

		Document object = new Document("dbRefToConcreteTypeWithPropertyAccess", dbRef);

		ClassWithLazyDbRefs result = converter.read(ClassWithLazyDbRefs.class, object);

		LazyDbRefTargetPropertyAccess proxy = result.dbRefToConcreteTypeWithPropertyAccess;
		assertThat(ReflectionTestUtils.getField(proxy, "id")).isNull();
		assertProxyIsResolved(proxy, false);
	}

	@Test // DATAMONGO-1076
	@DisabledForJreRange(min = JRE.JAVA_16, disabledReason = "Class Proxies for eg; ArrayList require to open java.util.")
	void shouldNotTriggerResolvingOfLazyLoadedProxyWhenFinalizeMethodIsInvoked() throws Exception {

		MongoPersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(WithObjectMethodOverrideLazyDbRefs.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("dbRefToPlainObject");

		String idValue = new ObjectId().toString();
		DBRef dbRef = converter.toDBRef(new LazyDbRefTargetPropertyAccess(idValue), property);

		WithObjectMethodOverrideLazyDbRefs result = converter.read(WithObjectMethodOverrideLazyDbRefs.class,
				new Document("dbRefToPlainObject", dbRef));

		ReflectionTestUtils.invokeMethod(result.dbRefToPlainObject, "finalize");

		assertProxyIsResolved(result.dbRefToPlainObject, false);
	}

	@Test // DATAMONGO-1194
	@DisabledForJreRange(min = JRE.JAVA_16, disabledReason = "Class Proxies for eg; ArrayList require to open java.util.")
	void shouldBulkFetchListOfReferences() {

		String id1 = "1";
		String id2 = "2";
		String value = "val";

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(
				Arrays.asList(new Document("_id", id1).append("value", value), new Document("_id", id2).append("value", value)))
						.when(converterSpy).bulkReadRefs(anyList());

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteCollection = new ArrayList<>(
				Arrays.asList(new LazyDbRefTarget(id1, value), new LazyDbRefTarget(id2, value)));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteCollection, false);
		assertThat(result.dbRefToConcreteCollection.get(0).getId()).isEqualTo(id1);
		assertProxyIsResolved(result.dbRefToConcreteCollection, true);
		assertThat(result.dbRefToConcreteCollection.get(1).getId()).isEqualTo(id2);

		verify(converterSpy, never()).readRef(Mockito.any(DBRef.class));
	}

	@Test // DATAMONGO-1666
	void shouldBulkFetchSetOfReferencesForConstructorCreation() {

		String id1 = "1";
		String id2 = "2";
		String value = "val";

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(
				Arrays.asList(new Document("_id", id1).append("value", value), new Document("_id", id2).append("value", value)))
						.when(converterSpy).bulkReadRefs(anyList());

		Document document = new Document("dbRefToInterface",
				Arrays.asList(new DBRef("lazyDbRefTarget", "1"), new DBRef("lazyDbRefTarget", "2")));

		ClassWithDbRefSetConstructor result = converterSpy.read(ClassWithDbRefSetConstructor.class, document);

		assertThat(result.dbRefToInterface).isInstanceOf(Set.class);

		verify(converterSpy, never()).readRef(Mockito.any(DBRef.class));
	}

	@Test // DATAMONGO-1194
	@DisabledForJreRange(min = JRE.JAVA_16, disabledReason = "Class Proxies for eg; ArrayList require to open java.util.")
	void shouldFallbackToOneByOneFetchingWhenElementsInListOfReferencesPointToDifferentCollections() {

		String id1 = "1";
		String id2 = "2";
		String value = "val";

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(new Document("_id", id1).append("value", value)).doReturn(new Document("_id", id2).append("value", value))
				.when(converterSpy).readRef(Mockito.any(DBRef.class));

		Document document = new Document();
		ClassWithLazyDbRefs lazyDbRefs = new ClassWithLazyDbRefs();
		lazyDbRefs.dbRefToConcreteCollection = new ArrayList<>(
				Arrays.asList(new LazyDbRefTarget(id1, value), new SerializableLazyDbRefTarget(id2, value)));
		converterSpy.write(lazyDbRefs, document);

		ClassWithLazyDbRefs result = converterSpy.read(ClassWithLazyDbRefs.class, document);

		assertProxyIsResolved(result.dbRefToConcreteCollection, false);
		assertThat(result.dbRefToConcreteCollection.get(0).getId()).isEqualTo(id1);
		assertProxyIsResolved(result.dbRefToConcreteCollection, true);
		assertThat(result.dbRefToConcreteCollection.get(1).getId()).isEqualTo(id2);

		verify(converterSpy, times(2)).readRef(Mockito.any(DBRef.class));
		verify(converterSpy, never()).bulkReadRefs(anyList());
	}

	@Test // DATAMONGO-1194
	void shouldBulkFetchMapOfReferences() {

		MapDBRefVal val1 = new MapDBRefVal();
		val1.id = BigInteger.ONE;

		MapDBRefVal val2 = new MapDBRefVal();
		val2.id = BigInteger.ZERO;

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(Arrays.asList(new Document("_id", val1.id), new Document("_id", val2.id))).when(converterSpy)
				.bulkReadRefs(anyList());

		Document document = new Document();
		MapDBRef mapDBRef = new MapDBRef();
		mapDBRef.map = new LinkedHashMap<>();
		mapDBRef.map.put("one", val1);
		mapDBRef.map.put("two", val2);

		converterSpy.write(mapDBRef, document);

		MapDBRef result = converterSpy.read(MapDBRef.class, document);

		// assertProxyIsResolved(result.map, false);
		assertThat(result.map.get("one").id).isEqualTo(val1.id);
		// assertProxyIsResolved(result.map, true);
		assertThat(result.map.get("two").id).isEqualTo(val2.id);

		verify(converterSpy, times(1)).bulkReadRefs(anyList());
		verify(converterSpy, never()).readRef(Mockito.any(DBRef.class));
	}

	@Test // DATAMONGO-1194
	void shouldBulkFetchLazyMapOfReferences() {

		MapDBRefVal val1 = new MapDBRefVal();
		val1.id = BigInteger.ONE;

		MapDBRefVal val2 = new MapDBRefVal();
		val2.id = BigInteger.ZERO;

		MappingMongoConverter converterSpy = spy(converter);
		doReturn(Arrays.asList(new Document("_id", val1.id), new Document("_id", val2.id))).when(converterSpy)
				.bulkReadRefs(anyList());

		Document document = new Document();
		MapDBRef mapDBRef = new MapDBRef();
		mapDBRef.lazyMap = new LinkedHashMap<>();
		mapDBRef.lazyMap.put("one", val1);
		mapDBRef.lazyMap.put("two", val2);

		converterSpy.write(mapDBRef, document);

		MapDBRef result = converterSpy.read(MapDBRef.class, document);

		assertProxyIsResolved(result.lazyMap, false);
		assertThat(result.lazyMap.get("one").id).isEqualTo(val1.id);
		assertProxyIsResolved(result.lazyMap, true);
		assertThat(result.lazyMap.get("two").id).isEqualTo(val2.id);

		verify(converterSpy, times(1)).bulkReadRefs(anyList());
		verify(converterSpy, never()).readRef(any());
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

	static class ClassWithDbRefSetConstructor {

		final @org.springframework.data.mongodb.core.mapping.DBRef Set<LazyDbRefTarget> dbRefToInterface;

		public ClassWithDbRefSetConstructor(Set<LazyDbRefTarget> dbRefToInterface) {
			this.dbRefToInterface = dbRefToInterface;
		}
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

		LazyDbRefTarget() {
			this(null);
		}

		LazyDbRefTarget(String id) {
			this(id, null);
		}

		LazyDbRefTarget(String id, String value) {
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

		LazyDbRefTargetPropertyAccess(String id) {
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
		LazyDbRefTargetWithPeristenceConstructor(String id, String value) {
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
		LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor(String id, String value) {
			super(id, value);
			this.persistenceConstructorCalled = true;
		}

		public LazyDbRefTargetWithPeristenceConstructorWithoutDefaultConstructor(Object id, Object value) {
			super(id.toString(), value.toString());
		}
	}

	static class SerializableLazyDbRefTarget extends LazyDbRefTarget implements Serializable {

		public SerializableLazyDbRefTarget() {}

		SerializableLazyDbRefTarget(String id, String value) {
			super(id, value);
		}

		private static final long serialVersionUID = 1L;
	}

	static class ToStringObjectMethodOverrideLazyDbRefTarget extends LazyDbRefTarget {

		private static final long serialVersionUID = 1L;

		public ToStringObjectMethodOverrideLazyDbRefTarget() {}

		ToStringObjectMethodOverrideLazyDbRefTarget(String id, String value) {
			super(id, value);
		}

		@Override
		public String toString() {
			return this.id + ":" + this.value;
		}
	}

	static class EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget extends LazyDbRefTarget {

		private static final long serialVersionUID = 1L;

		public EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget() {}

		EqualsAndHashCodeObjectMethodOverrideLazyDbRefTarget(String id, String value) {
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
		public boolean equals(@Nullable Object obj) {
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
