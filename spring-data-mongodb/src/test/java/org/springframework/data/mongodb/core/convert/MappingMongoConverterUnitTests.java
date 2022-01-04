/*
 * Copyright 2011-2021 the original author or authors.
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

import static java.time.ZoneId.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.model.MappingInstantiationException;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.convert.DocumentAccessorUnitTests.NestedType;
import org.springframework.data.mongodb.core.convert.DocumentAccessorUnitTests.ProjectingType;
import org.springframework.data.mongodb.core.convert.MappingMongoConverterUnitTests.ClassWithMapUsingEnumAsKey.FooBarEnum;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;
import org.springframework.data.mongodb.core.mapping.TextScore;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertCallback;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * Unit tests for {@link MappingMongoConverter}.
 *
 * @author Oliver Gierke
 * @author Patrik Wasik
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Roman Puchkovskiy
 * @author Heesu Jung
 */
@ExtendWith(MockitoExtension.class)
class MappingMongoConverterUnitTests {

	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;
	@Mock ApplicationContext context;
	@Mock DbRefResolver resolver;

	@BeforeEach
	void beforeEach() {

		MongoCustomConversions conversions = new MongoCustomConversions();

		mappingContext = new MongoMappingContext();
		mappingContext.setApplicationContext(context);
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		mappingContext.getPersistentEntity(Address.class);

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();
	}

	@Test
	void convertsAddressCorrectly() {

		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		org.bson.Document document = new org.bson.Document();

		converter.write(address, document);

		assertThat(document.get("city").toString()).isEqualTo("New York");
		assertThat(document.get("s").toString()).isEqualTo("Broadway");
	}

	@Test // DATAMONGO-130
	void writesMapTypeCorrectly() {

		Map<Locale, String> map = Collections.singletonMap(Locale.US, "Foo");

		org.bson.Document document = new org.bson.Document();
		converter.write(map, document);

		assertThat(document.get(Locale.US.toString()).toString()).isEqualTo("Foo");
	}

	@Test // DATAMONGO-130
	void readsMapWithCustomKeyTypeCorrectly() {

		org.bson.Document mapObject = new org.bson.Document(Locale.US.toString(), "Value");
		org.bson.Document document = new org.bson.Document("map", mapObject);

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, document);
		assertThat(result.map.get(Locale.US)).isEqualTo("Value");
	}

	@Test // DATAMONGO-128
	void usesDocumentsStoredTypeIfSubtypeOfRequest() {

		org.bson.Document document = new org.bson.Document();
		document.put("birthDate", new Date());
		document.put(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(Contact.class, document)).isInstanceOf(Person.class);
	}

	@Test // DATAMONGO-128
	void ignoresDocumentsStoredTypeIfCompletelyDifferentTypeRequested() {

		org.bson.Document document = new org.bson.Document();
		document.put("birthDate", new Date());
		document.put(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(BirthDateContainer.class, document)).isInstanceOf(BirthDateContainer.class);
	}

	@Test
	void writesTypeDiscriminatorIntoRootObject() {

		Person person = new Person();

		org.bson.Document result = new org.bson.Document();
		converter.write(person, result);

		assertThat(result.containsKey(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY)).isTrue();
		assertThat(result.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY).toString()).isEqualTo(Person.class.getName());
	}

	@Test // DATAMONGO-136
	void writesEnumsCorrectly() {

		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.sampleEnum = SampleEnum.FIRST;

		org.bson.Document result = new org.bson.Document();
		converter.write(value, result);

		assertThat(result.get("sampleEnum")).isInstanceOf(String.class);
		assertThat(result.get("sampleEnum").toString()).isEqualTo("FIRST");
	}

	@Test // DATAMONGO-209
	void writesEnumCollectionCorrectly() {

		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.enums = Arrays.asList(SampleEnum.FIRST);

		org.bson.Document result = new org.bson.Document();
		converter.write(value, result);

		assertThat(result.get("enums")).isInstanceOf(List.class);

		List<Object> enums = (List<Object>) result.get("enums");
		assertThat(enums.size()).isEqualTo(1);
		assertThat(enums.get(0)).isEqualTo("FIRST");
	}

	@Test // DATAMONGO-136
	void readsEnumsCorrectly() {
		org.bson.Document document = new org.bson.Document("sampleEnum", "FIRST");
		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, document);

		assertThat(result.sampleEnum).isEqualTo(SampleEnum.FIRST);
	}

	@Test // DATAMONGO-209
	void readsEnumCollectionsCorrectly() {

		BasicDBList enums = new BasicDBList();
		enums.add("FIRST");
		org.bson.Document document = new org.bson.Document("enums", enums);

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, document);

		assertThat(result.enums).isInstanceOf(List.class);
		assertThat(result.enums.size()).isEqualTo(1);
		assertThat(result.enums).contains(SampleEnum.FIRST);
	}

	@Test // DATAMONGO-144
	void considersFieldNameWhenWriting() {

		Person person = new Person();
		person.firstname = "Oliver";

		org.bson.Document result = new org.bson.Document();
		converter.write(person, result);

		assertThat(result.containsKey("foo")).isTrue();
		assertThat(result.containsKey("firstname")).isFalse();
	}

	@Test // DATAMONGO-144
	void considersFieldNameWhenReading() {

		org.bson.Document document = new org.bson.Document("foo", "Oliver");
		Person result = converter.read(Person.class, document);

		assertThat(result.firstname).isEqualTo("Oliver");
	}

	@Test
	void resolvesNestedComplexTypeForConstructorCorrectly() {

		org.bson.Document address = new org.bson.Document("street", "110 Southwark Street");
		address.put("city", "London");

		BasicDBList addresses = new BasicDBList();
		addresses.add(address);

		org.bson.Document person = new org.bson.Document("firstname", "Oliver");
		person.put("addresses", addresses);

		Person result = converter.read(Person.class, person);
		assertThat(result.addresses).isNotNull();
	}

	@Test // DATAMONGO-145
	void writesCollectionWithInterfaceCorrectly() {

		Person person = new Person();
		person.firstname = "Oliver";

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.contacts = Arrays.asList((Contact) person);

		org.bson.Document document = new org.bson.Document();
		converter.write(wrapper, document);

		Object result = document.get("contacts");
		assertThat(result).isInstanceOf(List.class);
		List<Object> contacts = (List<Object>) result;
		org.bson.Document personDocument = (org.bson.Document) contacts.get(0);
		assertThat(personDocument.get("foo").toString()).isEqualTo("Oliver");
		assertThat((String) personDocument.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY)).isEqualTo(Person.class.getName());
	}

	@Test // DATAMONGO-145
	void readsCollectionWithInterfaceCorrectly() {

		org.bson.Document person = new org.bson.Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());
		person.put("foo", "Oliver");

		BasicDBList contacts = new BasicDBList();
		contacts.add(person);

		CollectionWrapper result = converter.read(CollectionWrapper.class, new org.bson.Document("contacts", contacts));
		assertThat(result.contacts).isNotNull();
		assertThat(result.contacts.size()).isEqualTo(1);
		Contact contact = result.contacts.get(0);
		assertThat(contact).isInstanceOf(Person.class);
		assertThat(((Person) contact).firstname).isEqualTo("Oliver");
	}

	@Test
	void convertsLocalesOutOfTheBox() {
		LocaleWrapper wrapper = new LocaleWrapper();
		wrapper.locale = Locale.US;

		org.bson.Document document = new org.bson.Document();
		converter.write(wrapper, document);

		Object localeField = document.get("locale");
		assertThat(localeField).isInstanceOf(String.class);
		assertThat(localeField).isEqualTo("en_US");

		LocaleWrapper read = converter.read(LocaleWrapper.class, document);
		assertThat(read.locale).isEqualTo(Locale.US);
	}

	@Test // DATAMONGO-161
	void readsNestedMapsCorrectly() {

		Map<String, String> secondLevel = new HashMap<String, String>();
		secondLevel.put("key1", "value1");
		secondLevel.put("key2", "value2");

		Map<String, Map<String, String>> firstLevel = new HashMap<String, Map<String, String>>();
		firstLevel.put("level1", secondLevel);
		firstLevel.put("level2", secondLevel);

		ClassWithNestedMaps maps = new ClassWithNestedMaps();
		maps.nestedMaps = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		maps.nestedMaps.put("afield", firstLevel);

		org.bson.Document document = new org.bson.Document();
		converter.write(maps, document);

		ClassWithNestedMaps result = converter.read(ClassWithNestedMaps.class, document);
		Map<String, Map<String, Map<String, String>>> nestedMap = result.nestedMaps;
		assertThat(nestedMap).isNotNull();
		assertThat(nestedMap.get("afield")).isEqualTo(firstLevel);
	}

	@Test // DATACMNS-42, DATAMONGO-171
	void writesClassWithBigDecimal() {

		BigDecimalContainer container = new BigDecimalContainer();
		container.value = BigDecimal.valueOf(2.5d);
		container.map = Collections.singletonMap("foo", container.value);

		org.bson.Document document = new org.bson.Document();
		converter.write(container, document);

		assertThat(document.get("value")).isInstanceOf(String.class);
		assertThat((String) document.get("value")).isEqualTo("2.5");
		assertThat(((org.bson.Document) document.get("map")).get("foo")).isInstanceOf(String.class);
	}

	@Test // DATACMNS-42, DATAMONGO-171
	void readsClassWithBigDecimal() {

		org.bson.Document document = new org.bson.Document("value", "2.5");
		document.put("map", new org.bson.Document("foo", "2.5"));

		BasicDBList list = new BasicDBList();
		list.add("2.5");
		document.put("collection", list);
		BigDecimalContainer result = converter.read(BigDecimalContainer.class, document);

		assertThat(result.value).isEqualTo(BigDecimal.valueOf(2.5d));
		assertThat(result.map.get("foo")).isEqualTo(BigDecimal.valueOf(2.5d));
		assertThat(result.collection.get(0)).isEqualTo(BigDecimal.valueOf(2.5d));
	}

	@Test
	void writesNestedCollectionsCorrectly() {

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.strings = Arrays.asList(Arrays.asList("Foo"));

		org.bson.Document document = new org.bson.Document();
		converter.write(wrapper, document);

		Object outerStrings = document.get("strings");
		assertThat(outerStrings).isInstanceOf(List.class);

		List<Object> typedOuterString = (List<Object>) outerStrings;
		assertThat(typedOuterString.size()).isEqualTo(1);
	}

	@Test // DATAMONGO-192
	void readsEmptySetsCorrectly() {

		Person person = new Person();
		person.addresses = Collections.emptySet();

		org.bson.Document document = new org.bson.Document();
		converter.write(person, document);
		converter.read(Person.class, document);
	}

	@Test
	void convertsObjectIdStringsToObjectIdCorrectly() {
		PersonPojoStringId p1 = new PersonPojoStringId("1234567890", "Text-1");
		org.bson.Document doc1 = new org.bson.Document();

		converter.write(p1, doc1);
		assertThat(doc1.get("_id")).isInstanceOf(String.class);

		PersonPojoStringId p2 = new PersonPojoStringId(new ObjectId().toString(), "Text-1");
		org.bson.Document doc2 = new org.bson.Document();

		converter.write(p2, doc2);
		assertThat(doc2.get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-207
	void convertsCustomEmptyMapCorrectly() {

		org.bson.Document map = new org.bson.Document();
		org.bson.Document wrapper = new org.bson.Document("map", map);

		ClassWithSortedMap result = converter.read(ClassWithSortedMap.class, wrapper);

		assertThat(result).isInstanceOf(ClassWithSortedMap.class);
		assertThat(result.map).isInstanceOf(SortedMap.class);
	}

	@Test // DATAMONGO-211
	void maybeConvertHandlesNullValuesCorrectly() {
		assertThat(converter.convertToMongoType(null)).isNull();
	}

	@Test // DATAMONGO-1509
	void writesGenericTypeCorrectly() {

		GenericType<Address> type = new GenericType<Address>();
		type.content = new Address();
		type.content.city = "London";

		org.bson.Document result = new org.bson.Document();
		converter.write(type, result);

		org.bson.Document content = (org.bson.Document) result.get("content");
		assertTypeHint(content, Address.class);
		assertThat(content.get("city")).isNotNull();
	}

	@Test
	void readsGenericTypeCorrectly() {

		org.bson.Document address = new org.bson.Document("_class", Address.class.getName());
		address.put("city", "London");

		GenericType<?> result = converter.read(GenericType.class, new org.bson.Document("content", address));
		assertThat(result.content).isInstanceOf(Address.class);
	}

	@Test // DATAMONGO-228
	void writesNullValuesForMaps() {

		ClassWithMapProperty foo = new ClassWithMapProperty();
		foo.map = Collections.singletonMap(Locale.US, null);

		org.bson.Document result = new org.bson.Document();
		converter.write(foo, result);

		Object map = result.get("map");
		assertThat(map).isInstanceOf(org.bson.Document.class);
		assertThat(((org.bson.Document) map).keySet()).contains("en_US");
	}

	@Test
	void writesBigIntegerIdCorrectly() {

		ClassWithBigIntegerId foo = new ClassWithBigIntegerId();
		foo.id = BigInteger.valueOf(23L);

		org.bson.Document result = new org.bson.Document();
		converter.write(foo, result);

		assertThat(result.get("_id")).isInstanceOf(String.class);
	}

	@Test
	void convertsObjectsIfNecessary() {

		ObjectId id = new ObjectId();
		assertThat(converter.convertToMongoType(id)).isEqualTo(id);
	}

	@Test // DATAMONGO-235
	void writesMapOfListsCorrectly() {

		ClassWithMapProperty input = new ClassWithMapProperty();
		input.mapOfLists = Collections.singletonMap("Foo", Arrays.asList("Bar"));

		org.bson.Document result = new org.bson.Document();
		converter.write(input, result);

		Object field = result.get("mapOfLists");
		assertThat(field).isInstanceOf(org.bson.Document.class);

		org.bson.Document map = (org.bson.Document) field;
		Object foo = map.get("Foo");
		assertThat(foo).isInstanceOf(List.class);

		List<Object> value = (List<Object>) foo;
		assertThat(value.size()).isEqualTo(1);
		assertThat(value.get(0)).isEqualTo("Bar");
	}

	@Test // DATAMONGO-235
	void readsMapListValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add("Bar");
		org.bson.Document source = new org.bson.Document("mapOfLists", new org.bson.Document("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		assertThat(result.mapOfLists).isNotNull();
	}

	@Test // DATAMONGO-235
	void writesMapsOfObjectsCorrectly() {

		ClassWithMapProperty input = new ClassWithMapProperty();
		input.mapOfObjects = new HashMap<String, Object>();
		input.mapOfObjects.put("Foo", Arrays.asList("Bar"));

		org.bson.Document result = new org.bson.Document();
		converter.write(input, result);

		Object field = result.get("mapOfObjects");
		assertThat(field).isInstanceOf(org.bson.Document.class);

		org.bson.Document map = (org.bson.Document) field;
		Object foo = map.get("Foo");
		assertThat(foo).isInstanceOf(List.class);

		List value = (List) foo;
		assertThat(value.size()).isEqualTo(1);
		assertThat(value.get(0)).isEqualTo("Bar");
	}

	@Test // DATAMONGO-235
	void readsMapOfObjectsListValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add("Bar");
		org.bson.Document source = new org.bson.Document("mapOfObjects", new org.bson.Document("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		assertThat(result.mapOfObjects).isNotNull();
	}

	@Test // DATAMONGO-245
	void readsMapListNestedValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add(new org.bson.Document("Hello", "World"));
		org.bson.Document source = new org.bson.Document("mapOfObjects", new org.bson.Document("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		Object firstObjectInFoo = ((List<?>) result.mapOfObjects.get("Foo")).get(0);
		assertThat(firstObjectInFoo).isInstanceOf(Map.class);
		assertThat(((Map<?, ?>) firstObjectInFoo).get("Hello")).isEqualTo("World");
	}

	@Test // DATAMONGO-245
	void readsMapDoublyNestedValuesCorrectly() {

		org.bson.Document nested = new org.bson.Document();
		org.bson.Document doubly = new org.bson.Document();
		doubly.append("Hello", "World");
		nested.append("nested", doubly);
		org.bson.Document source = new org.bson.Document("mapOfObjects", new org.bson.Document("Foo", nested));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		Object foo = result.mapOfObjects.get("Foo");
		assertThat(foo).isInstanceOf(Map.class);
		Object doublyNestedObject = ((Map<?, ?>) foo).get("nested");
		assertThat(doublyNestedObject).isInstanceOf(Map.class);
		assertThat(((Map<?, ?>) doublyNestedObject).get("Hello")).isEqualTo("World");
	}

	@Test // DATAMONGO-245
	void readsMapListDoublyNestedValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		org.bson.Document nested = new org.bson.Document();
		org.bson.Document doubly = new org.bson.Document();
		doubly.append("Hello", "World");
		nested.append("nested", doubly);
		list.add(nested);
		org.bson.Document source = new org.bson.Document("mapOfObjects", new org.bson.Document("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		Object firstObjectInFoo = ((List<?>) result.mapOfObjects.get("Foo")).get(0);
		assertThat(firstObjectInFoo).isInstanceOf(Map.class);
		Object doublyNestedObject = ((Map<?, ?>) firstObjectInFoo).get("nested");
		assertThat(doublyNestedObject).isInstanceOf(Map.class);
		assertThat(((Map<?, ?>) doublyNestedObject).get("Hello")).isEqualTo("World");
	}

	@Test // DATAMONGO-259
	void writesListOfMapsCorrectly() {

		Map<String, Locale> map = Collections.singletonMap("Foo", Locale.ENGLISH);

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.listOfMaps = new ArrayList<Map<String, Locale>>();
		wrapper.listOfMaps.add(map);

		org.bson.Document result = new org.bson.Document();
		converter.write(wrapper, result);

		List<Object> list = (List<Object>) result.get("listOfMaps");
		assertThat(list).isNotNull();
		assertThat(list.size()).isEqualTo(1);

		org.bson.Document document = (org.bson.Document) list.get(0);
		assertThat(document.containsKey("Foo")).isTrue();
		assertThat((String) document.get("Foo")).isEqualTo(Locale.ENGLISH.toString());
	}

	@Test // DATAMONGO-259
	void readsListOfMapsCorrectly() {

		org.bson.Document map = new org.bson.Document("Foo", "en");

		BasicDBList list = new BasicDBList();
		list.add(map);

		org.bson.Document wrapperSource = new org.bson.Document("listOfMaps", list);

		CollectionWrapper wrapper = converter.read(CollectionWrapper.class, wrapperSource);

		assertThat(wrapper.listOfMaps).isNotNull();
		assertThat(wrapper.listOfMaps.size()).isEqualTo(1);
		assertThat(wrapper.listOfMaps.get(0)).isNotNull();
		assertThat(wrapper.listOfMaps.get(0).get("Foo")).isEqualTo(Locale.ENGLISH);
	}

	@Test // DATAMONGO-259
	void writesPlainMapOfCollectionsCorrectly() {

		Map<String, List<Locale>> map = Collections.singletonMap("Foo", Arrays.asList(Locale.US));
		org.bson.Document result = new org.bson.Document();
		converter.write(map, result);

		assertThat(result.containsKey("Foo")).isTrue();
		assertThat(result.get("Foo")).isNotNull();
		assertThat(result.get("Foo")).isInstanceOf(List.class);

		List list = (List) result.get("Foo");

		assertThat(list.size()).isEqualTo(1);
		assertThat(list.get(0)).isEqualTo(Locale.US.toString());
	}

	@Test // DATAMONGO-285
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void testSaveMapWithACollectionAsValue() {

		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("string", "hello");
		List<String> list = new ArrayList<String>();
		list.add("ping");
		list.add("pong");
		keyValues.put("list", list);

		org.bson.Document document = new org.bson.Document();
		converter.write(keyValues, document);

		Map<String, Object> keyValuesFromMongo = converter.read(Map.class, document);

		assertThat(keyValuesFromMongo.size()).isEqualTo(keyValues.size());
		assertThat(keyValuesFromMongo.get("string")).isEqualTo(keyValues.get("string"));
		assertThat(List.class.isAssignableFrom(keyValuesFromMongo.get("list").getClass())).isTrue();
		List<String> listFromMongo = (List) keyValuesFromMongo.get("list");
		assertThat(listFromMongo.size()).isEqualTo(list.size());
		assertThat(listFromMongo.get(0)).isEqualTo(list.get(0));
		assertThat(listFromMongo.get(1)).isEqualTo(list.get(1));
	}

	@Test // DATAMONGO-309
	@SuppressWarnings({ "unchecked" })
	void writesArraysAsMapValuesCorrectly() {

		ClassWithMapProperty wrapper = new ClassWithMapProperty();
		wrapper.mapOfObjects = new HashMap<String, Object>();
		wrapper.mapOfObjects.put("foo", new String[] { "bar" });

		org.bson.Document result = new org.bson.Document();
		converter.write(wrapper, result);

		Object mapObject = result.get("mapOfObjects");
		assertThat(mapObject).isInstanceOf(org.bson.Document.class);

		org.bson.Document map = (org.bson.Document) mapObject;
		Object valueObject = map.get("foo");
		assertThat(valueObject).isInstanceOf(List.class);

		List<Object> list = (List<Object>) valueObject;
		assertThat(list.size()).isEqualTo(1);
		assertThat(list).contains((Object) "bar");
	}

	@Test // DATAMONGO-324
	void writesDocumentCorrectly() {

		org.bson.Document document = new org.bson.Document();
		document.put("foo", "bar");

		org.bson.Document result = new org.bson.Document();

		converter.write(document, result);

		result.remove(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);
		assertThat(document).isEqualTo(result);
	}

	@Test // DATAMONGO-324
	void readsDocumentCorrectly() {

		org.bson.Document document = new org.bson.Document();
		document.put("foo", "bar");

		org.bson.Document result = converter.read(org.bson.Document.class, document);

		assertThat(result).isEqualTo(document);
	}

	@Test // DATAMONGO-329
	void writesMapAsGenericFieldCorrectly() {

		Map<String, A<String>> objectToSave = new HashMap<String, A<String>>();
		objectToSave.put("test", new A<String>("testValue"));

		A<Map<String, A<String>>> a = new A<Map<String, A<String>>>(objectToSave);
		org.bson.Document result = new org.bson.Document();

		converter.write(a, result);

		assertThat(result.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY)).isEqualTo(A.class.getName());
		assertThat(result.get("valueType")).isEqualTo(HashMap.class.getName());

		org.bson.Document object = (org.bson.Document) result.get("value");
		assertThat(object).isNotNull();

		org.bson.Document inner = (org.bson.Document) object.get("test");
		assertThat(inner).isNotNull();
		assertThat(inner.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY)).isEqualTo(A.class.getName());
		assertThat(inner.get("valueType")).isEqualTo(String.class.getName());
		assertThat(inner.get("value")).isEqualTo("testValue");
	}

	@Test
	void writesIntIdCorrectly() {

		ClassWithIntId value = new ClassWithIntId();
		value.id = 5;

		org.bson.Document result = new org.bson.Document();
		converter.write(value, result);

		assertThat(result.get("_id")).isEqualTo(5);
	}

	@Test // DATAMONGO-368
	@SuppressWarnings("unchecked")
	void writesNullValuesForCollection() {

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.contacts = Arrays.asList(new Person(), null);

		org.bson.Document result = new org.bson.Document();
		converter.write(wrapper, result);

		Object contacts = result.get("contacts");
		assertThat(contacts).isInstanceOf(Collection.class);
		assertThat(((Collection<?>) contacts).size()).isEqualTo(2);
		assertThat((Collection<Object>) contacts).containsNull();
	}

	@Test // DATAMONGO-379
	void considersDefaultingExpressionsAtConstructorArguments() {

		org.bson.Document document = new org.bson.Document("foo", "bar");
		document.put("foobar", 2.5);

		DefaultedConstructorArgument result = converter.read(DefaultedConstructorArgument.class, document);
		assertThat(result.bar).isEqualTo(-1);
	}

	@Test // DATAMONGO-379
	void usesDocumentFieldIfReferencedInAtValue() {

		org.bson.Document document = new org.bson.Document("foo", "bar");
		document.put("something", 37);
		document.put("foobar", 2.5);

		DefaultedConstructorArgument result = converter.read(DefaultedConstructorArgument.class, document);
		assertThat(result.bar).isEqualTo(37);
	}

	@Test // DATAMONGO-379
	void rejectsNotFoundConstructorParameterForPrimitiveType() {

		org.bson.Document document = new org.bson.Document("foo", "bar");

		assertThatThrownBy(() -> converter.read(DefaultedConstructorArgument.class, document))
				.isInstanceOf(MappingInstantiationException.class);
	}

	@Test // DATAMONGO-358
	void writesListForObjectPropertyCorrectly() {

		Attribute attribute = new Attribute();
		attribute.key = "key";
		attribute.value = Arrays.asList("1", "2");

		Item item = new Item();
		item.attributes = Arrays.asList(attribute);

		org.bson.Document result = new org.bson.Document();

		converter.write(item, result);

		Item read = converter.read(Item.class, result);
		assertThat(read.attributes.size()).isEqualTo(1);
		assertThat(read.attributes.get(0).key).isEqualTo(attribute.key);
		assertThat(read.attributes.get(0).value).isInstanceOf(Collection.class);

		@SuppressWarnings("unchecked")
		Collection<String> values = (Collection<String>) read.attributes.get(0).value;

		assertThat(values.size()).isEqualTo(2);
		assertThat(values).contains("1", "2");
	}

	@Test // DATAMONGO-380
	void rejectsMapWithKeyContainingDotsByDefault() {
		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> converter.write(Collections.singletonMap("foo.bar", "foobar"), new org.bson.Document()));
	}

	@Test // DATAMONGO-380
	void escapesDotInMapKeysIfReplacementConfigured() {

		converter.setMapKeyDotReplacement("~");

		org.bson.Document document = new org.bson.Document();
		converter.write(Collections.singletonMap("foo.bar", "foobar"), document);

		assertThat((String) document.get("foo~bar")).isEqualTo("foobar");
		assertThat(document.containsKey("foo.bar")).isFalse();
	}

	@Test // DATAMONGO-380
	@SuppressWarnings("unchecked")
	void unescapesDotInMapKeysIfReplacementConfigured() {

		converter.setMapKeyDotReplacement("~");

		org.bson.Document document = new org.bson.Document("foo~bar", "foobar");
		Map<String, String> result = converter.read(Map.class, document);

		assertThat(result.get("foo.bar")).isEqualTo("foobar");
		assertThat(result.containsKey("foobar")).isFalse();
	}

	@Test // DATAMONGO-382
	@Disabled("mongo3 - no longer supported")
	void convertsSetToBasicDBList() {

		Address address = new Address();
		address.city = "London";
		address.street = "Foo";

		Object result = converter.convertToMongoType(Collections.singleton(address), ClassTypeInformation.OBJECT);
		assertThat(result).isInstanceOf(List.class);

		Set<?> readResult = converter.read(Set.class, (org.bson.Document) result);
		assertThat(readResult.size()).isEqualTo(1);
		assertThat(readResult.iterator().next()).isInstanceOf(Address.class);
	}

	@Test // DATAMONGO-402, GH-3702
	void readsMemberClassCorrectly() {

		org.bson.Document document = new org.bson.Document("inner",
				new LinkedHashMap<>(new org.bson.Document("value", "FOO!")));

		Outer outer = converter.read(Outer.class, document);
		assertThat(outer.inner).isNotNull();
		assertThat(outer.inner.value).isEqualTo("FOO!");
		assertSyntheticFieldValueOf(outer.inner, outer);
	}

	@Test // DATAMONGO-458
	void readEmptyCollectionIsModifiable() {

		org.bson.Document document = new org.bson.Document("contactsSet", new BasicDBList());
		CollectionWrapper wrapper = converter.read(CollectionWrapper.class, document);

		assertThat(wrapper.contactsSet).isNotNull();
		wrapper.contactsSet.add(new Contact() {});
	}

	@Test // DATAMONGO-424
	void readsPlainDBRefObject() {

		DBRef dbRef = new DBRef("foo", 2);
		org.bson.Document document = new org.bson.Document("ref", dbRef);

		DBRefWrapper result = converter.read(DBRefWrapper.class, document);
		assertThat(result.ref).isEqualTo(dbRef);
	}

	@Test // DATAMONGO-424
	void readsCollectionOfDBRefs() {

		DBRef dbRef = new DBRef("foo", 2);
		BasicDBList refs = new BasicDBList();
		refs.add(dbRef);

		org.bson.Document document = new org.bson.Document("refs", refs);

		DBRefWrapper result = converter.read(DBRefWrapper.class, document);
		assertThat(result.refs).hasSize(1);
		assertThat(result.refs).contains(dbRef);
	}

	@Test // DATAMONGO-424
	void readsDBRefMap() {

		DBRef dbRef = mock(DBRef.class);
		org.bson.Document refMap = new org.bson.Document("foo", dbRef);
		org.bson.Document document = new org.bson.Document("refMap", refMap);

		DBRefWrapper result = converter.read(DBRefWrapper.class, document);

		assertThat(result.refMap.entrySet()).hasSize(1);
		assertThat(result.refMap.values()).contains(dbRef);
	}

	@Test // DATAMONGO-424
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void resolvesDBRefMapValue() {

		when(resolver.fetch(Mockito.any(DBRef.class))).thenReturn(new org.bson.Document());
		DBRef dbRef = mock(DBRef.class);

		org.bson.Document refMap = new org.bson.Document("foo", dbRef);
		org.bson.Document document = new org.bson.Document("personMap", refMap);

		DBRefWrapper result = converter.read(DBRefWrapper.class, document);

		assertThat(result.personMap.entrySet()).hasSize(1);
		assertThat(result.personMap.values()).anyMatch(Person.class::isInstance);
	}

	@Test // DATAMONGO-462
	void writesURLsAsStringOutOfTheBox() throws Exception {

		URLWrapper wrapper = new URLWrapper();
		wrapper.url = new URL("https://springsource.org");
		org.bson.Document sink = new org.bson.Document();

		converter.write(wrapper, sink);

		assertThat(sink.get("url")).isEqualTo("https://springsource.org");
	}

	@Test // DATAMONGO-462
	void readsURLFromStringOutOfTheBox() throws Exception {
		org.bson.Document document = new org.bson.Document("url", "https://springsource.org");
		URLWrapper result = converter.read(URLWrapper.class, document);
		assertThat(result.url).isEqualTo(new URL("https://springsource.org"));
	}

	@Test // DATAMONGO-485
	void writesComplexIdCorrectly() {

		ComplexId id = new ComplexId();
		id.innerId = 4711L;

		ClassWithComplexId entity = new ClassWithComplexId();
		entity.complexId = id;

		org.bson.Document document = new org.bson.Document();
		converter.write(entity, document);

		Object idField = document.get("_id");
		assertThat(idField).isNotNull();
		assertThat(idField).isInstanceOf(org.bson.Document.class);
		assertThat(((org.bson.Document) idField).get("innerId")).isEqualTo(4711L);
	}

	@Test // DATAMONGO-485
	void readsComplexIdCorrectly() {

		org.bson.Document innerId = new org.bson.Document("innerId", 4711L);
		org.bson.Document entity = new org.bson.Document("_id", innerId);

		ClassWithComplexId result = converter.read(ClassWithComplexId.class, entity);

		assertThat(result.complexId).isNotNull();
		assertThat(result.complexId.innerId).isEqualTo(4711L);
	}

	@Test // DATAMONGO-489
	void readsArraysAsMapValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add("Foo");
		list.add("Bar");

		org.bson.Document map = new org.bson.Document("key", list);
		org.bson.Document wrapper = new org.bson.Document("mapOfStrings", map);

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, wrapper);
		assertThat(result.mapOfStrings).isNotNull();

		String[] values = result.mapOfStrings.get("key");
		assertThat(values).isNotNull();
		assertThat(values).hasSize(2);
	}

	@Test // DATAMONGO-497
	void readsEmptyCollectionIntoConstructorCorrectly() {

		org.bson.Document source = new org.bson.Document("attributes", new BasicDBList());

		TypWithCollectionConstructor result = converter.read(TypWithCollectionConstructor.class, source);
		assertThat(result.attributes).isNotNull();
	}

	@Test // DATAMONGO-2400
	void writeJavaTimeValuesViaCodec() {

		configureConverterWithNativeJavaTimeCodec();
		TypeWithLocalDateTime source = new TypeWithLocalDateTime();

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target).containsEntry("date", source.date);
	}

	void configureConverterWithNativeJavaTimeCodec() {

		converter = new MappingMongoConverter(resolver, mappingContext);

		converter.setCustomConversions(MongoCustomConversions
				.create(MongoCustomConversions.MongoConverterConfigurationAdapter::useNativeDriverJavaTimeCodecs));
		converter.afterPropertiesSet();
	}

	private static void assertSyntheticFieldValueOf(Object target, Object expected) {

		for (int i = 0; i < 10; i++) {
			try {
				assertThat(ReflectionTestUtils.getField(target, "this$" + i)).isEqualTo(expected);
				return;
			} catch (IllegalArgumentException e) {
				// Suppress and try next
			}
		}

		fail(String.format("Didn't find synthetic field on %s!", target));
	}

	@Test // DATAMGONGO-508
	void eagerlyReturnsDBRefObjectIfTargetAlreadyIsOne() {

		DBRef dbRef = new DBRef("collection", "id");

		MongoPersistentProperty property = mock(MongoPersistentProperty.class);

		assertThat(converter.createDBRef(dbRef, property)).isEqualTo(dbRef);
	}

	@Test // DATAMONGO-523, DATAMONGO-1509
	void considersTypeAliasAnnotation() {

		Aliased aliased = new Aliased();
		aliased.name = "foo";

		org.bson.Document result = new org.bson.Document();
		converter.write(aliased, result);

		assertTypeHint(result, "_");
	}

	@Test // DATAMONGO-533
	void marshalsThrowableCorrectly() {

		ThrowableWrapper wrapper = new ThrowableWrapper();
		wrapper.throwable = new Exception();

		org.bson.Document document = new org.bson.Document();
		converter.write(wrapper, document);
	}

	@Test // DATAMONGO-592
	void recursivelyConvertsSpELReadValue() {

		org.bson.Document input = org.bson.Document.parse(
				"{ \"_id\" : { \"$oid\" : \"50ca271c4566a2b08f2d667a\" }, \"_class\" : \"com.recorder.TestRecorder2$ObjectContainer\", \"property\" : { \"property\" : 100 } }");

		converter.read(ObjectContainer.class, input);
	}

	@Test // DATAMONGO-724
	void mappingConsidersCustomConvertersNotWritingTypeInformation() {

		Person person = new Person();
		person.firstname = "Dave";

		ClassWithMapProperty entity = new ClassWithMapProperty();
		entity.mapOfPersons = new HashMap<String, Person>();
		entity.mapOfPersons.put("foo", person);
		entity.mapOfObjects = new HashMap<String, Object>();
		entity.mapOfObjects.put("foo", person);

		CustomConversions conversions = new MongoCustomConversions(
				Arrays.asList(new Converter<Person, org.bson.Document>() {

					@Override
					public org.bson.Document convert(Person source) {
						return new org.bson.Document().append("firstname", source.firstname)//
								.append("_class", Person.class.getName());
					}

				}, new Converter<org.bson.Document, Person>() {

					@Override
					public Person convert(org.bson.Document source) {
						Person person = new Person();
						person.firstname = source.get("firstname").toString();
						person.lastname = "converter";
						return person;
					}
				}));

		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		MappingMongoConverter mongoConverter = new MappingMongoConverter(resolver, context);
		mongoConverter.setCustomConversions(conversions);
		mongoConverter.afterPropertiesSet();

		org.bson.Document document = new org.bson.Document();
		mongoConverter.write(entity, document);

		ClassWithMapProperty result = mongoConverter.read(ClassWithMapProperty.class, document);

		assertThat(result.mapOfPersons).isNotNull();
		Person personCandidate = result.mapOfPersons.get("foo");
		assertThat(personCandidate).isNotNull();
		assertThat(personCandidate.firstname).isEqualTo("Dave");

		assertThat(result.mapOfObjects).isNotNull();
		Object value = result.mapOfObjects.get("foo");
		assertThat(value).isNotNull();
		assertThat(value).isInstanceOf(Person.class);
		assertThat(((Person) value).firstname).isEqualTo("Dave");
		assertThat(((Person) value).lastname).isEqualTo("converter");
	}

	@Test // DATAMONGO-743, DATAMONGO-2198
	void readsIntoStringsOutOfTheBox() {

		String target = converter.read(String.class, new org.bson.Document("firstname", "Dave"));

		assertThat(target).startsWith("{");
		assertThat(target).endsWith("}");
		assertThat(target).contains("\"firstname\"");
		assertThat(target).contains("\"Dave\"");
	}

	@Test // DATAMONGO-766
	void writesProjectingTypeCorrectly() {

		NestedType nested = new NestedType();
		nested.c = "C";

		ProjectingType type = new ProjectingType();
		type.name = "name";
		type.foo = "bar";
		type.a = nested;

		org.bson.Document result = new org.bson.Document();
		converter.write(type, result);

		assertThat(result.get("name")).isEqualTo((Object) "name");
		org.bson.Document aValue = DocumentTestUtils.getAsDocument(result, "a");
		assertThat(aValue.get("b")).isEqualTo((Object) "bar");
		assertThat(aValue.get("c")).isEqualTo((Object) "C");
	}

	@Test // DATAMONGO-812, DATAMONGO-893, DATAMONGO-1509
	void convertsListToBasicDBListAndRetainsTypeInformationForComplexObjects() {

		Address address = new Address();
		address.city = "London";
		address.street = "Foo";

		Object result = converter.convertToMongoType(Collections.singletonList(address),
				ClassTypeInformation.from(InterfaceType.class));

		assertThat(result).isInstanceOf(List.class);

		List<Object> dbList = (List<Object>) result;
		assertThat(dbList).hasSize(1);
		assertTypeHint(getAsDocument(dbList, 0), Address.class);
	}

	@Test // DATAMONGO-812
	void convertsListToBasicDBListWithoutTypeInformationForSimpleTypes() {

		Object result = converter.convertToMongoType(Collections.singletonList("foo"));

		assertThat(result).isInstanceOf(List.class);

		List<Object> dbList = (List<Object>) result;
		assertThat(dbList).hasSize(1);
		assertThat(dbList.get(0)).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-812, DATAMONGO-1509
	void convertsArrayToBasicDBListAndRetainsTypeInformationForComplexObjects() {

		Address address = new Address();
		address.city = "London";
		address.street = "Foo";

		Object result = converter.convertToMongoType(new Address[] { address }, ClassTypeInformation.OBJECT);

		assertThat(result).isInstanceOf(List.class);

		List<Object> dbList = (List<Object>) result;
		assertThat(dbList).hasSize(1);
		assertTypeHint(getAsDocument(dbList, 0), Address.class);
	}

	@Test // DATAMONGO-812
	void convertsArrayToBasicDBListWithoutTypeInformationForSimpleTypes() {

		Object result = converter.convertToMongoType(new String[] { "foo" });

		assertThat(result).isInstanceOf(List.class);

		List<Object> dbList = (List<Object>) result;
		assertThat(dbList).hasSize(1);
		assertThat(dbList.get(0)).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-833
	void readsEnumSetCorrectly() {

		BasicDBList enumSet = new BasicDBList();
		enumSet.add("SECOND");
		org.bson.Document document = new org.bson.Document("enumSet", enumSet);

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, document);

		assertThat(result.enumSet).isInstanceOf(EnumSet.class);
		assertThat(result.enumSet.size()).isEqualTo(1);
		assertThat(result.enumSet).contains(SampleEnum.SECOND);
	}

	@Test // DATAMONGO-833
	void readsEnumMapCorrectly() {

		org.bson.Document enumMap = new org.bson.Document("FIRST", "Dave");
		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class,
				new org.bson.Document("enumMap", enumMap));

		assertThat(result.enumMap).isInstanceOf(EnumMap.class);
		assertThat(result.enumMap.size()).isEqualTo(1);
		assertThat(result.enumMap.get(SampleEnum.FIRST)).isEqualTo("Dave");
	}

	@Test // DATAMONGO-887
	void readsTreeMapCorrectly() {

		org.bson.Document person = new org.bson.Document("foo", "Dave");
		org.bson.Document treeMapOfPerson = new org.bson.Document("key", person);
		org.bson.Document document = new org.bson.Document("treeMapOfPersons", treeMapOfPerson);

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, document);

		assertThat(result.treeMapOfPersons).isNotNull();
		assertThat(result.treeMapOfPersons.get("key")).isNotNull();
		assertThat(result.treeMapOfPersons.get("key").firstname).isEqualTo("Dave");
	}

	@Test // DATAMONGO-887
	void writesTreeMapCorrectly() {

		Person person = new Person();
		person.firstname = "Dave";

		ClassWithMapProperty source = new ClassWithMapProperty();
		source.treeMapOfPersons = new TreeMap<String, Person>();
		source.treeMapOfPersons.put("key", person);

		org.bson.Document result = new org.bson.Document();

		converter.write(source, result);

		org.bson.Document map = getAsDocument(result, "treeMapOfPersons");
		org.bson.Document entry = getAsDocument(map, "key");
		assertThat(entry.get("foo")).isEqualTo("Dave");
	}

	@Test // DATAMONGO-858
	void shouldWriteEntityWithGeoBoxCorrectly() {

		ClassWithGeoBox object = new ClassWithGeoBox();
		object.box = new Box(new Point(1, 2), new Point(3, 4));

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		assertThat(document).isNotNull();
		assertThat(document.get("box")).isInstanceOf(org.bson.Document.class);
		assertThat(document.get("box")).isEqualTo((Object) new org.bson.Document()
				.append("first", toDocument(object.box.getFirst())).append("second", toDocument(object.box.getSecond())));
	}

	private static org.bson.Document toDocument(Point point) {
		return new org.bson.Document("x", point.getX()).append("y", point.getY());
	}

	@Test // DATAMONGO-858
	void shouldReadEntityWithGeoBoxCorrectly() {

		ClassWithGeoBox object = new ClassWithGeoBox();
		object.box = new Box(new Point(1, 2), new Point(3, 4));

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		ClassWithGeoBox result = converter.read(ClassWithGeoBox.class, document);

		assertThat(result).isNotNull();
		assertThat(result.box).isEqualTo(object.box);
	}

	@Test // DATAMONGO-858
	void shouldWriteEntityWithGeoPolygonCorrectly() {

		ClassWithGeoPolygon object = new ClassWithGeoPolygon();
		object.polygon = new Polygon(new Point(1, 2), new Point(3, 4), new Point(4, 5));

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		assertThat(document).isNotNull();

		assertThat(document.get("polygon")).isInstanceOf(org.bson.Document.class);
		org.bson.Document polygonDoc = (org.bson.Document) document.get("polygon");

		@SuppressWarnings("unchecked")
		List<org.bson.Document> points = (List<org.bson.Document>) polygonDoc.get("points");

		assertThat(points).hasSize(3);
		assertThat(points).contains(toDocument(object.polygon.getPoints().get(0)),
				toDocument(object.polygon.getPoints().get(1)), toDocument(object.polygon.getPoints().get(2)));
	}

	@Test // DATAMONGO-858
	void shouldReadEntityWithGeoPolygonCorrectly() {

		ClassWithGeoPolygon object = new ClassWithGeoPolygon();
		object.polygon = new Polygon(new Point(1, 2), new Point(3, 4), new Point(4, 5));

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		ClassWithGeoPolygon result = converter.read(ClassWithGeoPolygon.class, document);

		assertThat(result).isNotNull();
		assertThat(result.polygon).isEqualTo(object.polygon);
	}

	@Test // DATAMONGO-858
	void shouldWriteEntityWithGeoCircleCorrectly() {

		ClassWithGeoCircle object = new ClassWithGeoCircle();
		Circle circle = new Circle(new Point(1, 2), 3);
		Distance radius = circle.getRadius();
		object.circle = circle;

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		assertThat(document).isNotNull();
		assertThat(document.get("circle")).isInstanceOf(org.bson.Document.class);
		assertThat(document.get("circle")).isEqualTo((Object) new org.bson.Document("center",
				new org.bson.Document("x", circle.getCenter().getX()).append("y", circle.getCenter().getY()))
						.append("radius", radius.getNormalizedValue()).append("metric", radius.getMetric().toString()));
	}

	@Test // DATAMONGO-858
	void shouldReadEntityWithGeoCircleCorrectly() {

		ClassWithGeoCircle object = new ClassWithGeoCircle();
		object.circle = new Circle(new Point(1, 2), 3);

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		ClassWithGeoCircle result = converter.read(ClassWithGeoCircle.class, document);

		assertThat(result).isNotNull();
		assertThat(result.circle).isEqualTo(result.circle);
	}

	@Test // DATAMONGO-858
	void shouldWriteEntityWithGeoSphereCorrectly() {

		ClassWithGeoSphere object = new ClassWithGeoSphere();
		Sphere sphere = new Sphere(new Point(1, 2), 3);
		Distance radius = sphere.getRadius();
		object.sphere = sphere;

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		assertThat(document).isNotNull();
		assertThat(document.get("sphere")).isInstanceOf(org.bson.Document.class);
		assertThat(document.get("sphere")).isEqualTo((Object) new org.bson.Document("center",
				new org.bson.Document("x", sphere.getCenter().getX()).append("y", sphere.getCenter().getY()))
						.append("radius", radius.getNormalizedValue()).append("metric", radius.getMetric().toString()));
	}

	@Test // DATAMONGO-858
	void shouldWriteEntityWithGeoSphereWithMetricDistanceCorrectly() {

		ClassWithGeoSphere object = new ClassWithGeoSphere();
		Sphere sphere = new Sphere(new Point(1, 2), new Distance(3, Metrics.KILOMETERS));
		Distance radius = sphere.getRadius();
		object.sphere = sphere;

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		assertThat(document).isNotNull();
		assertThat(document.get("sphere")).isInstanceOf(org.bson.Document.class);
		assertThat(document.get("sphere")).isEqualTo((Object) new org.bson.Document("center",
				new org.bson.Document("x", sphere.getCenter().getX()).append("y", sphere.getCenter().getY()))
						.append("radius", radius.getNormalizedValue()).append("metric", radius.getMetric().toString()));
	}

	@Test // DATAMONGO-858
	void shouldReadEntityWithGeoSphereCorrectly() {

		ClassWithGeoSphere object = new ClassWithGeoSphere();
		object.sphere = new Sphere(new Point(1, 2), 3);

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		ClassWithGeoSphere result = converter.read(ClassWithGeoSphere.class, document);

		assertThat(result).isNotNull();
		assertThat(result.sphere).isEqualTo(object.sphere);
	}

	@Test // DATAMONGO-858
	void shouldWriteEntityWithGeoShapeCorrectly() {

		ClassWithGeoShape object = new ClassWithGeoShape();
		Sphere sphere = new Sphere(new Point(1, 2), 3);
		Distance radius = sphere.getRadius();
		object.shape = sphere;

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		assertThat(document).isNotNull();
		assertThat(document.get("shape")).isInstanceOf(org.bson.Document.class);
		assertThat(document.get("shape")).isEqualTo((Object) new org.bson.Document("center",
				new org.bson.Document("x", sphere.getCenter().getX()).append("y", sphere.getCenter().getY()))
						.append("radius", radius.getNormalizedValue()).append("metric", radius.getMetric().toString()));
	}

	@Test // DATAMONGO-858
	@Disabled
	void shouldReadEntityWithGeoShapeCorrectly() {

		ClassWithGeoShape object = new ClassWithGeoShape();
		Sphere sphere = new Sphere(new Point(1, 2), 3);
		object.shape = sphere;

		org.bson.Document document = new org.bson.Document();
		converter.write(object, document);

		ClassWithGeoShape result = converter.read(ClassWithGeoShape.class, document);

		assertThat(result).isNotNull();
		assertThat(result.shape).isEqualTo(sphere);
	}

	@Test // DATAMONGO-976
	void shouldIgnoreTextScorePropertyWhenWriting() {

		ClassWithTextScoreProperty source = new ClassWithTextScoreProperty();
		source.score = Float.MAX_VALUE;

		org.bson.Document document = new org.bson.Document();
		converter.write(source, document);

		assertThat(document.get("score")).isNull();
	}

	@Test // DATAMONGO-976
	void shouldIncludeTextScorePropertyWhenReading() {

		ClassWithTextScoreProperty entity = converter.read(ClassWithTextScoreProperty.class,
				new org.bson.Document("score", 5F));
		assertThat(entity.score).isEqualTo(5F);
	}

	@Test // DATAMONGO-1001, DATAMONGO-1509
	void shouldWriteCglibProxiedClassTypeInformationCorrectly() {

		ProxyFactory factory = new ProxyFactory();
		factory.setTargetClass(GenericType.class);
		factory.setProxyTargetClass(true);

		GenericType<?> proxied = (GenericType<?>) factory.getProxy();
		org.bson.Document document = new org.bson.Document();
		converter.write(proxied, document);

		assertTypeHint(document, GenericType.class);
	}

	@Test // DATAMONGO-1001
	void shouldUseTargetObjectOfLazyLoadingProxyWhenWriting() {

		LazyLoadingProxy mock = mock(LazyLoadingProxy.class);

		org.bson.Document document = new org.bson.Document();
		converter.write(mock, document);

		verify(mock, times(1)).getTarget();
	}

	@Test // DATAMONGO-1034
	void rejectsBasicDbListToBeConvertedIntoComplexType() {

		List<Object> inner = new ArrayList<Object>();
		inner.add("key");
		inner.add("value");

		List<Object> outer = new ArrayList<Object>();
		outer.add(inner);
		outer.add(inner);

		org.bson.Document source = new org.bson.Document("attributes", outer);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> converter.read(Item.class, source));
	}

	@Test // DATAMONGO-1058
	void readShouldRespectExplicitFieldNameForDbRef() {

		org.bson.Document source = new org.bson.Document();
		source.append("explict-name-for-db-ref", new DBRef("foo", "1"));

		converter.read(ClassWithExplicitlyNamedDBRefProperty.class, source);

		verify(resolver, times(1)).resolveDbRef(Mockito.any(MongoPersistentProperty.class), Mockito.any(DBRef.class),
				Mockito.any(DbRefResolverCallback.class), Mockito.any(DbRefProxyHandler.class));
	}

	@Test // DATAMONGO-1050
	void writeShouldUseExplicitFieldnameForIdPropertyWhenAnnotated() {

		RootForClassWithExplicitlyRenamedIdField source = new RootForClassWithExplicitlyRenamedIdField();
		source.id = "rootId";
		source.nested = new ClassWithExplicitlyRenamedField();
		source.nested.id = "nestedId";

		org.bson.Document sink = new org.bson.Document();
		converter.write(source, sink);

		assertThat(sink.get("_id")).isEqualTo("rootId");
		assertThat(sink.get("nested")).isEqualTo(new org.bson.Document().append("id", "nestedId"));
	}

	@Test // DATAMONGO-1050
	void readShouldUseExplicitFieldnameForIdPropertyWhenAnnotated() {

		org.bson.Document source = new org.bson.Document().append("_id", "rootId").append("nested",
				new org.bson.Document("id", "nestedId"));

		RootForClassWithExplicitlyRenamedIdField sink = converter.read(RootForClassWithExplicitlyRenamedIdField.class,
				source);

		assertThat(sink.id).isEqualTo("rootId");
		assertThat(sink.nested).isNotNull();
		assertThat(sink.nested.id).isEqualTo("nestedId");
	}

	@Test // DATAMONGO-1050
	void namedIdFieldShouldExtractValueFromUnderscoreIdField() {

		org.bson.Document document = new org.bson.Document().append("_id", "A").append("id", "B");

		ClassWithNamedIdField withNamedIdField = converter.read(ClassWithNamedIdField.class, document);

		assertThat(withNamedIdField.id).isEqualTo("A");
	}

	@Test // DATAMONGO-1050
	void explicitlyRenamedIfFieldShouldExtractValueFromIdField() {

		org.bson.Document document = new org.bson.Document().append("_id", "A").append("id", "B");

		ClassWithExplicitlyRenamedField withExplicitlyRenamedField = converter.read(ClassWithExplicitlyRenamedField.class,
				document);

		assertThat(withExplicitlyRenamedField.id).isEqualTo("B");
	}

	@Test // DATAMONGO-1050
	void annotatedIdFieldShouldExtractValueFromUnderscoreIdField() {

		org.bson.Document document = new org.bson.Document().append("_id", "A").append("id", "B");

		ClassWithAnnotatedIdField withAnnotatedIdField = converter.read(ClassWithAnnotatedIdField.class, document);

		assertThat(withAnnotatedIdField.key).isEqualTo("A");
	}

	@Test // DATAMONGO-1102
	void convertsJava8DateTimeTypesToDateAndBack() {

		TypeWithLocalDateTime source = new TypeWithLocalDateTime();
		LocalDateTime reference = source.date;
		org.bson.Document result = new org.bson.Document();

		converter.write(source, result);

		assertThat(result.get("date")).isInstanceOf(Date.class);
		assertThat(converter.read(TypeWithLocalDateTime.class, result).date)
				.isEqualTo(reference.truncatedTo(ChronoUnit.MILLIS));
	}

	@Test // DATAMONGO-1128
	@Disabled("really we should find a solution for this")
	void writesOptionalsCorrectly() {

		TypeWithOptional type = new TypeWithOptional();
		type.localDateTime = Optional.of(LocalDateTime.now());

		org.bson.Document result = new org.bson.Document();

		converter.write(type, result);

		assertThat(getAsDocument(result, "string")).isEqualTo(new org.bson.Document());

		org.bson.Document localDateTime = getAsDocument(result, "localDateTime");
		assertThat(localDateTime.get("value")).isInstanceOf(Date.class);
	}

	@Test // DATAMONGO-1128
	@Disabled("Broken by DATAMONGO-1992 - In fact, storing Optional fields seems an anti-pattern.")
	void readsOptionalsCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Date reference = Date.from(now.atZone(systemDefault()).toInstant());

		org.bson.Document optionalOfLocalDateTime = new org.bson.Document("value", reference);
		org.bson.Document result = new org.bson.Document("localDateTime", optionalOfLocalDateTime);

		TypeWithOptional read = converter.read(TypeWithOptional.class, result);

		assertThat(read.string).isEmpty();
		assertThat(read.localDateTime).isEqualTo(Optional.of(now));
	}

	@Test // DATAMONGO-1118
	void convertsMapKeyUsingCustomConverterForAndBackwards() {

		MappingMongoConverter converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(
				new MongoCustomConversions(Arrays.asList(new FooBarEnumToStringConverter(), new StringToFooNumConverter())));
		converter.afterPropertiesSet();

		ClassWithMapUsingEnumAsKey source = new ClassWithMapUsingEnumAsKey();
		source.map = new HashMap<FooBarEnum, String>();
		source.map.put(FooBarEnum.FOO, "wohoo");

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(converter.read(ClassWithMapUsingEnumAsKey.class, target).map).isEqualTo(source.map);
	}

	@Test // DATAMONGO-1118
	void writesMapKeyUsingCustomConverter() {

		MappingMongoConverter converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(new MongoCustomConversions(Arrays.asList(new FooBarEnumToStringConverter())));
		converter.afterPropertiesSet();

		ClassWithMapUsingEnumAsKey source = new ClassWithMapUsingEnumAsKey();
		source.map = new HashMap<FooBarEnum, String>();
		source.map.put(FooBarEnum.FOO, "spring");
		source.map.put(FooBarEnum.BAR, "data");

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		org.bson.Document map = DocumentTestUtils.getAsDocument(target, "map");

		assertThat(map.containsKey("foo-enum-value")).isTrue();
		assertThat(map.containsKey("bar-enum-value")).isTrue();
	}

	@Test // DATAMONGO-1118
	void readsMapKeyUsingCustomConverter() {

		MappingMongoConverter converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(new MongoCustomConversions(Arrays.asList(new StringToFooNumConverter())));
		converter.afterPropertiesSet();

		org.bson.Document source = new org.bson.Document("map", new org.bson.Document("foo-enum-value", "spring"));

		ClassWithMapUsingEnumAsKey target = converter.read(ClassWithMapUsingEnumAsKey.class, source);

		assertThat(target.map.get(FooBarEnum.FOO)).isEqualTo("spring");
	}

	@Test // DATAMONGO-1471
	void readsDocumentWithPrimitiveIdButNoValue() {
		assertThat(converter.read(ClassWithIntId.class, new org.bson.Document())).isNotNull();
	}

	@Test // DATAMONGO-1497
	void readsPropertyFromNestedFieldCorrectly() {

		org.bson.Document source = new org.bson.Document("nested", new org.bson.Document("sample", "value"));
		TypeWithPropertyInNestedField result = converter.read(TypeWithPropertyInNestedField.class, source);

		assertThat(result.sample).isEqualTo("value");
	}

	@Test // DATAMONGO-1525
	void readsEmptyEnumSet() {

		org.bson.Document source = new org.bson.Document("enumSet", Collections.emptyList());

		assertThat(converter.read(ClassWithEnumProperty.class, source).enumSet).isEqualTo(EnumSet.noneOf(SampleEnum.class));
	}

	@Test // DATAMONGO-1757
	void failsReadingDocumentIntoSimpleType() {

		org.bson.Document nested = new org.bson.Document("key", "value");
		org.bson.Document source = new org.bson.Document("map", new org.bson.Document("key", nested));

		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> converter.read(TypeWithMapOfLongValues.class, source));
	}

	@Test // DATAMONGO-1831
	void shouldConvertArrayInConstructorCorrectly() {

		org.bson.Document source = new org.bson.Document("array", Collections.emptyList());

		assertThat(converter.read(WithArrayInConstructor.class, source).array).isEmpty();
	}

	@Test // DATAMONGO-1831
	void shouldConvertNullForArrayInConstructorCorrectly() {

		org.bson.Document source = new org.bson.Document();

		assertThat(converter.read(WithArrayInConstructor.class, source).array).isNull();
	}

	@Test // DATAMONGO-1898
	void writesInterfaceBackedEnumsToSimpleNameByDefault() {

		org.bson.Document document = new org.bson.Document();

		DocWithInterfacedEnum source = new DocWithInterfacedEnum();
		source.property = InterfacedEnum.INSTANCE;

		converter.write(source, document);

		assertThat(document) //
				.hasSize(2) //
				.hasEntrySatisfying("_class", __ -> {}) //
				.hasEntrySatisfying("property", value -> InterfacedEnum.INSTANCE.name().equals(value));
	}

	@Test // DATAMONGO-1898
	void rejectsConversionFromStringToEnumBackedInterface() {

		org.bson.Document document = new org.bson.Document("property", InterfacedEnum.INSTANCE.name());

		assertThatExceptionOfType(ConverterNotFoundException.class) //
				.isThrownBy(() -> converter.read(DocWithInterfacedEnum.class, document));
	}

	@Test // DATAMONGO-1898
	void readsInterfacedEnumIfConverterIsRegistered() {

		org.bson.Document document = new org.bson.Document("property", InterfacedEnum.INSTANCE.name());

		Converter<String, SomeInterface> enumConverter = new Converter<String, SomeInterface>() {

			@Override
			public SomeInterface convert(String source) {
				return InterfacedEnum.valueOf(source);
			}
		};

		converter.setCustomConversions(new MongoCustomConversions(Collections.singletonList(enumConverter)));
		converter.afterPropertiesSet();

		DocWithInterfacedEnum result = converter.read(DocWithInterfacedEnum.class, document);

		assertThat(result.property).isEqualTo(InterfacedEnum.INSTANCE);
	}

	@Test // DATAMONGO-1904
	void readsNestedArraysCorrectly() {

		List<List<List<Float>>> floats = Collections.singletonList(Collections.singletonList(Arrays.asList(1.0f, 2.0f)));

		org.bson.Document document = new org.bson.Document("nestedFloats", floats);

		WithNestedLists result = converter.read(WithNestedLists.class, document);

		assertThat(result.nestedFloats).hasDimensions(1, 1).isEqualTo(new float[][][] { { { 1.0f, 2.0f } } });
	}

	@Test // DATAMONGO-1992
	void readsImmutableObjectCorrectly() {

		org.bson.Document document = new org.bson.Document("_id", "foo");

		ImmutableObject result = converter.read(ImmutableObject.class, document);

		assertThat(result.id).isEqualTo("foo");
		assertThat(result.witherUsed).isTrue();
	}

	@Test // DATAMONGO-2026
	void readsImmutableObjectWithConstructorIdPropertyCorrectly() {

		org.bson.Document source = new org.bson.Document("_id", "spring").append("value", "data");

		ImmutableObjectWithIdConstructorPropertyAndNoIdWitherMethod target = converter
				.read(ImmutableObjectWithIdConstructorPropertyAndNoIdWitherMethod.class, source);

		assertThat(target.id).isEqualTo("spring");
		assertThat(target.value).isEqualTo("data");
	}

	@Test // DATAMONGO-2011
	void readsNestedListsToObjectCorrectly() {

		List<String> values = Arrays.asList("ONE", "TWO");
		org.bson.Document source = new org.bson.Document("value", Collections.singletonList(values));

		assertThat(converter.read(Attribute.class, source).value).isInstanceOf(List.class);
	}

	@Test // DATAMONGO-2043
	void omitsTypeHintWhenWritingSimpleTypes() {

		org.bson.Document target = new org.bson.Document();
		converter.write(new org.bson.Document("value", "FitzChivalry"), target);

		assertThat(target).doesNotContainKeys("_class");
	}

	@Test // DATAMONGO-1798
	void convertStringIdThatIsAnObjectIdHexToObjectIdIfTargetIsObjectId() {

		ObjectId source = new ObjectId();
		assertThat(converter.convertId(source.toHexString(), ObjectId.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-1798
	void donNotConvertStringIdThatIsAnObjectIdHexToObjectIdIfTargetIsString() {

		ObjectId source = new ObjectId();
		assertThat(converter.convertId(source.toHexString(), String.class)).isEqualTo(source.toHexString());
	}

	@Test // DATAMONGO-1798
	void donNotConvertStringIdThatIsAnObjectIdHexToObjectIdIfTargetIsObject() {

		ObjectId source = new ObjectId();
		assertThat(converter.convertId(source.toHexString(), Object.class)).isEqualTo(source.toHexString());
	}

	@Test // DATAMONGO-2135
	void addsEqualObjectsToCollection() {

		org.bson.Document itemDocument = new org.bson.Document("itemKey", "123");
		org.bson.Document orderDocument = new org.bson.Document("items",
				Arrays.asList(itemDocument, itemDocument, itemDocument));

		Order order = converter.read(Order.class, orderDocument);

		assertThat(order.items).hasSize(3);
	}

	@Test // DATAMONGO-1849
	void mapsValueToExplicitTargetType() {

		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.script = "if (a > b) a else b";

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target.get("script")).isEqualTo(new Code(source.script));
	}

	@Test // DATAMONGO-2328
	void readsScriptAsStringWhenAnnotatedWithFieldTargetType() {

		String reference = "if (a > b) a else b";
		WithExplicitTargetTypes target = converter.read(WithExplicitTargetTypes.class,
				new org.bson.Document("script", new Code(reference)));

		assertThat(target.script).isEqualTo(reference);
	}

	@Test // DATAMONGO-1849
	void mapsCollectionValueToExplicitTargetType() {

		String script = "if (a > b) a else b";
		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.scripts = Collections.singletonList(script);

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target.get("scripts", List.class)).containsExactly(new Code(script));
	}

	@Test // DATAMONGO-1849
	void mapsBigDecimalToDecimal128WhenAnnotatedWithFieldTargetType() {

		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.bigDecimal = BigDecimal.valueOf(3.14159D);

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target.get("bigDecimal")).isEqualTo(new Decimal128(source.bigDecimal));
	}

	@Test // DATAMONGO-2328
	void mapsDateToLongWhenAnnotatedWithFieldTargetType() {

		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.dateAsLong = new Date();

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target.get("dateAsLong")).isEqualTo(source.dateAsLong.getTime());
	}

	@Test // DATAMONGO-2328
	void readsLongAsDateWhenAnnotatedWithFieldTargetType() {

		Date reference = new Date();
		WithExplicitTargetTypes target = converter.read(WithExplicitTargetTypes.class,
				new org.bson.Document("dateAsLong", reference.getTime()));

		assertThat(target.dateAsLong).isEqualTo(reference);
	}

	@Test // DATAMONGO-2328
	void mapsLongToDateWhenAnnotatedWithFieldTargetType() {

		Date date = new Date();
		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.longAsDate = date.getTime();

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target.get("longAsDate")).isEqualTo(date);
	}

	@Test // DATAMONGO-2328
	void readsDateAsLongWhenAnnotatedWithFieldTargetType() {

		Date reference = new Date();
		WithExplicitTargetTypes target = converter.read(WithExplicitTargetTypes.class,
				new org.bson.Document("longAsDate", reference));

		assertThat(target.longAsDate).isEqualTo(reference.getTime());
	}

	@Test // DATAMONGO-2328
	void mapsStringAsBooleanWhenAnnotatedWithFieldTargetType() {

		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.stringAsBoolean = "true";

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target.get("stringAsBoolean")).isEqualTo(true);
	}

	@Test // DATAMONGO-2328
	void readsBooleanAsStringWhenAnnotatedWithFieldTargetType() {

		WithExplicitTargetTypes target = converter.read(WithExplicitTargetTypes.class,
				new org.bson.Document("stringAsBoolean", true));

		assertThat(target.stringAsBoolean).isEqualTo("true");
	}

	@Test // DATAMONGO-2328
	void mapsDateAsObjectIdWhenAnnotatedWithFieldTargetType() {

		WithExplicitTargetTypes source = new WithExplicitTargetTypes();
		source.dateAsObjectId = new Date();

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		// need to compare the the timestamp as ObjectId has an internal counter
		assertThat(target.get("dateAsObjectId", ObjectId.class).getTimestamp())
				.isEqualTo(new ObjectId(source.dateAsObjectId).getTimestamp());
	}

	@Test // DATAMONGO-2328
	void readsObjectIdAsDateWhenAnnotatedWithFieldTargetType() {

		ObjectId reference = new ObjectId();
		WithExplicitTargetTypes target = converter.read(WithExplicitTargetTypes.class,
				new org.bson.Document("dateAsObjectId", reference));

		assertThat(target.dateAsObjectId).isEqualTo(new Date(reference.getTimestamp()));
	}

	@Test // DATAMONGO-2410
	void shouldAllowReadingBackDbObject() {

		assertThat(converter.read(BasicDBObject.class, new org.bson.Document("property", "value")))
				.isEqualTo(new BasicDBObject("property", "value"));
		assertThat(converter.read(DBObject.class, new org.bson.Document("property", "value")))
				.isEqualTo(new BasicDBObject("property", "value"));
	}

	@Test // DATAMONGO-2479
	void entityCallbacksAreNotSetByDefault() {
		assertThat(ReflectionTestUtils.getField(converter, "entityCallbacks")).isNull();
	}

	@Test // DATAMONGO-2479
	void entityCallbacksShouldBeInitiatedOnSettingApplicationContext() {

		ApplicationContext ctx = new StaticApplicationContext();
		converter.setApplicationContext(ctx);

		assertThat(ReflectionTestUtils.getField(converter, "entityCallbacks")).isNotNull();
	}

	@Test // DATAMONGO-2479
	void setterForEntityCallbackOverridesContextInitializedOnes() {

		ApplicationContext ctx = new StaticApplicationContext();
		converter.setApplicationContext(ctx);

		EntityCallbacks callbacks = EntityCallbacks.create();
		converter.setEntityCallbacks(callbacks);

		assertThat(ReflectionTestUtils.getField(converter, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2479
	void setterForApplicationContextShouldNotOverrideAlreadySetEntityCallbacks() {

		EntityCallbacks callbacks = EntityCallbacks.create();
		ApplicationContext ctx = new StaticApplicationContext();

		converter.setEntityCallbacks(callbacks);
		converter.setApplicationContext(ctx);

		assertThat(ReflectionTestUtils.getField(converter, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2479
	void resolveDBRefMapValueShouldInvokeCallbacks() {

		AfterConvertCallback<Person> afterConvertCallback = spy(new ReturningAfterConvertCallback());
		converter.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		when(resolver.fetch(Mockito.any(DBRef.class))).thenReturn(new org.bson.Document());
		DBRef dbRef = mock(DBRef.class);

		org.bson.Document refMap = new org.bson.Document("foo", dbRef);
		org.bson.Document document = new org.bson.Document("personMap", refMap);

		DBRefWrapper result = converter.read(DBRefWrapper.class, document);

		verify(afterConvertCallback).onAfterConvert(eq(result.personMap.get("foo")), eq(new org.bson.Document()), any());
	}

	@Test // DATAMONGO-2300
	void readAndConvertDBRefNestedByMapCorrectly() {

		org.bson.Document cluster = new org.bson.Document("_id", 100L);
		DBRef dbRef = new DBRef("clusters", 100L);

		org.bson.Document data = new org.bson.Document("_id", 3L);
		data.append("cluster", dbRef);

		MappingMongoConverter spyConverter = spy(converter);
		Mockito.doReturn(cluster).when(spyConverter).readRef(dbRef);

		Map<Object, Object> result = spyConverter.readMap(spyConverter.getConversionContext(ObjectPath.ROOT), data,
				ClassTypeInformation.MAP);

		assertThat(((Map) result.get("cluster")).get("_id")).isEqualTo(100L);
	}

	@Test // GH-3546
	void readFlattensNestedDocumentToStringIfNecessary() {

		org.bson.Document source = new org.bson.Document("s",
				new org.bson.Document("json", "string").append("_id", UUID.randomUUID()));

		Address target = converter.read(Address.class, source);
		assertThat(target.street).isNotNull();
	}

	@Test // DATAMONGO-1902
	void writeFlattensUnwrappedType() {

		WithNullableUnwrapped source = new WithNullableUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.transientValue = "must-not-be-written";
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target).containsEntry("_id", "id-1") //
				.containsEntry("stringValue", "string-val") //
				.containsEntry("listValue", Arrays.asList("list-val-1", "list-val-2")) //
				.containsEntry("with-at-field-annotation", "@Field") //
				.doesNotContainKey("embeddableValue") //
				.doesNotContainKey("transientValue");
	}

	@Test // DATAMONGO-1902
	void writePrefixesUnwrappedType() {

		WithPrefixedNullableUnwrapped source = new WithPrefixedNullableUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.transientValue = "must-not-be-written";
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target).containsEntry("_id", "id-1") //
				.containsEntry("prefix-stringValue", "string-val") //
				.containsEntry("prefix-listValue", Arrays.asList("list-val-1", "list-val-2")) //
				.containsEntry("prefix-with-at-field-annotation", "@Field") //
				.doesNotContainKey("embeddableValue") //
				.doesNotContainKey("transientValue") //
				.doesNotContainKey("prefix-transientValue");
	}

	@Test // DATAMONGO-1902
	void writeNullUnwrappedType() {

		WithNullableUnwrapped source = new WithNullableUnwrapped();
		source.id = "id-1";
		source.embeddableValue = null;

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target) //
				.doesNotContainKey("prefix-stringValue").doesNotContainKey("prefix-listValue")
				.doesNotContainKey("embeddableValue");
	}

	@Test // DATAMONGO-1902
	void writeDeepNestedUnwrappedType() {

		WrapperAroundWithUnwrapped source = new WrapperAroundWithUnwrapped();
		source.someValue = "root-level-value";
		source.nullableEmbedded = new WithNullableUnwrapped();
		source.nullableEmbedded.id = "id-1";
		source.nullableEmbedded.embeddableValue = new EmbeddableType();
		source.nullableEmbedded.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.nullableEmbedded.embeddableValue.stringValue = "string-val";
		source.nullableEmbedded.embeddableValue.transientValue = "must-not-be-written";
		source.nullableEmbedded.embeddableValue.atFieldAnnotatedValue = "@Field";

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target).containsEntry("someValue", "root-level-value") //
				.containsEntry("nullableEmbedded", new org.bson.Document("_id", "id-1").append("stringValue", "string-val") //
						.append("listValue", Arrays.asList("list-val-1", "list-val-2")) //
						.append("with-at-field-annotation", "@Field")); //
	}

	@Test // DATAMONGO-1902
	void readUnwrappedType() {

		org.bson.Document source = new org.bson.Document("_id", "id-1") //
				.append("stringValue", "string-val") //
				.append("listValue", Arrays.asList("list-val-1", "list-val-2")) //
				.append("with-at-field-annotation", "@Field");

		EmbeddableType embeddableValue = new EmbeddableType();
		embeddableValue.stringValue = "string-val";
		embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		embeddableValue.atFieldAnnotatedValue = "@Field";

		WithNullableUnwrapped target = converter.read(WithNullableUnwrapped.class, source);
		assertThat(target.embeddableValue).isEqualTo(embeddableValue);
	}

	@Test // DATAMONGO-1902
	void readPrefixedUnwrappedType() {

		org.bson.Document source = new org.bson.Document("_id", "id-1") //
				.append("prefix-stringValue", "string-val") //
				.append("prefix-listValue", Arrays.asList("list-val-1", "list-val-2")) //
				.append("prefix-with-at-field-annotation", "@Field");

		EmbeddableType embeddableValue = new EmbeddableType();
		embeddableValue.stringValue = "string-val";
		embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		embeddableValue.atFieldAnnotatedValue = "@Field";

		WithPrefixedNullableUnwrapped target = converter.read(WithPrefixedNullableUnwrapped.class, source);
		assertThat(target.embeddableValue).isEqualTo(embeddableValue);
	}

	@Test // DATAMONGO-1902
	void readNullableUnwrappedTypeWhenSourceDoesNotContainValues() {

		org.bson.Document source = new org.bson.Document("_id", "id-1");

		WithNullableUnwrapped target = converter.read(WithNullableUnwrapped.class, source);
		assertThat(target.embeddableValue).isNull();
	}

	@Test // DATAMONGO-1902
	void readEmptyUnwrappedTypeWhenSourceDoesNotContainValues() {

		org.bson.Document source = new org.bson.Document("_id", "id-1");

		WithEmptyUnwrappedType target = converter.read(WithEmptyUnwrappedType.class, source);
		assertThat(target.embeddableValue).isNotNull();
	}

	@Test // DATAMONGO-1902
	void readDeepNestedUnwrappedType() {

		org.bson.Document source = new org.bson.Document("someValue", "root-level-value").append("nullableEmbedded",
				new org.bson.Document("_id", "id-1").append("stringValue", "string-val") //
						.append("listValue", Arrays.asList("list-val-1", "list-val-2")) //
						.append("with-at-field-annotation", "@Field"));

		WrapperAroundWithUnwrapped target = converter.read(WrapperAroundWithUnwrapped.class, source);

		EmbeddableType embeddableValue = new EmbeddableType();
		embeddableValue.stringValue = "string-val";
		embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		embeddableValue.atFieldAnnotatedValue = "@Field";

		assertThat(target.someValue).isEqualTo("root-level-value");
		assertThat(target.nullableEmbedded).isNotNull();
		assertThat(target.nullableEmbedded.embeddableValue).isEqualTo(embeddableValue);
	}

	@Test // DATAMONGO-1902
	void readUnwrappedTypeWithComplexValue() {

		org.bson.Document source = new org.bson.Document("_id", "id-1").append("address",
				new org.bson.Document("s", "1007 Mountain Drive").append("city", "Gotham"));

		WithNullableUnwrapped target = converter.read(WithNullableUnwrapped.class, source);

		Address expected = new Address();
		expected.city = "Gotham";
		expected.street = "1007 Mountain Drive";

		assertThat(target.embeddableValue.address) //
				.isEqualTo(expected);
	}

	@Test // DATAMONGO-1902
	void writeUnwrappedTypeWithComplexValue() {

		WithNullableUnwrapped source = new WithNullableUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.address = new Address();
		source.embeddableValue.address.city = "Gotham";
		source.embeddableValue.address.street = "1007 Mountain Drive";

		org.bson.Document target = new org.bson.Document();
		converter.write(source, target);

		assertThat(target) //
				.containsEntry("address", new org.bson.Document("s", "1007 Mountain Drive").append("city", "Gotham")) //
				.doesNotContainKey("street") //
				.doesNotContainKey("address.s") //
				.doesNotContainKey("city") //
				.doesNotContainKey("address.city");
	}

	@Test // GH-3580
	void shouldFallbackToConfiguredCustomConversionTargetOnRead() {

		GenericTypeConverter genericTypeConverter = spy(new GenericTypeConverter());

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(genericTypeConverter);
		}));
		converter.afterPropertiesSet();

		org.bson.Document source = new org.bson.Document("_class", SubTypeOfGenericType.class.getName()).append("value",
				"v1");
		GenericType target = converter.read(GenericType.class, source);

		assertThat(target).isInstanceOf(GenericType.class);
		assertThat(target.content).isEqualTo("v1");

		verify(genericTypeConverter).convert(eq(source));
	}

	@Test // GH-3580
	void shouldUseMostConcreteCustomConversionTargetOnRead() {

		GenericTypeConverter genericTypeConverter = spy(new GenericTypeConverter());
		SubTypeOfGenericTypeConverter subTypeOfGenericTypeConverter = spy(new SubTypeOfGenericTypeConverter());

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(genericTypeConverter);
			it.registerConverter(subTypeOfGenericTypeConverter);
		}));
		converter.afterPropertiesSet();

		org.bson.Document source = new org.bson.Document("_class", SubTypeOfGenericType.class.getName()).append("value",
				"v1");
		GenericType target = converter.read(GenericType.class, source);

		assertThat(target).isInstanceOf(SubTypeOfGenericType.class);
		assertThat(target.content).isEqualTo("v1_s");

		verify(genericTypeConverter, never()).convert(any());
		verify(subTypeOfGenericTypeConverter).convert(eq(source));
	}


	@Test // GH-3660
	void usesCustomConverterForMapTypesOnWrite() {

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(new TypeImplementingMapToDocumentConverter());
		}));
		converter.afterPropertiesSet();

		TypeImplementingMap source = new TypeImplementingMap("one", 2);
		org.bson.Document target = new org.bson.Document();

		converter.write(source, target);

		assertThat(target).containsEntry("1st", "one").containsEntry("2nd", 2);
	}

	@Test // GH-3660
	void usesCustomConverterForTypesImplementingMapOnWrite() {

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(new TypeImplementingMapToDocumentConverter());
		}));
		converter.afterPropertiesSet();

		TypeImplementingMap source = new TypeImplementingMap("one", 2);
		org.bson.Document target = new org.bson.Document();

		converter.write(source, target);

		assertThat(target).containsEntry("1st", "one").containsEntry("2nd", 2);
	}

	@Test // GH-3660
	void usesCustomConverterForTypesImplementingMapOnRead() {

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(new DocumentToTypeImplementingMapConverter());
		}));
		converter.afterPropertiesSet();

		org.bson.Document source = new org.bson.Document("1st", "one")
				.append("2nd", 2)
				.append("_class", TypeImplementingMap.class.getName());

		TypeImplementingMap target = converter.read(TypeImplementingMap.class, source);

		assertThat(target).isEqualTo(new TypeImplementingMap("one", 2));
	}

	@Test // GH-3660
	void usesCustomConverterForPropertiesUsingTypesThatImplementMapOnWrite() {

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(new TypeImplementingMapToDocumentConverter());
		}));
		converter.afterPropertiesSet();

		TypeWrappingTypeImplementingMap source = new TypeWrappingTypeImplementingMap();
		source.typeImplementingMap = new TypeImplementingMap("one", 2);
		org.bson.Document target = new org.bson.Document();

		converter.write(source, target);

		assertThat(target).containsEntry("typeImplementingMap", new org.bson.Document("1st", "one").append("2nd", 2));
	}

	@Test // GH-3660
	void usesCustomConverterForPropertiesUsingTypesImplementingMapOnRead() {

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(new DocumentToTypeImplementingMapConverter());
		}));
		converter.afterPropertiesSet();

		org.bson.Document source = new org.bson.Document("typeImplementingMap",
		new org.bson.Document("1st", "one")
				.append("2nd", 2))
				.append("_class", TypeWrappingTypeImplementingMap.class.getName());

		TypeWrappingTypeImplementingMap target = converter.read(TypeWrappingTypeImplementingMap.class, source);

		assertThat(target.typeImplementingMap).isEqualTo(new TypeImplementingMap("one", 2));
	}

	@Test // GH-3407
	void shouldWriteNullPropertyCorrectly() {

		WithFieldWrite fieldWrite = new WithFieldWrite();

		org.bson.Document document = new org.bson.Document();
		converter.write(fieldWrite, document);

		assertThat(document).containsEntry("writeAlways", null).doesNotContainKey("writeNonNull");
		assertThat(document).containsEntry("writeAlwaysPerson", null).doesNotContainKey("writeNonNullPerson");
	}

	@Test // GH-3686
	void readsCollectionContainingNullValue() {

		org.bson.Document source = new org.bson.Document("items", Arrays.asList(new org.bson.Document("itemKey", "i1"), null, new org.bson.Document("itemKey", "i3")));

		Order target = converter.read(Order.class, source);

		assertThat(target.items)
				.map(it -> it != null ? it.itemKey : null)
				.containsExactly("i1", null, "i3");
	}

	@Test // GH-3686
	void readsArrayContainingNullValue() {

		org.bson.Document source = new org.bson.Document("arrayOfStrings", Arrays.asList("i1", null, "i3"));

		WithArrays target = converter.read(WithArrays.class, source);

		assertThat(target.arrayOfStrings).containsExactly("i1", null, "i3");
	}

	@Test // GH-3686
	void readsMapContainingNullValue() {

		org.bson.Document source = new org.bson.Document("mapOfObjects", new org.bson.Document("item1", "i1").append("item2", null).append("item3", "i3"));

		ClassWithMapProperty target = converter.read(ClassWithMapProperty.class, source);

		assertThat(target.mapOfObjects)
				.containsEntry("item1", "i1")
				.containsEntry("item2", null)
				.containsEntry("item3", "i3");
	}

	@Test // GH-3670
	void appliesCustomConverterEvenToSimpleTypes() {

		converter = new MappingMongoConverter(resolver, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(it -> {
			it.registerConverter(new MongoSimpleTypeConverter());
		}));
		converter.afterPropertiesSet();

		org.bson.Document source = new org.bson.Document("content", new Binary(new byte[] {0x00, 0x42}));

		GenericType<Object> target = converter.read(GenericType.class, source);
		assertThat(target.content).isInstanceOf(byte[].class);
	}

	@Test // GH-3702
	void readsRawDocument() {

		org.bson.Document source = new org.bson.Document("_id", "id-1").append("raw", new org.bson.Document("simple", 1).append("document", new org.bson.Document("inner-doc", 1)));

		WithRawDocumentProperties target = converter.read(WithRawDocumentProperties.class, source);

		assertThat(target.raw).isInstanceOf(org.bson.Document.class).isEqualTo( new org.bson.Document("simple", 1).append("document", new org.bson.Document("inner-doc", 1)));
	}

	@Test // GH-3702
	void readsListOfRawDocument() {

		org.bson.Document source = new org.bson.Document("_id", "id-1").append("listOfRaw", Arrays.asList(new org.bson.Document("simple", 1).append("document", new org.bson.Document("inner-doc", 1))));

		WithRawDocumentProperties target = converter.read(WithRawDocumentProperties.class, source);

		assertThat(target.listOfRaw)
				.containsExactly(new org.bson.Document("simple", 1).append("document", new org.bson.Document("inner-doc", 1)));
	}

	@Test // GH-3692
	void readsMapThatDoesNotComeAsDocument() {

		org.bson.Document source = new org.bson.Document("_id", "id-1").append("mapOfObjects", Collections.singletonMap("simple", 1));

		ClassWithMapProperty target = converter.read(ClassWithMapProperty.class, source);

		assertThat(target.mapOfObjects).containsEntry("simple",1);
	}

	@Test // GH-3851
	void associationMappingShouldFallBackToDefaultIfNoAtReferenceAnnotationPresent/* as done via jmolecules */() {

		UUID id = UUID.randomUUID();
		Person sourceValue = new Person();
		sourceValue.id = id.toString();

		DocumentAccessor accessor = new DocumentAccessor(new org.bson.Document());
		MongoPersistentProperty persistentProperty = mock(MongoPersistentProperty.class);
		when(persistentProperty.isAssociation()).thenReturn(true);
		when(persistentProperty.getFieldName()).thenReturn("pName");
		doReturn(ClassTypeInformation.from(Person.class)).when(persistentProperty).getTypeInformation();
		doReturn(Person.class).when(persistentProperty).getType();
		doReturn(Person.class).when(persistentProperty).getRawType();

		converter.writePropertyInternal(sourceValue, accessor, persistentProperty);

		assertThat(accessor.getDocument()).isEqualTo(new org.bson.Document("pName", new org.bson.Document("_id", id.toString())));
	}

	@Test // GH-2860
	void projectShouldReadSimpleInterfaceProjection() {

		org.bson.Document source = new org.bson.Document("birthDate",
				Date.from(LocalDate.of(1999, 12, 1).atStartOfDay().toInstant(ZoneOffset.UTC))).append("foo",
				"Walter");

		EntityProjectionIntrospector discoverer = EntityProjectionIntrospector.create(converter.getProjectionFactory(),
				EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy()
						.and((target, underlyingType) -> !converter.conversions.isSimpleType(target)),
				mappingContext);

		EntityProjection<PersonProjection, Person> projection = discoverer
				.introspect(PersonProjection.class, Person.class);
		PersonProjection person = converter.project(projection, source);

		assertThat(person.getBirthDate()).isEqualTo(LocalDate.of(1999, 12, 1));
		assertThat(person.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2860
	void projectShouldReadSimpleDtoProjection() {

		org.bson.Document source = new org.bson.Document("birthDate",
				Date.from(LocalDate.of(1999, 12, 1).atStartOfDay().toInstant(ZoneOffset.UTC))).append("foo",
				"Walter");

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(converter.getProjectionFactory(),
				EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy()
						.and((target, underlyingType) -> !converter.conversions.isSimpleType(target)),
				mappingContext);

		EntityProjection<PersonDto, Person> projection = introspector
				.introspect(PersonDto.class, Person.class);
		PersonDto person = converter.project(projection, source);

		assertThat(person.getBirthDate()).isEqualTo(LocalDate.of(1999, 12, 1));
		assertThat(person.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2860
	void projectShouldReadNestedProjection() {

		org.bson.Document source = new org.bson.Document("addresses",
				Collections.singletonList(new org.bson.Document("s", "hwy")));

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(converter.getProjectionFactory(),
				EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy()
						.and((target, underlyingType) -> !converter.conversions.isSimpleType(target)),
				mappingContext);

		EntityProjection<WithNestedProjection, Person> projection = introspector
				.introspect(WithNestedProjection.class, Person.class);
		WithNestedProjection person = converter.project(projection, source);

		assertThat(person.getAddresses()).extracting(AddressProjection::getStreet).hasSize(1).containsOnly("hwy");
	}

	@Test // GH-2860
	void projectShouldReadProjectionWithNestedEntity() {

		org.bson.Document source = new org.bson.Document("addresses",
				Collections.singletonList(new org.bson.Document("s", "hwy")));

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(converter.getProjectionFactory(),
				EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy()
						.and((target, underlyingType) -> !converter.conversions.isSimpleType(target)),
				mappingContext);

		EntityProjection<ProjectionWithNestedEntity, Person> projection = introspector
				.introspect(ProjectionWithNestedEntity.class, Person.class);
		ProjectionWithNestedEntity person = converter.project(projection, source);

		assertThat(person.getAddresses()).extracting(Address::getStreet).hasSize(1).containsOnly("hwy");
	}

	static class GenericType<T> {
		T content;
	}

	static class ClassWithEnumProperty {

		SampleEnum sampleEnum;
		List<SampleEnum> enums;
		EnumSet<SampleEnum> enumSet;
		EnumMap<SampleEnum, String> enumMap;
	}

	enum SampleEnum {
		FIRST {
			@Override
			void method() {}
		},
		SECOND {
			@Override
			void method() {}
		};

		abstract void method();
	}

	interface InterfaceType {

	}

	@EqualsAndHashCode
	@Getter
	static class Address implements InterfaceType {
		@Field("s")
		String street;
		String city;
	}

	interface Contact {

	}

	static class Person implements Contact {

		@Id String id;

		Date birthDate;

		@Field("foo") String firstname;
		String lastname;

		Set<Address> addresses;

		Person() {

		}

		@PersistenceConstructor
		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	interface PersonProjection {

		LocalDate getBirthDate();

		String getFirstname();
	}

	interface WithNestedProjection {

		Set<AddressProjection> getAddresses();
	}

	interface ProjectionWithNestedEntity {

		Set<Address> getAddresses();
	}

	interface AddressProjection {

		String getStreet();
	}

	static class PersonDto {

		LocalDate birthDate;

		@Field("foo") String firstname;
		String lastname;

		public PersonDto(LocalDate birthDate, String firstname, String lastname) {
			this.birthDate = birthDate;
			this.firstname = firstname;
			this.lastname = lastname;
		}

		public LocalDate getBirthDate() {
			return birthDate;
		}

		public String getFirstname() {
			return firstname;
		}

		public String getLastname() {
			return lastname;
		}
	}

	static class ClassWithSortedMap {
		SortedMap<String, String> map;
	}

	static class ClassWithMapProperty {
		Map<Locale, String> map;
		Map<String, List<String>> mapOfLists;
		Map<String, Object> mapOfObjects;
		Map<String, String[]> mapOfStrings;
		Map<String, Person> mapOfPersons;
		TreeMap<String, Person> treeMapOfPersons;
	}

	static class ClassWithNestedMaps {
		Map<String, Map<String, Map<String, String>>> nestedMaps;
	}

	static class BirthDateContainer {
		Date birthDate;
	}

	static class BigDecimalContainer {
		BigDecimal value;
		Map<String, BigDecimal> map;
		List<BigDecimal> collection;
	}

	static class CollectionWrapper {
		List<Contact> contacts;
		List<List<String>> strings;
		List<Map<String, Locale>> listOfMaps;
		Set<Contact> contactsSet;
	}

	static class LocaleWrapper {
		Locale locale;
	}

	static class ClassWithBigIntegerId {
		@Id BigInteger id;
	}

	static class A<T> {

		String valueType;
		T value;

		A(T value) {
			this.valueType = value.getClass().getName();
			this.value = value;
		}
	}

	static class ClassWithIntId {

		@Id int id;
	}

	static class DefaultedConstructorArgument {

		String foo;
		int bar;
		double foobar;

		DefaultedConstructorArgument(String foo, @Value("#root.something ?: -1") int bar, double foobar) {
			this.foo = foo;
			this.bar = bar;
			this.foobar = foobar;
		}
	}

	static class Item {
		List<Attribute> attributes;
	}

	static class Attribute {
		String key;
		Object value;
	}

	static class Outer {

		class Inner {
			String value;
		}

		Inner inner;
	}

	static class DBRefWrapper {

		DBRef ref;
		List<DBRef> refs;
		Map<String, DBRef> refMap;
		Map<String, Person> personMap;
	}

	static class URLWrapper {
		URL url;
	}

	static class ClassWithComplexId {

		@Id ComplexId complexId;
	}

	static class ComplexId {
		Long innerId;
	}

	static class TypWithCollectionConstructor {

		List<Attribute> attributes;

		public TypWithCollectionConstructor(List<Attribute> attributes) {
			this.attributes = attributes;
		}
	}

	@TypeAlias("_")
	static class Aliased {
		String name;
	}

	static class ThrowableWrapper {

		Throwable throwable;
	}

	@Document
	static class PrimitiveContainer {

		@Field("property") private int m_property;

		@PersistenceConstructor
		public PrimitiveContainer(@Value("#root.property") int a_property) {
			m_property = a_property;
		}

		public int property() {
			return m_property;
		}
	}

	@Document
	static class ObjectContainer {

		@Field("property") private PrimitiveContainer m_property;

		@PersistenceConstructor
		public ObjectContainer(@Value("#root.property") PrimitiveContainer a_property) {
			m_property = a_property;
		}

		public PrimitiveContainer property() {
			return m_property;
		}
	}

	class ClassWithGeoBox {

		Box box;
	}

	class ClassWithGeoCircle {

		Circle circle;
	}

	class ClassWithGeoSphere {

		Sphere sphere;
	}

	class ClassWithGeoPolygon {

		Polygon polygon;
	}

	class ClassWithGeoShape {

		Shape shape;
	}

	class ClassWithTextScoreProperty {

		@TextScore Float score;
	}

	class ClassWithExplicitlyNamedDBRefProperty {

		@Field("explict-name-for-db-ref") //
		@org.springframework.data.mongodb.core.mapping.DBRef //
		ClassWithIntId dbRefProperty;

		public ClassWithIntId getDbRefProperty() {
			return dbRefProperty;
		}
	}

	static class RootForClassWithExplicitlyRenamedIdField {

		@Id String id;
		ClassWithExplicitlyRenamedField nested;
	}

	static class ClassWithExplicitlyRenamedField {

		@Field("id") String id;
	}

	static class RootForClassWithNamedIdField {

		String id;
		ClassWithNamedIdField nested;
	}

	static class ClassWithNamedIdField {

		String id;
	}

	static class ClassWithAnnotatedIdField {

		@Id String key;
	}

	static class TypeWithLocalDateTime {

		LocalDateTime date;

		TypeWithLocalDateTime() {
			this.date = LocalDateTime.now();
		}
	}

	static class TypeWithOptional {

		Optional<String> string = Optional.empty();
		Optional<LocalDateTime> localDateTime = Optional.empty();
	}

	static class ClassWithMapUsingEnumAsKey {

		enum FooBarEnum {
			FOO, BAR
		}

		Map<FooBarEnum, String> map;
	}

	@WritingConverter
	static class FooBarEnumToStringConverter implements Converter<FooBarEnum, String> {

		@Override
		public String convert(FooBarEnum source) {

			if (source == null) {
				return null;
			}

			return FooBarEnum.FOO.equals(source) ? "foo-enum-value" : "bar-enum-value";
		}
	}

	@ReadingConverter
	static class StringToFooNumConverter implements Converter<String, FooBarEnum> {

		@Override
		public FooBarEnum convert(String source) {

			if (source == null) {
				return null;
			}

			if ("foo-enum-value".equals(source)) {
				return FooBarEnum.FOO;
			}
			if ("bar-enum-value".equals(source)) {
				return FooBarEnum.BAR;
			}

			throw new ConversionNotSupportedException(source, String.class, null);
		}
	}

	static class TypeWithPropertyInNestedField {
		@Field("nested.sample") String sample;
	}

	static class TypeWithMapOfLongValues {
		Map<String, Long> map;
	}

	@RequiredArgsConstructor
	static class WithArrayInConstructor {

		final String[] array;

	}

	static class WithArrays {
		String[] arrayOfStrings;
	}

	// DATAMONGO-1898

	// DATACMNS-1278
	static interface SomeInterface {}

	static enum InterfacedEnum implements SomeInterface {
		INSTANCE;
	}

	static class DocWithInterfacedEnum {
		SomeInterface property;
	}

	// DATAMONGO-1904

	static class WithNestedLists {
		float[][][] nestedFloats;
	}

	static class ImmutableObject {

		final String id;
		final String name;
		final boolean witherUsed;

		private ImmutableObject(String id) {
			this.id = id;
			this.name = null;
			this.witherUsed = false;
		}

		private ImmutableObject(String id, String name, boolean witherUsed) {
			this.id = id;
			this.name = name;
			this.witherUsed = witherUsed;
		}

		public ImmutableObject() {
			this.id = null;
			this.name = null;
			witherUsed = false;
		}

		public ImmutableObject withId(String id) {
			return new ImmutableObject(id, name, true);
		}

		public String getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public boolean isWitherUsed() {
			return witherUsed;
		}
	}

	@RequiredArgsConstructor
	static class ImmutableObjectWithIdConstructorPropertyAndNoIdWitherMethod {

		final @Id String id;
		String value;
	}

	// DATAMONGO-2135

	@EqualsAndHashCode // equality check by fields
	static class SomeItem {
		String itemKey;
	}

	static class Order {
		Collection<SomeItem> items = new ArrayList<>();
	}

	static class WithExplicitTargetTypes {

		@Field(targetType = FieldType.SCRIPT) //
		String script;

		@Field(targetType = FieldType.SCRIPT) //
		List<String> scripts;

		@Field(targetType = FieldType.DECIMAL128) //
		BigDecimal bigDecimal;

		@Field(targetType = FieldType.INT64) //
		Date dateAsLong;

		@Field(targetType = FieldType.DATE_TIME) //
		Long longAsDate;

		@Field(targetType = FieldType.BOOLEAN) //
		String stringAsBoolean;

		@Field(targetType = FieldType.OBJECT_ID) //
		Date dateAsObjectId;
	}

	static class WrapperAroundWithUnwrapped {

		String someValue;
		WithNullableUnwrapped nullableEmbedded;
		WithEmptyUnwrappedType emptyEmbedded;
		WithPrefixedNullableUnwrapped prefixedEmbedded;
	}

	static class WithNullableUnwrapped {

		String id;

		@Unwrapped.Nullable EmbeddableType embeddableValue;
	}

	static class WithPrefixedNullableUnwrapped {

		String id;

		@Unwrapped.Nullable("prefix-") EmbeddableType embeddableValue;
	}

	static class WithEmptyUnwrappedType {

		String id;

		@Unwrapped.Empty EmbeddableType embeddableValue;
	}

	@EqualsAndHashCode
	static class EmbeddableType {

		String stringValue;
		List<String> listValue;

		@Field("with-at-field-annotation") //
		String atFieldAnnotatedValue;

		@Transient //
		String transientValue;

		Address address;
	}

	static class ReturningAfterConvertCallback implements AfterConvertCallback<Person> {

		@Override
		public Person onAfterConvert(Person entity, org.bson.Document document, String collection) {

			return entity;
		}
	}

	static class SubTypeOfGenericType extends GenericType<String> {

	}

	@ReadingConverter
	static class GenericTypeConverter implements Converter<org.bson.Document, GenericType<?>> {

		@Override
		public GenericType<?> convert(org.bson.Document source) {

			GenericType<Object> target = new GenericType<>();
			target.content = source.get("value");
			return target;
		}
	}

	@ReadingConverter
	static class SubTypeOfGenericTypeConverter implements Converter<org.bson.Document, SubTypeOfGenericType> {

		@Override
		public SubTypeOfGenericType convert(org.bson.Document source) {

			SubTypeOfGenericType target = new SubTypeOfGenericType();
			target.content = source.getString("value") + "_s";
			return target;
		}
	}

	@WritingConverter
	static class TypeImplementingMapToDocumentConverter implements Converter<TypeImplementingMap, org.bson.Document> {

		@Nullable
		@Override
		public org.bson.Document convert(TypeImplementingMap source) {
			return new org.bson.Document("1st", source.val1).append("2nd", source.val2);
		}
	}

	@ReadingConverter
	static class DocumentToTypeImplementingMapConverter implements Converter<org.bson.Document, TypeImplementingMap> {

		@Nullable
		@Override
		public TypeImplementingMap convert(org.bson.Document source) {
			return new TypeImplementingMap(source.getString("1st"), source.getInteger("2nd"));
		}
	}

	@ReadingConverter
	public static class MongoSimpleTypeConverter implements Converter<Binary, Object> {

		@Override
		public byte[] convert(Binary source) {
			return source.getData();
		}
	}

	static class TypeWrappingTypeImplementingMap {

		String id;
		TypeImplementingMap typeImplementingMap;
	}

	@EqualsAndHashCode
	static class TypeImplementingMap implements Map<String,String> {

		String val1;
		int val2;

		TypeImplementingMap(String val1, int val2) {
			this.val1 = val1;
			this.val2 = val2;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public boolean containsKey(Object key) {
			return false;
		}

		@Override
		public boolean containsValue(Object value) {
			return false;
		}

		@Override
		public String get(Object key) {
			return null;
		}

		@Nullable
		@Override
		public String put(String key, String value) {
			return null;
		}

		@Override
		public String remove(Object key) {
			return null;
		}

		@Override
		public void putAll(@NonNull Map<? extends String, ? extends String> m) {

		}

		@Override
		public void clear() {

		}

		@NonNull
		@Override
		public Set<String> keySet() {
			return null;
		}

		@NonNull
		@Override
		public Collection<String> values() {
			return null;
		}

		@NonNull
		@Override
		public Set<Entry<String, String>> entrySet() {
			return null;
		}
	}

	static class WithRawDocumentProperties {

		String id;
		org.bson.Document raw;
		List<org.bson.Document> listOfRaw;
	}

	static class WithFieldWrite {

		@org.springframework.data.mongodb.core.mapping.Field(
				write = org.springframework.data.mongodb.core.mapping.Field.Write.NON_NULL) Integer writeNonNull;

		@org.springframework.data.mongodb.core.mapping.Field(
				write = org.springframework.data.mongodb.core.mapping.Field.Write.ALWAYS) Integer writeAlways;

		@org.springframework.data.mongodb.core.mapping.DBRef @org.springframework.data.mongodb.core.mapping.Field(
				write = org.springframework.data.mongodb.core.mapping.Field.Write.NON_NULL) Person writeNonNullPerson;

		@org.springframework.data.mongodb.core.mapping.DBRef @org.springframework.data.mongodb.core.mapping.Field(
				write = org.springframework.data.mongodb.core.mapping.Field.Write.ALWAYS) Person writeAlwaysPerson;

	}
}
