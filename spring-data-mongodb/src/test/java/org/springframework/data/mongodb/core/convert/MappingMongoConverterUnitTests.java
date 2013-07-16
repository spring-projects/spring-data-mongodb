/*
 * Copyright 2011-2013 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.bson.types.ObjectId;
import org.hamcrest.Matcher;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.MappingInstantiationException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link MappingMongoConverter}.
 * 
 * @author Oliver Gierke
 * @author Patrik Wasik
 */
@RunWith(MockitoJUnitRunner.class)
public class MappingMongoConverterUnitTests {

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;
	@Mock MongoDbFactory factory;
	@Mock ApplicationContext context;

	@Before
	public void setUp() {

		mappingContext = new MongoMappingContext();
		mappingContext.setApplicationContext(context);
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(factory, mappingContext);
		converter.afterPropertiesSet();
	}

	@Test
	public void convertsAddressCorrectly() {

		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		DBObject dbObject = new BasicDBObject();

		converter.write(address, dbObject);

		assertThat(dbObject.get("city").toString(), is("New York"));
		assertThat(dbObject.get("street").toString(), is("Broadway"));
	}

	@Test
	public void convertsJodaTimeTypesCorrectly() {

		converter = new MappingMongoConverter(factory, mappingContext);
		converter.afterPropertiesSet();

		Person person = new Person();
		person.birthDate = new LocalDate();

		DBObject dbObject = new BasicDBObject();
		converter.write(person, dbObject);

		assertThat(dbObject.get("birthDate"), is(instanceOf(Date.class)));

		Person result = converter.read(Person.class, dbObject);
		assertThat(result.birthDate, is(notNullValue()));
	}

	@Test
	public void convertsCustomTypeOnConvertToMongoType() {

		converter = new MappingMongoConverter(factory, mappingContext);
		converter.afterPropertiesSet();

		LocalDate date = new LocalDate();
		converter.convertToMongoType(date);
	}

	/**
	 * @see DATAMONGO-130
	 */
	@Test
	public void writesMapTypeCorrectly() {

		Map<Locale, String> map = Collections.singletonMap(Locale.US, "Foo");

		BasicDBObject dbObject = new BasicDBObject();
		converter.write(map, dbObject);

		assertThat(dbObject.get(Locale.US.toString()).toString(), is("Foo"));
	}

	/**
	 * @see DATAMONGO-130
	 */
	@Test
	public void readsMapWithCustomKeyTypeCorrectly() {

		DBObject mapObject = new BasicDBObject(Locale.US.toString(), "Value");
		DBObject dbObject = new BasicDBObject("map", mapObject);

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, dbObject);
		assertThat(result.map.get(Locale.US), is("Value"));
	}

	/**
	 * @see DATAMONGO-128
	 */
	@Test
	public void usesDocumentsStoredTypeIfSubtypeOfRequest() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("birthDate", new LocalDate());
		dbObject.put(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(Contact.class, dbObject), is(instanceOf(Person.class)));
	}

	/**
	 * @see DATAMONGO-128
	 */
	@Test
	public void ignoresDocumentsStoredTypeIfCompletelyDifferentTypeRequested() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("birthDate", new LocalDate());
		dbObject.put(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(BirthDateContainer.class, dbObject), is(instanceOf(BirthDateContainer.class)));
	}

	@Test
	public void writesTypeDiscriminatorIntoRootObject() {

		Person person = new Person();

		DBObject result = new BasicDBObject();
		converter.write(person, result);

		assertThat(result.containsField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(true));
		assertThat(result.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY).toString(), is(Person.class.getName()));
	}

	/**
	 * @see DATAMONGO-136
	 */
	@Test
	public void writesEnumsCorrectly() {

		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.sampleEnum = SampleEnum.FIRST;

		DBObject result = new BasicDBObject();
		converter.write(value, result);

		assertThat(result.get("sampleEnum"), is(instanceOf(String.class)));
		assertThat(result.get("sampleEnum").toString(), is("FIRST"));
	}

	/**
	 * @see DATAMONGO-209
	 */
	@Test
	public void writesEnumCollectionCorrectly() {

		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.enums = Arrays.asList(SampleEnum.FIRST);

		DBObject result = new BasicDBObject();
		converter.write(value, result);

		assertThat(result.get("enums"), is(instanceOf(BasicDBList.class)));

		BasicDBList enums = (BasicDBList) result.get("enums");
		assertThat(enums.size(), is(1));
		assertThat((String) enums.get(0), is("FIRST"));
	}

	/**
	 * @see DATAMONGO-136
	 */
	@Test
	public void readsEnumsCorrectly() {
		DBObject dbObject = new BasicDBObject("sampleEnum", "FIRST");
		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);

		assertThat(result.sampleEnum, is(SampleEnum.FIRST));
	}

	/**
	 * @see DATAMONGO-209
	 */
	@Test
	public void readsEnumCollectionsCorrectly() {

		BasicDBList enums = new BasicDBList();
		enums.add("FIRST");
		DBObject dbObject = new BasicDBObject("enums", enums);

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);

		assertThat(result.enums, is(instanceOf(List.class)));
		assertThat(result.enums.size(), is(1));
		assertThat(result.enums, hasItem(SampleEnum.FIRST));
	}

	/**
	 * @see DATAMONGO-144
	 */
	@Test
	public void considersFieldNameWhenWriting() {

		Person person = new Person();
		person.firstname = "Oliver";

		DBObject result = new BasicDBObject();
		converter.write(person, result);

		assertThat(result.containsField("foo"), is(true));
		assertThat(result.containsField("firstname"), is(false));
	}

	/**
	 * @see DATAMONGO-144
	 */
	@Test
	public void considersFieldNameWhenReading() {

		DBObject dbObject = new BasicDBObject("foo", "Oliver");
		Person result = converter.read(Person.class, dbObject);

		assertThat(result.firstname, is("Oliver"));
	}

	@Test
	public void resolvesNestedComplexTypeForConstructorCorrectly() {

		DBObject address = new BasicDBObject("street", "110 Southwark Street");
		address.put("city", "London");

		BasicDBList addresses = new BasicDBList();
		addresses.add(address);

		DBObject person = new BasicDBObject("firstname", "Oliver");
		person.put("addresses", addresses);

		Person result = converter.read(Person.class, person);
		assertThat(result.addresses, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-145
	 */
	@Test
	public void writesCollectionWithInterfaceCorrectly() {

		Person person = new Person();
		person.firstname = "Oliver";

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.contacts = Arrays.asList((Contact) person);

		BasicDBObject dbObject = new BasicDBObject();
		converter.write(wrapper, dbObject);

		Object result = dbObject.get("contacts");
		assertThat(result, is(instanceOf(BasicDBList.class)));
		BasicDBList contacts = (BasicDBList) result;
		DBObject personDbObject = (DBObject) contacts.get(0);
		assertThat(personDbObject.get("foo").toString(), is("Oliver"));
		assertThat((String) personDbObject.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(Person.class.getName()));
	}

	/**
	 * @see DATAMONGO-145
	 */
	@Test
	public void readsCollectionWithInterfaceCorrectly() {

		BasicDBObject person = new BasicDBObject(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());
		person.put("foo", "Oliver");

		BasicDBList contacts = new BasicDBList();
		contacts.add(person);

		CollectionWrapper result = converter.read(CollectionWrapper.class, new BasicDBObject("contacts", contacts));
		assertThat(result.contacts, is(notNullValue()));
		assertThat(result.contacts.size(), is(1));
		Contact contact = result.contacts.get(0);
		assertThat(contact, is(instanceOf(Person.class)));
		assertThat(((Person) contact).firstname, is("Oliver"));
	}

	@Test
	public void convertsLocalesOutOfTheBox() {
		LocaleWrapper wrapper = new LocaleWrapper();
		wrapper.locale = Locale.US;

		DBObject dbObject = new BasicDBObject();
		converter.write(wrapper, dbObject);

		Object localeField = dbObject.get("locale");
		assertThat(localeField, is(instanceOf(String.class)));
		assertThat((String) localeField, is("en_US"));

		LocaleWrapper read = converter.read(LocaleWrapper.class, dbObject);
		assertThat(read.locale, is(Locale.US));
	}

	/**
	 * @see DATAMONGO-161
	 */
	@Test
	public void readsNestedMapsCorrectly() {

		Map<String, String> secondLevel = new HashMap<String, String>();
		secondLevel.put("key1", "value1");
		secondLevel.put("key2", "value2");

		Map<String, Map<String, String>> firstLevel = new HashMap<String, Map<String, String>>();
		firstLevel.put("level1", secondLevel);
		firstLevel.put("level2", secondLevel);

		ClassWithNestedMaps maps = new ClassWithNestedMaps();
		maps.nestedMaps = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		maps.nestedMaps.put("afield", firstLevel);

		DBObject dbObject = new BasicDBObject();
		converter.write(maps, dbObject);

		ClassWithNestedMaps result = converter.read(ClassWithNestedMaps.class, dbObject);
		Map<String, Map<String, Map<String, String>>> nestedMap = result.nestedMaps;
		assertThat(nestedMap, is(notNullValue()));
		assertThat(nestedMap.get("afield"), is(firstLevel));
	}

	/**
	 * @see DATACMNS-42, DATAMONGO-171
	 */
	@Test
	public void writesClassWithBigDecimal() {

		BigDecimalContainer container = new BigDecimalContainer();
		container.value = BigDecimal.valueOf(2.5d);
		container.map = Collections.singletonMap("foo", container.value);

		DBObject dbObject = new BasicDBObject();
		converter.write(container, dbObject);

		assertThat(dbObject.get("value"), is(instanceOf(String.class)));
		assertThat((String) dbObject.get("value"), is("2.5"));
		assertThat(((DBObject) dbObject.get("map")).get("foo"), is(instanceOf(String.class)));
	}

	/**
	 * @see DATACMNS-42, DATAMONGO-171
	 */
	@Test
	public void readsClassWithBigDecimal() {

		DBObject dbObject = new BasicDBObject("value", "2.5");
		dbObject.put("map", new BasicDBObject("foo", "2.5"));

		BasicDBList list = new BasicDBList();
		list.add("2.5");
		dbObject.put("collection", list);
		BigDecimalContainer result = converter.read(BigDecimalContainer.class, dbObject);

		assertThat(result.value, is(BigDecimal.valueOf(2.5d)));
		assertThat(result.map.get("foo"), is(BigDecimal.valueOf(2.5d)));
		assertThat(result.collection.get(0), is(BigDecimal.valueOf(2.5d)));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void writesNestedCollectionsCorrectly() {

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.strings = Arrays.asList(Arrays.asList("Foo"));

		DBObject dbObject = new BasicDBObject();
		converter.write(wrapper, dbObject);

		Object outerStrings = dbObject.get("strings");
		assertThat(outerStrings, is(instanceOf(BasicDBList.class)));

		BasicDBList typedOuterString = (BasicDBList) outerStrings;
		assertThat(typedOuterString.size(), is(1));
	}

	/**
	 * @see DATAMONGO-192
	 */
	@Test
	public void readsEmptySetsCorrectly() {

		Person person = new Person();
		person.addresses = Collections.emptySet();

		DBObject dbObject = new BasicDBObject();
		converter.write(person, dbObject);
		converter.read(Person.class, dbObject);
	}

	@Test
	public void convertsObjectIdStringsToObjectIdCorrectly() {
		PersonPojoStringId p1 = new PersonPojoStringId("1234567890", "Text-1");
		DBObject dbo1 = new BasicDBObject();

		converter.write(p1, dbo1);
		assertThat(dbo1.get("_id"), is(instanceOf(String.class)));

		PersonPojoStringId p2 = new PersonPojoStringId(new ObjectId().toString(), "Text-1");
		DBObject dbo2 = new BasicDBObject();

		converter.write(p2, dbo2);
		assertThat(dbo2.get("_id"), is(instanceOf(ObjectId.class)));
	}

	/**
	 * @see DATAMONGO-207
	 */
	@Test
	public void convertsCustomEmptyMapCorrectly() {

		DBObject map = new BasicDBObject();
		DBObject wrapper = new BasicDBObject("map", map);

		ClassWithSortedMap result = converter.read(ClassWithSortedMap.class, wrapper);

		assertThat(result, is(instanceOf(ClassWithSortedMap.class)));
		assertThat(result.map, is(instanceOf(SortedMap.class)));
	}

	/**
	 * @see DATAMONGO-211
	 */
	@Test
	public void maybeConvertHandlesNullValuesCorrectly() {
		assertThat(converter.convertToMongoType(null), is(nullValue()));
	}

	@Test
	public void writesGenericTypeCorrectly() {

		GenericType<Address> type = new GenericType<Address>();
		type.content = new Address();
		type.content.city = "London";

		BasicDBObject result = new BasicDBObject();
		converter.write(type, result);

		DBObject content = (DBObject) result.get("content");
		assertThat(content.get("_class"), is(notNullValue()));
		assertThat(content.get("city"), is(notNullValue()));
	}

	@Test
	public void readsGenericTypeCorrectly() {

		DBObject address = new BasicDBObject("_class", Address.class.getName());
		address.put("city", "London");

		GenericType<?> result = converter.read(GenericType.class, new BasicDBObject("content", address));
		assertThat(result.content, is(instanceOf(Address.class)));

	}

	/**
	 * @see DATAMONGO-228
	 */
	@Test
	public void writesNullValuesForMaps() {

		ClassWithMapProperty foo = new ClassWithMapProperty();
		foo.map = Collections.singletonMap(Locale.US, null);

		DBObject result = new BasicDBObject();
		converter.write(foo, result);

		Object map = result.get("map");
		assertThat(map, is(instanceOf(DBObject.class)));
		assertThat(((DBObject) map).keySet(), hasItem("en_US"));
	}

	@Test
	public void writesBigIntegerIdCorrectly() {

		ClassWithBigIntegerId foo = new ClassWithBigIntegerId();
		foo.id = BigInteger.valueOf(23L);

		DBObject result = new BasicDBObject();
		converter.write(foo, result);

		assertThat(result.get("_id"), is(instanceOf(String.class)));
	}

	public void convertsObjectsIfNecessary() {

		ObjectId id = new ObjectId();
		assertThat(converter.convertToMongoType(id), is((Object) id));
	}

	/**
	 * @see DATAMONGO-235
	 */
	@Test
	public void writesMapOfListsCorrectly() {

		ClassWithMapProperty input = new ClassWithMapProperty();
		input.mapOfLists = Collections.singletonMap("Foo", Arrays.asList("Bar"));

		BasicDBObject result = new BasicDBObject();
		converter.write(input, result);

		Object field = result.get("mapOfLists");
		assertThat(field, is(instanceOf(DBObject.class)));

		DBObject map = (DBObject) field;
		Object foo = map.get("Foo");
		assertThat(foo, is(instanceOf(BasicDBList.class)));

		BasicDBList value = (BasicDBList) foo;
		assertThat(value.size(), is(1));
		assertThat((String) value.get(0), is("Bar"));
	}

	/**
	 * @see DATAMONGO-235
	 */
	@Test
	public void readsMapListValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add("Bar");
		DBObject source = new BasicDBObject("mapOfLists", new BasicDBObject("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		assertThat(result.mapOfLists, is(not(nullValue())));
	}

	/**
	 * @see DATAMONGO-235
	 */
	@Test
	public void writesMapsOfObjectsCorrectly() {

		ClassWithMapProperty input = new ClassWithMapProperty();
		input.mapOfObjects = new HashMap<String, Object>();
		input.mapOfObjects.put("Foo", Arrays.asList("Bar"));

		BasicDBObject result = new BasicDBObject();
		converter.write(input, result);

		Object field = result.get("mapOfObjects");
		assertThat(field, is(instanceOf(DBObject.class)));

		DBObject map = (DBObject) field;
		Object foo = map.get("Foo");
		assertThat(foo, is(instanceOf(BasicDBList.class)));

		BasicDBList value = (BasicDBList) foo;
		assertThat(value.size(), is(1));
		assertThat((String) value.get(0), is("Bar"));
	}

	/**
	 * @see DATAMONGO-235
	 */
	@Test
	public void readsMapOfObjectsListValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add("Bar");
		DBObject source = new BasicDBObject("mapOfObjects", new BasicDBObject("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		assertThat(result.mapOfObjects, is(not(nullValue())));
	}

	/**
	 * @see DATAMONGO-245
	 */
	@Test
	public void readsMapListNestedValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		BasicDBObject nested = new BasicDBObject();
		nested.append("Hello", "World");
		list.add(nested);
		DBObject source = new BasicDBObject("mapOfObjects", new BasicDBObject("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		Object firstObjectInFoo = ((List<?>) result.mapOfObjects.get("Foo")).get(0);
		assertThat(firstObjectInFoo, is(instanceOf(Map.class)));
		assertThat((String) ((Map<?, ?>) firstObjectInFoo).get("Hello"), is(equalTo("World")));
	}

	/**
	 * @see DATAMONGO-245
	 */
	@Test
	public void readsMapDoublyNestedValuesCorrectly() {

		BasicDBObject nested = new BasicDBObject();
		BasicDBObject doubly = new BasicDBObject();
		doubly.append("Hello", "World");
		nested.append("nested", doubly);
		DBObject source = new BasicDBObject("mapOfObjects", new BasicDBObject("Foo", nested));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		Object foo = result.mapOfObjects.get("Foo");
		assertThat(foo, is(instanceOf(Map.class)));
		Object doublyNestedObject = ((Map<?, ?>) foo).get("nested");
		assertThat(doublyNestedObject, is(instanceOf(Map.class)));
		assertThat((String) ((Map<?, ?>) doublyNestedObject).get("Hello"), is(equalTo("World")));
	}

	/**
	 * @see DATAMONGO-245
	 */
	@Test
	public void readsMapListDoublyNestedValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		BasicDBObject nested = new BasicDBObject();
		BasicDBObject doubly = new BasicDBObject();
		doubly.append("Hello", "World");
		nested.append("nested", doubly);
		list.add(nested);
		DBObject source = new BasicDBObject("mapOfObjects", new BasicDBObject("Foo", list));

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, source);
		Object firstObjectInFoo = ((List<?>) result.mapOfObjects.get("Foo")).get(0);
		assertThat(firstObjectInFoo, is(instanceOf(Map.class)));
		Object doublyNestedObject = ((Map<?, ?>) firstObjectInFoo).get("nested");
		assertThat(doublyNestedObject, is(instanceOf(Map.class)));
		assertThat((String) ((Map<?, ?>) doublyNestedObject).get("Hello"), is(equalTo("World")));
	}

	/**
	 * @see DATAMONGO-259
	 */
	@Test
	public void writesListOfMapsCorrectly() {

		Map<String, Locale> map = Collections.singletonMap("Foo", Locale.ENGLISH);

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.listOfMaps = new ArrayList<Map<String, Locale>>();
		wrapper.listOfMaps.add(map);

		DBObject result = new BasicDBObject();
		converter.write(wrapper, result);

		BasicDBList list = (BasicDBList) result.get("listOfMaps");
		assertThat(list, is(notNullValue()));
		assertThat(list.size(), is(1));

		DBObject dbObject = (DBObject) list.get(0);
		assertThat(dbObject.containsField("Foo"), is(true));
		assertThat((String) dbObject.get("Foo"), is(Locale.ENGLISH.toString()));
	}

	/**
	 * @see DATAMONGO-259
	 */
	@Test
	public void readsListOfMapsCorrectly() {

		DBObject map = new BasicDBObject("Foo", "en");

		BasicDBList list = new BasicDBList();
		list.add(map);

		DBObject wrapperSource = new BasicDBObject("listOfMaps", list);

		CollectionWrapper wrapper = converter.read(CollectionWrapper.class, wrapperSource);

		assertThat(wrapper.listOfMaps, is(notNullValue()));
		assertThat(wrapper.listOfMaps.size(), is(1));
		assertThat(wrapper.listOfMaps.get(0), is(notNullValue()));
		assertThat(wrapper.listOfMaps.get(0).get("Foo"), is(Locale.ENGLISH));
	}

	/**
	 * @see DATAMONGO-259
	 */
	@Test
	public void writesPlainMapOfCollectionsCorrectly() {

		Map<String, List<Locale>> map = Collections.singletonMap("Foo", Arrays.asList(Locale.US));
		DBObject result = new BasicDBObject();
		converter.write(map, result);

		assertThat(result.containsField("Foo"), is(true));
		assertThat(result.get("Foo"), is(notNullValue()));
		assertThat(result.get("Foo"), is(instanceOf(BasicDBList.class)));

		BasicDBList list = (BasicDBList) result.get("Foo");

		assertThat(list.size(), is(1));
		assertThat(list.get(0), is((Object) Locale.US.toString()));
	}

	/**
	 * @see DATAMONGO-285
	 */
	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSaveMapWithACollectionAsValue() {

		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("string", "hello");
		List<String> list = new ArrayList<String>();
		list.add("ping");
		list.add("pong");
		keyValues.put("list", list);

		DBObject dbObject = new BasicDBObject();
		converter.write(keyValues, dbObject);

		Map<String, Object> keyValuesFromMongo = converter.read(Map.class, dbObject);

		assertEquals(keyValues.size(), keyValuesFromMongo.size());
		assertEquals(keyValues.get("string"), keyValuesFromMongo.get("string"));
		assertTrue(List.class.isAssignableFrom(keyValuesFromMongo.get("list").getClass()));
		List<String> listFromMongo = (List) keyValuesFromMongo.get("list");
		assertEquals(list.size(), listFromMongo.size());
		assertEquals(list.get(0), listFromMongo.get(0));
		assertEquals(list.get(1), listFromMongo.get(1));
	}

	/**
	 * @see DATAMONGO-309
	 */
	@Test
	@SuppressWarnings({ "unchecked" })
	public void writesArraysAsMapValuesCorrectly() {

		ClassWithMapProperty wrapper = new ClassWithMapProperty();
		wrapper.mapOfObjects = new HashMap<String, Object>();
		wrapper.mapOfObjects.put("foo", new String[] { "bar" });

		DBObject result = new BasicDBObject();
		converter.write(wrapper, result);

		Object mapObject = result.get("mapOfObjects");
		assertThat(mapObject, is(instanceOf(BasicDBObject.class)));

		DBObject map = (DBObject) mapObject;
		Object valueObject = map.get("foo");
		assertThat(valueObject, is(instanceOf(BasicDBList.class)));

		List<Object> list = (List<Object>) valueObject;
		assertThat(list.size(), is(1));
		assertThat(list, hasItem((Object) "bar"));
	}

	/**
	 * @see DATAMONGO-324
	 */
	@Test
	public void writesDbObjectCorrectly() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("foo", "bar");

		DBObject result = new BasicDBObject();

		converter.write(dbObject, result);

		result.removeField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);
		assertThat(dbObject, is(result));
	}

	/**
	 * @see DATAMONGO-324
	 */
	@Test
	public void readsDbObjectCorrectly() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("foo", "bar");

		DBObject result = converter.read(DBObject.class, dbObject);

		assertThat(result, is(dbObject));
	}

	/**
	 * @see DATAMONGO-329
	 */
	@Test
	public void writesMapAsGenericFieldCorrectly() {

		Map<String, A<String>> objectToSave = new HashMap<String, A<String>>();
		objectToSave.put("test", new A<String>("testValue"));

		A<Map<String, A<String>>> a = new A<Map<String, A<String>>>(objectToSave);
		DBObject result = new BasicDBObject();

		converter.write(a, result);

		assertThat((String) result.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(A.class.getName()));
		assertThat((String) result.get("valueType"), is(HashMap.class.getName()));

		DBObject object = (DBObject) result.get("value");
		assertThat(object, is(notNullValue()));

		DBObject inner = (DBObject) object.get("test");
		assertThat(inner, is(notNullValue()));
		assertThat((String) inner.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(A.class.getName()));
		assertThat((String) inner.get("valueType"), is(String.class.getName()));
		assertThat((String) inner.get("value"), is("testValue"));
	}

	@Test
	public void writesIntIdCorrectly() {

		ClassWithIntId value = new ClassWithIntId();
		value.id = 5;

		DBObject result = new BasicDBObject();
		converter.write(value, result);

		assertThat(result.get("_id"), is((Object) 5));
	}

	/**
	 * @see DATAMONGO-368
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void writesNullValuesForCollection() {

		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.contacts = Arrays.<Contact> asList(new Person(), null);

		DBObject result = new BasicDBObject();
		converter.write(wrapper, result);

		Object contacts = result.get("contacts");
		assertThat(contacts, is(instanceOf(Collection.class)));
		assertThat(((Collection<?>) contacts).size(), is(2));
		assertThat((Collection<Object>) contacts, hasItem(nullValue()));
	}

	/**
	 * @see DATAMONGO-379
	 */
	@Test
	public void considersDefaultingExpressionsAtConstructorArguments() {

		DBObject dbObject = new BasicDBObject("foo", "bar");
		dbObject.put("foobar", 2.5);

		DefaultedConstructorArgument result = converter.read(DefaultedConstructorArgument.class, dbObject);
		assertThat(result.bar, is(-1));
	}

	/**
	 * @see DATAMONGO-379
	 */
	@Test
	public void usesDocumentFieldIfReferencedInAtValue() {

		DBObject dbObject = new BasicDBObject("foo", "bar");
		dbObject.put("something", 37);
		dbObject.put("foobar", 2.5);

		DefaultedConstructorArgument result = converter.read(DefaultedConstructorArgument.class, dbObject);
		assertThat(result.bar, is(37));
	}

	/**
	 * @see DATAMONGO-379
	 */
	@Test(expected = MappingInstantiationException.class)
	public void rejectsNotFoundConstructorParameterForPrimitiveType() {

		DBObject dbObject = new BasicDBObject("foo", "bar");

		converter.read(DefaultedConstructorArgument.class, dbObject);
	}

	/**
	 * @see DATAMONGO-358
	 */
	@Test
	public void writesListForObjectPropertyCorrectly() {

		Attribute attribute = new Attribute();
		attribute.key = "key";
		attribute.value = Arrays.asList("1", "2");

		Item item = new Item();
		item.attributes = Arrays.asList(attribute);

		DBObject result = new BasicDBObject();

		converter.write(item, result);

		Item read = converter.read(Item.class, result);
		assertThat(read.attributes.size(), is(1));
		assertThat(read.attributes.get(0).key, is(attribute.key));
		assertThat(read.attributes.get(0).value, is(instanceOf(Collection.class)));

		@SuppressWarnings("unchecked")
		Collection<String> values = (Collection<String>) read.attributes.get(0).value;

		assertThat(values.size(), is(2));
		assertThat(values, hasItems("1", "2"));
	}

	/**
	 * @see DATAMONGO-380
	 */
	@Test(expected = MappingException.class)
	public void rejectsMapWithKeyContainingDotsByDefault() {
		converter.write(Collections.singletonMap("foo.bar", "foobar"), new BasicDBObject());
	}

	/**
	 * @see DATAMONGO-380
	 */
	@Test
	public void escapesDotInMapKeysIfReplacementConfigured() {

		converter.setMapKeyDotReplacement("~");

		DBObject dbObject = new BasicDBObject();
		converter.write(Collections.singletonMap("foo.bar", "foobar"), dbObject);

		assertThat((String) dbObject.get("foo~bar"), is("foobar"));
		assertThat(dbObject.containsField("foo.bar"), is(false));
	}

	/**
	 * @see DATAMONGO-380
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void unescapesDotInMapKeysIfReplacementConfigured() {

		converter.setMapKeyDotReplacement("~");

		DBObject dbObject = new BasicDBObject("foo~bar", "foobar");
		Map<String, String> result = converter.read(Map.class, dbObject);

		assertThat(result.get("foo.bar"), is("foobar"));
		assertThat(result.containsKey("foobar"), is(false));
	}

	/**
	 * @see DATAMONGO-382
	 */
	@Test
	public void convertsSetToBasicDBList() {

		Address address = new Address();
		address.city = "London";
		address.street = "Foo";

		Object result = converter.convertToMongoType(Collections.singleton(address));
		assertThat(result, is(instanceOf(BasicDBList.class)));

		Set<?> readResult = converter.read(Set.class, (BasicDBList) result);
		assertThat(readResult.size(), is(1));
		assertThat(readResult.iterator().next(), is(instanceOf(Map.class)));
	}

	/**
	 * @see DATAMONGO-402
	 */
	@Test
	public void readsMemberClassCorrectly() {

		DBObject dbObject = new BasicDBObject("inner", new BasicDBObject("value", "FOO!"));

		Outer outer = converter.read(Outer.class, dbObject);
		assertThat(outer.inner, is(notNullValue()));
		assertThat(outer.inner.value, is("FOO!"));
		assertSyntheticFieldValueOf(outer.inner, outer);
	}

	/**
	 * @see DATAMONGO-347
	 */
	@Test
	public void createsSimpleDBRefCorrectly() {

		Person person = new Person();
		person.id = "foo";

		DBRef dbRef = converter.toDBRef(person, null);
		assertThat(dbRef.getId(), is((Object) "foo"));
		assertThat(dbRef.getRef(), is("person"));
	}

	/**
	 * @see DATAMONGO-347
	 */
	@Test
	public void createsDBRefWithClientSpecCorrectly() {

		PropertyPath path = PropertyPath.from("person", PersonClient.class);
		MongoPersistentProperty property = mappingContext.getPersistentPropertyPath(path).getLeafProperty();

		Person person = new Person();
		person.id = "foo";

		DBRef dbRef = converter.toDBRef(person, property);
		assertThat(dbRef.getId(), is((Object) "foo"));
		assertThat(dbRef.getRef(), is("person"));
	}

	/**
	 * @see DATAMONGO-458
	 */
	@Test
	public void readEmptyCollectionIsModifiable() {

		DBObject dbObject = new BasicDBObject("contactsSet", new BasicDBList());
		CollectionWrapper wrapper = converter.read(CollectionWrapper.class, dbObject);

		assertThat(wrapper.contactsSet, is(notNullValue()));
		wrapper.contactsSet.add(new Contact() {});
	}

	/**
	 * @see DATAMONGO-424
	 */
	@Test
	public void readsPlainDBRefObject() {

		DBRef dbRef = new DBRef(mock(DB.class), "foo", 2);
		DBObject dbObject = new BasicDBObject("ref", dbRef);

		DBRefWrapper result = converter.read(DBRefWrapper.class, dbObject);
		assertThat(result.ref, is(dbRef));
	}

	/**
	 * @see DATAMONGO-424
	 */
	@Test
	public void readsCollectionOfDBRefs() {

		DBRef dbRef = new DBRef(mock(DB.class), "foo", 2);
		BasicDBList refs = new BasicDBList();
		refs.add(dbRef);

		DBObject dbObject = new BasicDBObject("refs", refs);

		DBRefWrapper result = converter.read(DBRefWrapper.class, dbObject);
		assertThat(result.refs, hasSize(1));
		assertThat(result.refs, hasItem(dbRef));
	}

	/**
	 * @see DATAMONGO-424
	 */
	@Test
	public void readsDBRefMap() {

		DBRef dbRef = mock(DBRef.class);
		BasicDBObject refMap = new BasicDBObject("foo", dbRef);
		DBObject dbObject = new BasicDBObject("refMap", refMap);

		DBRefWrapper result = converter.read(DBRefWrapper.class, dbObject);

		assertThat(result.refMap.entrySet(), hasSize(1));
		assertThat(result.refMap.values(), hasItem(dbRef));
	}

	/**
	 * @see DATAMONGO-424
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void resolvesDBRefMapValue() {

		DBRef dbRef = mock(DBRef.class);
		when(dbRef.fetch()).thenReturn(new BasicDBObject());

		BasicDBObject refMap = new BasicDBObject("foo", dbRef);
		DBObject dbObject = new BasicDBObject("personMap", refMap);

		DBRefWrapper result = converter.read(DBRefWrapper.class, dbObject);

		Matcher isPerson = instanceOf(Person.class);

		assertThat(result.personMap.entrySet(), hasSize(1));
		assertThat(result.personMap.values(), hasItem(isPerson));
	}

	/**
	 * @see DATAMONGO-462
	 */
	@Test
	public void writesURLsAsStringOutOfTheBox() throws Exception {

		URLWrapper wrapper = new URLWrapper();
		wrapper.url = new URL("http://springsource.org");
		DBObject sink = new BasicDBObject();

		converter.write(wrapper, sink);

		assertThat(sink.get("url"), is((Object) "http://springsource.org"));
	}

	/**
	 * @see DATAMONGO-462
	 */
	@Test
	public void readsURLFromStringOutOfTheBox() throws Exception {
		DBObject dbObject = new BasicDBObject("url", "http://springsource.org");
		URLWrapper result = converter.read(URLWrapper.class, dbObject);
		assertThat(result.url, is(new URL("http://springsource.org")));
	}

	/**
	 * @see DATAMONGO-485
	 */
	@Test
	public void writesComplexIdCorrectly() {

		ComplexId id = new ComplexId();
		id.innerId = 4711L;

		ClassWithComplexId entity = new ClassWithComplexId();
		entity.complexId = id;

		DBObject dbObject = new BasicDBObject();
		converter.write(entity, dbObject);

		Object idField = dbObject.get("_id");
		assertThat(idField, is(notNullValue()));
		assertThat(idField, is(instanceOf(DBObject.class)));
		assertThat(((DBObject) idField).get("innerId"), is((Object) 4711L));
	}

	/**
	 * @see DATAMONGO-485
	 */
	@Test
	public void readsComplexIdCorrectly() {

		DBObject innerId = new BasicDBObject("innerId", 4711L);
		DBObject entity = new BasicDBObject("_id", innerId);

		ClassWithComplexId result = converter.read(ClassWithComplexId.class, entity);

		assertThat(result.complexId, is(notNullValue()));
		assertThat(result.complexId.innerId, is(4711L));
	}

	/**
	 * @see DATAMONGO-489
	 */
	@Test
	public void readsArraysAsMapValuesCorrectly() {

		BasicDBList list = new BasicDBList();
		list.add("Foo");
		list.add("Bar");

		DBObject map = new BasicDBObject("key", list);
		DBObject wrapper = new BasicDBObject("mapOfStrings", map);

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, wrapper);
		assertThat(result.mapOfStrings, is(notNullValue()));

		String[] values = result.mapOfStrings.get("key");
		assertThat(values, is(notNullValue()));
		assertThat(values, is(arrayWithSize(2)));
	}

	/**
	 * @see DATAMONGO-497
	 */
	@Test
	public void readsEmptyCollectionIntoConstructorCorrectly() {

		DBObject source = new BasicDBObject("attributes", new BasicDBList());

		TypWithCollectionConstructor result = converter.read(TypWithCollectionConstructor.class, source);
		assertThat(result.attributes, is(notNullValue()));
	}

	private static void assertSyntheticFieldValueOf(Object target, Object expected) {

		for (int i = 0; i < 10; i++) {
			try {
				assertThat(ReflectionTestUtils.getField(target, "this$" + i), is(expected));
				return;
			} catch (IllegalArgumentException e) {
				// Suppress and try next
			}
		}

		fail(String.format("Didn't find synthetic field on %s!", target));
	}

	/**
	 * @see DATAMGONGO-508
	 */
	@Test
	public void eagerlyReturnsDBRefObjectIfTargetAlreadyIsOne() {

		DB db = mock(DB.class);
		DBRef dbRef = new DBRef(db, "collection", "id");

		org.springframework.data.mongodb.core.mapping.DBRef annotation = mock(org.springframework.data.mongodb.core.mapping.DBRef.class);

		assertThat(converter.createDBRef(dbRef, annotation), is(dbRef));
	}

	/**
	 * @see DATAMONGO-523
	 */
	@Test
	public void considersTypeAliasAnnotation() {

		Aliased aliased = new Aliased();
		aliased.name = "foo";

		DBObject result = new BasicDBObject();
		converter.write(aliased, result);

		Object type = result.get("_class");
		assertThat(type, is(notNullValue()));
		assertThat(type.toString(), is("_"));
	}

	/**
	 * @see DATAMONGO-533
	 */
	@Test
	public void marshalsThrowableCorrectly() {

		ThrowableWrapper wrapper = new ThrowableWrapper();
		wrapper.throwable = new Exception();

		DBObject dbObject = new BasicDBObject();
		converter.write(wrapper, dbObject);
	}

	/**
	 * @see DATAMONGO-592
	 */
	@Test
	public void recursivelyConvertsSpELReadValue() {

		DBObject input = (DBObject) JSON
				.parse("{ \"_id\" : { \"$oid\" : \"50ca271c4566a2b08f2d667a\" }, \"_class\" : \"com.recorder.TestRecorder2$ObjectContainer\", \"property\" : { \"property\" : 100 } }");

		converter.read(ObjectContainer.class, input);
	}

	/**
	 * @see DATAMONGO-657
	 */
	@Test
	public void convertDocumentWithMapDBRef() {

		MapDBRef mapDBRef = new MapDBRef();

		MapDBRefVal val = new MapDBRefVal();
		val.id = BigInteger.ONE;

		Map<String, MapDBRefVal> mapVal = new HashMap<String, MapDBRefVal>();
		mapVal.put("test", val);

		mapDBRef.map = mapVal;

		BasicDBObject dbObject = new BasicDBObject();
		converter.write(mapDBRef, dbObject);

		DBObject map = (DBObject) dbObject.get("map");

		assertThat(map.get("test"), instanceOf(DBRef.class));

		DBObject mapValDBObject = new BasicDBObject();
		mapValDBObject.put("_id", BigInteger.ONE);

		DBRef dbRef = mock(DBRef.class);
		when(dbRef.fetch()).thenReturn(mapValDBObject);

		((DBObject) dbObject.get("map")).put("test", dbRef);

		MapDBRef read = converter.read(MapDBRef.class, dbObject);

		assertThat(read.map.get("test").id, is(BigInteger.ONE));
	}

	/**
	 * @see DATAMONGO-724
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void mappingConsidersCustomConvertersNotWritingTypeInformation() {

		Person person = new Person();
		person.firstname = "Dave";

		ClassWithMapProperty entity = new ClassWithMapProperty();
		entity.mapOfPersons = new HashMap<String, Person>();
		entity.mapOfPersons.put("foo", person);
		entity.mapOfObjects = new HashMap<String, Object>();
		entity.mapOfObjects.put("foo", person);

		CustomConversions conversions = new CustomConversions(Arrays.asList(new Converter<Person, DBObject>() {

			@Override
			public DBObject convert(Person source) {
				return new BasicDBObject().append("firstname", source.firstname);
			}

		}, new Converter<DBObject, Person>() {

			@Override
			public Person convert(DBObject source) {
				Person person = new Person();
				person.firstname = source.get("firstname").toString();
				return person;
			}
		}));

		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		MappingMongoConverter mongoConverter = new MappingMongoConverter(factory, context);
		mongoConverter.setCustomConversions(conversions);
		mongoConverter.afterPropertiesSet();

		BasicDBObject dbObject = new BasicDBObject();
		mongoConverter.write(entity, dbObject);

		ClassWithMapProperty result = mongoConverter.read(ClassWithMapProperty.class, dbObject);

		assertThat(result.mapOfPersons, is(notNullValue()));
		Person personCandidate = result.mapOfPersons.get("foo");
		assertThat(personCandidate, is(notNullValue()));
		assertThat(personCandidate.firstname, is("Dave"));

		assertThat(result.mapOfObjects, is(notNullValue()));
		Object value = result.mapOfObjects.get("foo");
		assertThat(value, is(notNullValue()));
		assertThat(value, is(instanceOf(Map.class)));
	}

	@Document
	class MapDBRef {
		@org.springframework.data.mongodb.core.mapping.DBRef Map<String, MapDBRefVal> map;
	}

	@Document
	class MapDBRefVal {
		BigInteger id;
	}

	static class GenericType<T> {
		T content;
	}

	static class ClassWithEnumProperty {

		SampleEnum sampleEnum;
		List<SampleEnum> enums;
	}

	static enum SampleEnum {
		FIRST {
			@Override
			void method() {}
		},
		SECOND {
			@Override
			void method() {

			}
		};

		abstract void method();
	}

	static class Address {
		String street;
		String city;
	}

	interface Contact {

	}

	static class Person implements Contact {

		@Id String id;

		LocalDate birthDate;

		@Field("foo") String firstname;

		Set<Address> addresses;

		public Person() {

		}

		@PersistenceConstructor
		public Person(Set<Address> addresses) {
			this.addresses = addresses;
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
	}

	static class ClassWithNestedMaps {
		Map<String, Map<String, Map<String, String>>> nestedMaps;
	}

	static class BirthDateContainer {
		LocalDate birthDate;
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

		public A(T value) {
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

	static class PersonClient {

		@org.springframework.data.mongodb.core.mapping.DBRef Person person;
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

		@Field("property") private final int m_property;

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

		@Field("property") private final PrimitiveContainer m_property;

		@PersistenceConstructor
		public ObjectContainer(@Value("#root.property") PrimitiveContainer a_property) {
			m_property = a_property;
		}

		public PrimitiveContainer property() {
			return m_property;
		}
	}
}
