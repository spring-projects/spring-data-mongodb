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

package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.PersonPojoStringId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link MappingMongoConverter}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MappingMongoConverterUnitTests {

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;
	@Mock
	MongoDbFactory factory;

	@Before
	public void setUp() {
		
		mappingContext = new MongoMappingContext();
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

		List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
		converters.add(new LocalDateToDateConverter());
		converters.add(new DateToLocalDateConverter());
		
		CustomConversions conversions = new CustomConversions(converters);
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		converter = new MappingMongoConverter(factory, mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		Person person = new Person();
		person.birthDate = new LocalDate();

		DBObject dbObject = new BasicDBObject();
		converter.write(person, dbObject);

		assertThat(dbObject.get("birthDate"), is(Date.class));

		Person result = converter.read(Person.class, dbObject);
		assertThat(result.birthDate, is(notNullValue()));
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

		assertThat(converter.read(Contact.class, dbObject), is(Person.class));
	}

	/**
	 * @see DATAMONGO-128
	 */
	@Test
	public void ignoresDocumentsStoredTypeIfCompletelyDifferentTypeRequested() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("birthDate", new LocalDate());
		dbObject.put(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(BirthDateContainer.class, dbObject), is(BirthDateContainer.class));
	}

	@Test
	public void writesTypeDiscriminatorIntoRootObject() {

		Person person = new Person();
		person.birthDate = new LocalDate();

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
		
		assertThat(result.get("sampleEnum"), is(String.class));
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
		
		assertThat(result.get("enums"), is(BasicDBList.class));
		
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
		
		assertThat(result.enums, is(List.class));
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
	
	/**
	 * @see DATAMONGO-145
	 */
	@Test
	public void writesCollectionWithInterfaceCorrectly() {
		Person person = new Person();
		person.birthDate = new LocalDate();
		person.firstname = "Oliver";
		
		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.contacts = Arrays.asList((Contact) person);
		
		BasicDBObject dbObject = new BasicDBObject();
		converter.write(wrapper, dbObject);
		
		Object result = dbObject.get("contacts");
		assertThat(result, is(BasicDBList.class));
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
		assertThat(contact, is(Person.class));
		assertThat(((Person) contact).firstname, is("Oliver"));
		
	}
	
	@Test
	public void convertsLocalesOutOfTheBox() {
		LocaleWrapper wrapper = new LocaleWrapper();
		wrapper.locale = Locale.US;
		
		DBObject dbObject = new BasicDBObject();
		converter.write(wrapper, dbObject);
		
		Object localeField = dbObject.get("locale");
		assertThat(localeField, is(String.class));
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
		assertThat(dbo1.get("_id"), is(String.class));
		
		PersonPojoStringId p2 = new PersonPojoStringId(new ObjectId().toString(), "Text-1");
		DBObject dbo2 = new BasicDBObject();
		
		converter.write(p2, dbo2);
		assertThat(dbo2.get("_id"), is(ObjectId.class));
	}
	
	/**
	 * @see DATAMONGO-207
	 */
	@Test
	public void convertsCustomEmptyMapCorrectly() {
			
		DBObject map = new BasicDBObject();
		DBObject wrapper = new BasicDBObject("map", map);
		
		ClassWithSortedMap result = converter.read(ClassWithSortedMap.class, wrapper);
		
		assertThat(result, is(ClassWithSortedMap.class));
		assertThat(result.map, is(SortedMap.class));
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
		assertThat(result.get("Foo"), is(BasicDBList.class));
		
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void writesArraysAsMapValuesCorrectly() {
		
		ClassWithMapProperty wrapper = new ClassWithMapProperty();
		wrapper.mapOfObjects = new HashMap<String, Object>();
		wrapper.mapOfObjects.put("foo", new String[] { "bar" });
		
		DBObject result = new BasicDBObject();
		converter.write(wrapper, result);
		
		Object mapObject = result.get("mapOfObjects");
		assertThat(mapObject, is(BasicDBObject.class));
		
		DBObject map = (DBObject) mapObject;
		Object valueObject = map.get("foo");
		assertThat(valueObject, is(BasicDBList.class));
		
		List<Object> list = (List<Object>) valueObject;
		assertThat(list.size(), is(1));
		assertThat(list, hasItem((Object) "bar"));
	}
	
	class GenericType<T> {
		T content;
	}
	
	class ClassWithEnumProperty {
		
		SampleEnum sampleEnum;
		List<SampleEnum> enums;
	}
	
	enum SampleEnum {
		FIRST {
			@Override
			void method() {
							}
		},
		SECOND {
			@Override
			void method() {
				
			}
		};

		abstract void method();
	}
	
	class Address {
		String street;
		String city;
	}

	interface Contact {

	}

	public static class Person implements Contact {
		LocalDate birthDate;
		
		@Field("foo")
		String firstname;
		
		Set<Address> addresses;
		
		public Person() {
			
		}

		@PersistenceConstructor
		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	class ClassWithSortedMap {
		SortedMap<String, String> map;
	}
	
	class ClassWithMapProperty {
		Map<Locale, String> map;
		Map<String, List<String>> mapOfLists;
		Map<String, Object> mapOfObjects;
	}

	class ClassWithNestedMaps {
		Map<String, Map<String, Map<String, String>>> nestedMaps;
	}
	
	class BirthDateContainer {
		LocalDate birthDate;
	}
	
	class BigDecimalContainer {
		BigDecimal value;
		Map<String, BigDecimal> map;
		List<BigDecimal> collection;
	}
	
	class CollectionWrapper {
		List<Contact> contacts;
		List<List<String>> strings;
		List<Map<String, Locale>> listOfMaps;
	}

	class LocaleWrapper {
		Locale locale;
	}
	
	class ClassWithBigIntegerId {
		@Id
		BigInteger id;
	}
	
	private class LocalDateToDateConverter implements Converter<LocalDate, Date> {

		public Date convert(LocalDate source) {
			return source.toDateMidnight().toDate();
		}
	}

	private class DateToLocalDateConverter implements Converter<Date, LocalDate> {

		public LocalDate convert(Date source) {
			return new LocalDate(source.getTime());
		}
	}
}
