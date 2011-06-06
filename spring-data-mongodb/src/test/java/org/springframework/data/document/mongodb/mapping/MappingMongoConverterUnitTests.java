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

package org.springframework.data.document.mongodb.mapping;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.MongoDbFactory;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;

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
		converter = new MappingMongoConverter(factory, mappingContext);
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

		Set<Converter<?, ?>> converters = new HashSet<Converter<?, ?>>();
		converters.add(new LocalDateToDateConverter());
		converters.add(new DateToLocalDateConverter());

		List<Class<?>> customSimpleTypes = new ArrayList<Class<?>>();
		customSimpleTypes.add(LocalDate.class);
		mappingContext.setCustomSimpleTypes(customSimpleTypes);

		converter = new MappingMongoConverter(factory, mappingContext);
		converter.setCustomConverters(converters);
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
	 * @see DATADOC-130
	 */
	@Test
	public void writesMapTypeCorrectly() {

		Map<Locale, String> map = Collections.singletonMap(Locale.US, "Foo");

		BasicDBObject dbObject = new BasicDBObject();
		converter.write(map, dbObject);

		assertThat(dbObject.get(Locale.US.toString()).toString(), is("Foo"));
	}

	/**
	 * @see DATADOC-130
	 */
	@Test
	public void readsMapWithCustomKeyTypeCorrectly() {

		DBObject mapObject = new BasicDBObject(Locale.US.toString(), "Value");
		DBObject dbObject = new BasicDBObject("map", mapObject);

		ClassWithMapProperty result = converter.read(ClassWithMapProperty.class, dbObject);
		assertThat(result.map.get(Locale.US), is("Value"));
	}

	/**
	 * @see DATADOC-128
	 */
	@Test
	public void usesDocumentsStoredTypeIfSubtypeOfRequest() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("birthDate", new LocalDate());
		dbObject.put(MappingMongoConverter.CUSTOM_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(Contact.class, dbObject), is(Person.class));
	}

	/**
	 * @see DATADOC-128
	 */
	@Test
	public void ignoresDocumentsStoredTypeIfCompletelyDifferentTypeRequested() {

		DBObject dbObject = new BasicDBObject();
		dbObject.put("birthDate", new LocalDate());
		dbObject.put(MappingMongoConverter.CUSTOM_TYPE_KEY, Person.class.getName());

		assertThat(converter.read(BirthDateContainer.class, dbObject), is(BirthDateContainer.class));
	}

	@Test
	public void writesTypeDiscriminatorIntoRootObject() {

		Person person = new Person();
		person.birthDate = new LocalDate();

		DBObject result = new BasicDBObject();
		converter.write(person, result);

		assertThat(result.containsField(MappingMongoConverter.CUSTOM_TYPE_KEY), is(true));
		assertThat(result.get(MappingMongoConverter.CUSTOM_TYPE_KEY).toString(), is(Person.class.getName()));
	}

	/**
	 * @see DATADOC-136
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
	 * @see DATADOC-136
	 */
	@Test
	public void readsEnumsCorrectly() {
		DBObject dbObject = new BasicDBObject("sampleEnum", "FIRST");
		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);
		
		assertThat(result.sampleEnum, is(SampleEnum.FIRST));
	}
	
	/**
	 * @see DATADOC-144
	 */
	@Test
	public void considersFieldNameWhenWriting() {
		
		Person person = new Person();
		person.firstname ="Oliver";
		
		DBObject result = new BasicDBObject();
		converter.write(person, result);
		
		assertThat(result.containsField("foo"), is(true));
		assertThat(result.containsField("firstname"), is(false));
	}
	
	/**
	 * @see DATADOC-144
	 */
	@Test
	public void considersFieldNameWhenReading() {
		
		DBObject dbObject = new BasicDBObject("foo", "Oliver");
		Person result = converter.read(Person.class, dbObject);
		
		assertThat(result.firstname, is("Oliver"));
	}
	
	/**
	 * @see DATADOC-145
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
		assertThat((String) personDbObject.get(MappingMongoConverter.CUSTOM_TYPE_KEY), is(Person.class.getName()));
	}
	
	/**
	 * @see DATADOC-145
	 */
	@Test
	public void readsCollectionWithInterfaceCorrectly() {
		
		BasicDBObject person = new BasicDBObject(MappingMongoConverter.CUSTOM_TYPE_KEY, Person.class.getName());
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
	 * @see DATADOC-161
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
	 * @see DATACMNS-42
	 */
	@Test
	public void writesClassWithBigDecimal() {
		
		BigDecimalContainer container = new BigDecimalContainer();
		container.value = BigDecimal.valueOf(2.5d);
		
		DBObject dbObject = new BasicDBObject();
		converter.write(container, dbObject);
		
		assertThat(dbObject.get("value"), is((Object) container.value));
	}
	
	/**
	 * @see DATACMNS-42
	 */
	@Test
	public void readsClassWithBigDecimal() {
		
		DBObject dbObject = new BasicDBObject("value", 2.5d);
		BigDecimalContainer result = converter.read(BigDecimalContainer.class, dbObject);
		
		assertThat(result.value, is(BigDecimal.valueOf(2.5d)));
	}
	
	@Test
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
	
	class ClassWithEnumProperty {
		
		SampleEnum sampleEnum;
	}
	
	enum SampleEnum {
		FIRST, SECOND;
	}
	
	class Address {
		String street;
		String city;
	}

	interface Contact {

	}

	class Person implements Contact {
		LocalDate birthDate;
		
		@FieldName("foo")
		String firstname;
	}

	class ClassWithMapProperty {
		Map<Locale, String> map;
	}

	class ClassWithNestedMaps {
    Map<String, Map<String, Map<String, String>>> nestedMaps;
	}
	
	class BirthDateContainer {
		LocalDate birthDate;
	}
	
	class BigDecimalContainer {
		BigDecimal value;
	}
	
	class CollectionWrapper {
		List<Contact> contacts;
		List<List<String>> strings;
	}

	class LocaleWrapper {
		Locale locale;
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
