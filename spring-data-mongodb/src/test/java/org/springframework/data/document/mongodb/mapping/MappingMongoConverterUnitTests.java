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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
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

	@Before
	public void setUp() {
		mappingContext = new MongoMappingContext();
		converter = new MappingMongoConverter(mappingContext);
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

		converter = new MappingMongoConverter(mappingContext);
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
	
	
	class ClassWithEnumProperty {
		
		SampleEnum sampleEnum;
	}
	
	enum SampleEnum {
		FIRST, SECOND;
	}
	
	public static class Address {
		String street;
		String city;
	}

	interface Contact {

	}

	public static class Person implements Contact {
		LocalDate birthDate;
	}

	static class ClassWithMapProperty {
		Map<Locale, String> map;
	}

	public static class BirthDateContainer {
		LocalDate birthDate;
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
