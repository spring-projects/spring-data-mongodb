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
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    
    List<Converter<?, ?>> converters = new ArrayList<Converter<?,?>>();
    converters.add(new LocalDateToDateConverter());
    converters.add(new DateToLocalDateConverter());
    
    List<Class<?>> customSimpleTypes = new ArrayList<Class<?>>();
    customSimpleTypes.add(LocalDate.class);
    mappingContext.setCustomSimpleTypes(customSimpleTypes);
    
    converter = new MappingMongoConverter(mappingContext);
    converter.setConverters(converters);
    converter.afterPropertiesSet();
    
    Person person = new Person();
    person.birthDate = new LocalDate();
    
    DBObject dbObject = new BasicDBObject();
    converter.write(person, dbObject);
    
    assertTrue(dbObject.get("birthDate") instanceof Date);
    
    Person result = converter.read(Person.class, dbObject);
    assertThat(result.birthDate, is(notNullValue()));
  }

  /**
   * @see DATADOC-130
   */
  @Test
  public void convertsMapTypeCorrectly() {
    
    Map<Locale, String> map = Collections.singletonMap(Locale.US, "Foo");
    
    BasicDBObject dbObject = new BasicDBObject();
    converter.write(map, dbObject);
    
    assertThat(dbObject.get(Locale.US.toString()).toString(), is("Foo"));
  }
  
  public static class Address {
    String street;
    String city;
  }
  
  public static class Person {
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
