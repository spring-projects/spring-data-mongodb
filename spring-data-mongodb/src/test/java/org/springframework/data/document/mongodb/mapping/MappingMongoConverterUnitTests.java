package org.springframework.data.document.mongodb.mapping;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.mapping.BasicMappingContext;
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
  BasicMappingContext mappingContext;
  
  @Mock
  ApplicationContext applicationContext;
  
  @Before
  public void setUp() {
    mappingContext = new BasicMappingContext();
    mappingContext.setApplicationContext(applicationContext);
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
    
    converter = new MappingMongoConverter(mappingContext, converters);
    
    Person person = new Person();
    person.birthDate = new LocalDate();
    
    DBObject dbObject = new BasicDBObject();
    converter.write(person, dbObject);
    
    assertTrue(dbObject.get("birthDate") instanceof Date);
    
    Person result = converter.read(Person.class, dbObject);
    assertThat(result.birthDate, is(notNullValue()));
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
