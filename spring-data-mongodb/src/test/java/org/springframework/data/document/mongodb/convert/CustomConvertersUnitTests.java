package org.springframework.data.document.mongodb.convert;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.mapping.MongoMappingContext;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class CustomConvertersUnitTests {

  MappingMongoConverter converter;
  
  @Mock
  BarToDBObjectConverter barToDBObjectConverter;
  @Mock
  DBObjectToBarConverter dbObjectToBarConverter;
  
  MongoMappingContext context;
  MongoPersistentEntity<Foo> fooEntity;
  MongoPersistentEntity<Bar> barEntity;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    
    context = new MongoMappingContext();
    context.addPersistentEntity(Foo.class);
    context.addPersistentEntity(Bar.class);
    
    when(barToDBObjectConverter.convert(any(Bar.class))).thenReturn(new BasicDBObject());
    when(dbObjectToBarConverter.convert(any(DBObject.class))).thenReturn(new Bar());
    
    converter = new MappingMongoConverter(context);
    converter.setConverters(Arrays.asList(barToDBObjectConverter, dbObjectToBarConverter));
  }

  @Test
  public void nestedToDBObjectConverterGetsInvoked() {
    
    Foo foo = new Foo();
    foo.bar = new Bar();
    
    converter.write(foo, new BasicDBObject());
    verify(barToDBObjectConverter).convert(any(Bar.class));
  }
  
  @Test
  public void nestedFromDBObjectConverterGetsInvoked() {
    
    BasicDBObject dbObject = new BasicDBObject();
    dbObject.put("bar", new BasicDBObject());
    
    converter.read(Foo.class, dbObject);
    verify(dbObjectToBarConverter).convert(any(DBObject.class));
  }
  
  @Test
  public void toDBObjectConverterGetsInvoked() {
    
    converter.write(new Bar(), new BasicDBObject());
    verify(barToDBObjectConverter).convert(any(Bar.class));
  }
  
  @Test
  public void fromDBObjectConverterGetsInvoked() {
    
    converter.read(Bar.class, new BasicDBObject());
    verify(dbObjectToBarConverter).convert(any(DBObject.class));
  }

  public static class Foo {
    public Bar bar;
  }
  
  public static class Bar {
    public String foo;
  }
  
  private interface BarToDBObjectConverter extends Converter<Bar, DBObject> {

  }

  private interface DBObjectToBarConverter extends Converter<DBObject, Bar> {

  }
}
