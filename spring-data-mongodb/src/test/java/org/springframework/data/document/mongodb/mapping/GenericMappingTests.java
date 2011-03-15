package org.springframework.data.document.mongodb.mapping;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.mapping.BasicMappingContext;
import org.springframework.data.mapping.model.MappingContext;

/**
 * Unit tests for testing the mapping works with generic types.
 *
 * @author Oliver Gierke
 */
public class GenericMappingTests {

  MappingContext context;
  MongoConverter converter;

  @Before
  public void setUp() {
    context = new BasicMappingContext(new MongoMappingConfigurationBuilder(null));
    context.addPersistentEntity(StringWrapper.class);
    converter = new MappingMongoConverter(context);
  }

  @Test
  public void writesGenericTypeCorrectly() {

    StringWrapper wrapper = new StringWrapper();
    wrapper.container = new Container<String>();
    wrapper.container.content = "Foo!";

    context.addPersistentEntity(StringWrapper.class);

    DBObject dbObject = new BasicDBObject();
    converter.write(wrapper, dbObject);

    Object container = dbObject.get("container");
    assertThat(container, is(notNullValue()));
    assertTrue(container instanceof DBObject);

    Object content = ((DBObject) container).get("content");
    assertTrue(content instanceof String);
    assertThat((String) content, is("Foo!"));
  }

  @Test
  public void readsGenericTypeCorrectly() {

    DBObject content = new BasicDBObject("content", "Foo!");
    BasicDBObject container = new BasicDBObject("container", content);

    StringWrapper result = converter.read(StringWrapper.class, container);
    assertThat(result.container, is(notNullValue()));
    assertThat(result.container.content, is("Foo!"));
  }

  public class StringWrapper extends Wrapper<String> {

  }

  public class Wrapper<S> {
    Container<S> container;
  }

  public class Container<T> {
    T content;
  }
}
