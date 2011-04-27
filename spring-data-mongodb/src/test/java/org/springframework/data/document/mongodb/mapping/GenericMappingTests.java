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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.convert.MongoConverter;

/**
 * Unit tests for testing the mapping works with generic types.
 *
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class GenericMappingTests {

  MongoMappingContext context;
  MongoConverter converter;

  @Before
  public void setUp() throws Exception {
    context = new MongoMappingContext();
    context.setInitialEntitySet(Collections.singleton(StringWrapper.class));
    context.afterPropertiesSet();
    
    converter = new MappingMongoConverter(context);
  }

  @Test
  public void writesGenericTypeCorrectly() {

    StringWrapper wrapper = new StringWrapper();
    wrapper.container = new Container<String>();
    wrapper.container.content = "Foo!";

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
