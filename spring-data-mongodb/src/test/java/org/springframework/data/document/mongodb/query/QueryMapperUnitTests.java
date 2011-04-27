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
package org.springframework.data.document.mongodb.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.mapping.model.PersistentProperty;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link QueryMapper}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryMapperUnitTests {
  
  QueryMapper mapper;
  
  @Mock
  MongoConverter converter;
  @Mock
  PersistentEntity<?> entity;
  @Mock
  PersistentProperty property;

  @Before
  public void setUp() {
    when(entity.getIdProperty()).thenReturn(property);
    when(converter.convertObjectId(any())).thenReturn(new ObjectId());
    mapper = new QueryMapper(converter);
  }
  
  @Test
  public void translatesIdPropertyIntoIdKey() {
    
    DBObject query = new BasicDBObject("foo", "value");
    
    when(property.getName()).thenReturn("foo");
    
    DBObject result = mapper.getMappedObject(query, entity);
    assertThat(result.get("_id"), is(notNullValue()));
    assertThat(result.get("foo"), is(nullValue()));
  }
  
  @Test
  public void convertsStringIntoObjectId() {
    
    DBObject query = new BasicDBObject("_id", new ObjectId().toString());
    DBObject result = mapper.getMappedObject(query, null);
    assertThat(result.get("_id"), is(ObjectId.class));
  }
}
