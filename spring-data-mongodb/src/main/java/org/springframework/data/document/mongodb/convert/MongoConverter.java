/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb.convert;

import com.mongodb.BasicDBList;
import org.bson.types.ObjectId;
import org.springframework.data.document.mongodb.MongoReader;
import org.springframework.data.document.mongodb.MongoWriter;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.MongoPersistentProperty;
import org.springframework.data.mapping.model.MappingContext;


public interface MongoConverter extends MongoWriter<Object>, MongoReader<Object> {

  /**
   * Converts the given {@link ObjectId} to the given target type.
   *
   * @param <T>        the actual type to create
   * @param id         the source {@link ObjectId}
   * @param targetType the target type to convert the {@link ObjectId} to
   * @return
   */
  public <T> T convertObjectId(ObjectId id, Class<T> targetType);


  /**
   * Returns the {@link ObjectId} instance for the given id.
   *
   * @param id
   * @return
   */
  public ObjectId convertObjectId(Object id);
  
  MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext();

	Object maybeConvertObject(Object obj);

	Object[] maybeConvertArray(Object[] src);

	BasicDBList maybeConvertList(BasicDBList dbl);
}
