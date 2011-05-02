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
package org.springframework.data.document.mongodb.query;

import java.util.Collections;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.springframework.data.document.mongodb.convert.MongoConverter;

public class BasicUpdate extends Update {

  private DBObject updateObject = null;

  public BasicUpdate(String updateString) {
    super();
    this.updateObject = (DBObject) JSON.parse(updateString);
  }

  public BasicUpdate(DBObject updateObject) {
    super();
    this.updateObject = updateObject;
  }

  @Override
  public Update set(String key, Object value) {
    updateObject.put("$set", Collections.singletonMap(key, convertValueIfNecessary(value)));
    return this;
  }

  @Override
  public Update unset(String key) {
    updateObject.put("$unset", Collections.singletonMap(key, 1));
    return this;
  }

  @Override
  public Update inc(String key, Number inc) {
    updateObject.put("$inc", Collections.singletonMap(key, inc));
    return this;
  }

  @Override
  public Update push(String key, Object value) {
    updateObject.put("$push", Collections.singletonMap(key, convertValueIfNecessary(value)));
    return this;
  }

  @Override
  public Update pushAll(String key, Object[] values) {
    Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      convertedValues[i] = convertValueIfNecessary(values[i]);
    }
    DBObject keyValue = new BasicDBObject();
    keyValue.put(key, convertedValues);
    updateObject.put("$pushAll", keyValue);
    return this;
  }

  @Override
  public Update addToSet(String key, Object value) {
    updateObject.put("$addToSet", Collections.singletonMap(key, convertValueIfNecessary(value)));
    return this;
  }

  @Override
  public Update pop(String key, Position pos) {
    updateObject.put("$pop", Collections.singletonMap(key, (pos == Position.FIRST ? -1 : 1)));
    return this;
  }

  @Override
  public Update pull(String key, Object value) {
    updateObject.put("$pull", Collections.singletonMap(key, convertValueIfNecessary(value)));
    return this;
  }

  @Override
  public Update pullAll(String key, Object[] values) {
    Object[] convertedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      convertedValues[i] = convertValueIfNecessary(values[i]);
    }
    DBObject keyValue = new BasicDBObject();
    keyValue.put(key, convertedValues);
    updateObject.put("$pullAll", keyValue);
    return this;
  }

  @Override
  public Update rename(String oldName, String newName) {
    updateObject.put("$rename", Collections.singletonMap(oldName, newName));
    return this;
  }

  @Override
  public DBObject getUpdateObject() {
    return updateObject;
  }

}
