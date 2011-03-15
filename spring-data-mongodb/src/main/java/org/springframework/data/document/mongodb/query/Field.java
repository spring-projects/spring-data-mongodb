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

import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class Field {

  private Map<String, Integer> criteria = new HashMap<String, Integer>();

  private Map<String, Object> slices = new HashMap<String, Object>();

  public Field include(String key) {
    criteria.put(key, Integer.valueOf(1));
    return this;
  }

  public Field exclude(String key) {
    criteria.put(key, Integer.valueOf(0));
    return this;
  }

  public Field slice(String key, int size) {
    slices.put(key, Integer.valueOf(size));
    return this;
  }

  public Field slice(String key, int offset, int size) {
    slices.put(key, new Integer[]{Integer.valueOf(offset), Integer.valueOf(size)});
    return this;
  }

  public DBObject getFieldsObject() {
    DBObject dbo = new BasicDBObject();
    for (String k : criteria.keySet()) {
      dbo.put(k, (criteria.get(k)));
    }
    for (String k : slices.keySet()) {
      dbo.put(k, new BasicDBObject("$slice", (slices.get(k))));
    }
    return dbo;
  }
}
