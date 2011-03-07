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

package org.springframework.data.document.mongodb.mapping.index;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.springframework.data.document.mongodb.index.IndexDefinition;
import org.springframework.data.document.mongodb.index.IndexDirection;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoIndexDefinition implements IndexDefinition<DBObject> {

  private String collection = null;
  private String name = null;
  private IndexDirection direction = IndexDirection.ASCENDING;
  private boolean unique = false;
  private boolean dropDups = true;
  private boolean sparse = false;
  private String definition = null;

  public MongoIndexDefinition() {
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public IndexDirection getDirection() {
    return direction;
  }

  public void setDirection(IndexDirection direction) {
    this.direction = direction;
  }

  public boolean isUnique() {
    return unique;
  }

  public void setUnique(boolean unique) {
    this.unique = unique;
  }

  public boolean isDropDups() {
    return dropDups;
  }

  public void setDropDups(boolean dropDups) {
    this.dropDups = dropDups;
  }

  public boolean isSparse() {
    return sparse;
  }

  public void setSparse(boolean sparse) {
    this.sparse = sparse;
  }

  public String getDefinition() {
    return definition;
  }

  public void setDefinition(String definition) {
    this.definition = definition;
  }

  public DBObject getIndexDefinition() {
    DBObject dbo;
    if (null != definition) {
      dbo = (DBObject) JSON.parse(definition);
    } else {
      dbo = new BasicDBObject();
      dbo.put(name, (direction == IndexDirection.ASCENDING ? 1 : -1));
    }
    return dbo;
  }

  public DBObject getIndexOptions() {
    DBObject dbo = new BasicDBObject();
    dbo.put("dropDups", dropDups);
    dbo.put("sparse", sparse);
    dbo.put("unique", unique);
    return dbo;
  }

}
