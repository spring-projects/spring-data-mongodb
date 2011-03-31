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

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class BasicQuery extends Query {

  private DBObject queryObject = null;

  private DBObject fieldsObject = null;

  private DBObject sortObject = null;

  private int skip;

  private int limit;

  public BasicQuery(String query) {
    super();
    this.queryObject = (DBObject) JSON.parse(query);
  }

  public BasicQuery(DBObject queryObject) {
    super();
    this.queryObject = queryObject;
  }

  public BasicQuery(String query, String fields) {
    this.queryObject = (DBObject) JSON.parse(query);
    this.fieldsObject = (DBObject) JSON.parse(fields);
  }

  public BasicQuery(DBObject queryObject, DBObject fieldsObject) {
    this.queryObject = queryObject;
    this.fieldsObject = fieldsObject;
  }

  @Override
  public Query addCriteria(Criteria criteria) {
    this.queryObject.putAll(criteria.getCriteriaObject());
    return this;
  }

  public DBObject getQueryObject() {
    return this.queryObject;
  }

  public DBObject getFieldsObject() {
    return fieldsObject;
  }

  public DBObject getSortObject() {
    return sortObject;
  }

  public void setSortObject(DBObject sortObject) {
    this.sortObject = sortObject;
  }

  public int getSkip() {
    return skip;
  }

  public void setSkip(int skip) {
    this.skip = skip;
  }

  public int getLimit() {
    return this.limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

}
