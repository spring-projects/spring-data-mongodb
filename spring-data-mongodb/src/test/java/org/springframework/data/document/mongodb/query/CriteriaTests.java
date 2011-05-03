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

import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.document.mongodb.query.Index.Duplicates;

public class CriteriaTests {

  @Test
  public void testSimpleCriteria() {
    Criteria c = new Criteria("name").is("Bubba");
    Assert.assertEquals("{ \"name\" : \"Bubba\"}", c.getCriteriaObject().toString());
  }

  @Test
  public void testNotEqualCriteria() {
    Criteria c = new Criteria("name").ne("Bubba");
    Assert.assertEquals("{ \"name\" : { \"$ne\" : \"Bubba\"}}", c.getCriteriaObject().toString());
  }


  @Test
  public void testChainedCriteria() {
    Criteria c = new Criteria("name").is("Bubba").and("age").lt(21);
    Assert.assertEquals("{ \"name\" : \"Bubba\" , \"age\" : { \"$lt\" : 21}}", c.getCriteriaObject().toString());
  }
}
