/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Integration test for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:template-mapping.xml")
public class MongoTemplateMappingTests {

  @Autowired
  @Qualifier("mongoTemplate1")
  MongoTemplate template1;
  
  @Autowired
  @Qualifier("mongoTemplate2")
  MongoTemplate template2;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

@Before
  public void setUp() {
	template1.dropCollection(template1.getCollectionName(Person.class));
  }

  @Test
  public void insertsEntityCorrectly1() throws Exception {

	    addAndRetrievePerson(template1);
	    checkPersonPersisted(template1);

  }
  
  @Test
  public void insertsEntityCorrectly2() throws Exception {

    addAndRetrievePerson(template2);
    checkPersonPersisted(template2);
    
  }

  private void addAndRetrievePerson(MongoTemplate template) {
		Person person = new Person("Oliver");
	    person.setAge(25);
	    template.insert(person);

	    List<Person> result = template.find(new Query(Criteria.where("_id").is(person.getId())), Person.class);
	    assertThat(result.size(), is(1));
	    assertThat(result, hasItem(person));
	    assertThat(result.get(0).getFirstName(), is("Oliver"));
	    assertThat(result.get(0).getAge(), is(25));
  }
  
  private void checkPersonPersisted(MongoTemplate template) {
	    template.execute(Person.class, new CollectionCallback<Object>() {
				public Object doInCollection(DBCollection collection)
						throws MongoException, DataAccessException {
					DBObject dbo = collection.findOne();
					assertThat((String)dbo.get("name"), is("Oliver"));
					return null;
				}
		    });
  }

}
