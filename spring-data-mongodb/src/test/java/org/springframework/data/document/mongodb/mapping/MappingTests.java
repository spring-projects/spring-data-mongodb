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

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Index;
import org.springframework.data.document.mongodb.query.IndexDefinition;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertThat;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:mapping.xml")
public class MappingTests {

  @Autowired
  MongoTemplate template;
  @Autowired
  MongoMappingContext mappingContext;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void setUp() {
    template.dropCollection(template.getDefaultCollectionName());
    mappingContext.addPersistentEntity(Person.class);
  }

  @Test
  public void testWrite() {
    Person p = new Person(123456789, "John", "Doe", 37);

    Address addr = new Address();
    addr.setLines(new String[]{"1234 W. 1st Street", "Apt. 12"});
    addr.setCity("Anytown");
    addr.setPostalCode(12345);
    addr.setCountry("USA");
    p.setAddress(addr);

    Account acct = new Account();
    acct.setBalance(1000.00f);
    List<Account> accounts = new ArrayList<Account>();
    accounts.add(acct);
    p.setAccounts(accounts);

    template.insert(p);
  }

  @Test
  public void testRead() {
    MongoConverter converter = template.getConverter();

    List<Person> result = template.find(new Query(Criteria.where("ssn").is(123456789)), Person.class);
    assertThat(result.size(), is(1));
    assertThat(result.get(0).getAddress().getCountry(), is("USA"));
  }
}
