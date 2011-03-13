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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.mapping.BasicMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
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
  BasicMappingContext mappingContext;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void setUp() {
    template.dropCollection("person");
    template.dropCollection("account");
  }

  @Test
  public void testPersonPojo() {
    PersonPojo p = new PersonPojo(12345, "Person", "Pojo");
    template.insert(p);

    assertNotNull(p.getId());
  }

  @Test
  public void testPersonWithCustomIdName() {
    PersonCustomIdName p = new PersonCustomIdName(123456, "Custom", "Id");
    template.insert(p);

    List<PersonCustomIdName> result = template.find(new Query(Criteria.where("ssn").is(123456)), PersonCustomIdName.class);
    assertThat(result.size(), is(1));
  }

  @Test
  public void testPersonMapProperty() {
    PersonMapProperty p = new PersonMapProperty(1234567, "Map", "Property");
    Map<String, AccountPojo> accounts = new HashMap<String, AccountPojo>();

    AccountPojo checking = new AccountPojo("checking", 1000.0f);
    AccountPojo savings = new AccountPojo("savings", 10000.0f);

    accounts.put("checking", checking);
    accounts.put("savings", savings);
    p.setAccounts(accounts);

    template.insert(p);
    assertNotNull(p.getId());

    List<PersonMapProperty> result = template.find(new Query(Criteria.where("ssn").is(1234567)), PersonMapProperty.class);
    assertThat(result.size(), is(1));
    assertThat(result.get(0).getAccounts().size(), is(2));
    assertNotNull(result.get(0).getAccounts().get("checking"));
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testWriteEntity() {
    Person p = new Person(123456789, "John", "Doe", 37);

    Address addr = new Address();
    addr.setLines(new String[]{"1234 W. 1st Street", "Apt. 12"});
    addr.setCity("Anytown");
    addr.setPostalCode(12345);
    addr.setCountry("USA");
    p.setAddress(addr);

    Account acct = new Account();
    acct.setBalance(1000.00f);
    template.insert("account", acct);

    List<Account> accounts = new ArrayList<Account>();
    accounts.add(acct);
    p.setAccounts(accounts);

    template.insert("person", p);

    assertNotNull(p.getId());
  }

  @Test
  public void testReadEntity() {
    List<Person> result = template.find(new Query(Criteria.where("ssn").is(123456789)), Person.class);
    assertThat(result.size(), is(1));
    assertThat(result.get(0).getAddress().getCountry(), is("USA"));
    assertThat(result.get(0).getAccounts(), notNullValue());
  }
}
