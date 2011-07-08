/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.*;
import static org.springframework.data.mongodb.repository.StubParameterAccessor.*;

import java.lang.reflect.Method;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.data.mongodb.Person;
import org.springframework.data.mongodb.convert.MongoConverter;
import org.springframework.data.mongodb.query.Criteria;
import org.springframework.data.mongodb.query.Query;
import org.springframework.data.mongodb.repository.MongoQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit test for {@link MongoQueryCreator}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoQueryCreatorUnitTests {

	Method findByFirstname, findByFirstnameAndFriend, findByFirstnameNotNull;

	@Mock
	MongoConverter converter;

	@Before
	public void setUp() throws SecurityException, NoSuchMethodException {

		doAnswer(new Answer<Void>() {
			public Void answer(InvocationOnMock invocation) throws Throwable {
				DBObject dbObject = (DBObject) invocation.getArguments()[1];
				dbObject.put("value", new BasicDBObject("value", "value"));
				return null;
			}
		}).when(converter).write(any(), Mockito.any(DBObject.class));

	}

	@Test
	public void createsQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByFirstName", Person.class);

		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Oliver"));

		creator.createQuery();

		creator = new MongoQueryCreator(new PartTree("findByFirstNameAndFriend", Person.class), getAccessor(converter,
				"Oliver", new Person()));
		creator.createQuery();
	}

	@Test
	public void createsNotNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameNotNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter)).createQuery();

		assertThat(query.getQueryObject(), is(new Query(Criteria.where("firstName").ne(null)).getQueryObject()));
	}

	@Test
	public void createsIsNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameIsNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter)).createQuery();

		assertThat(query.getQueryObject(), is(new Query(Criteria.where("firstName").is(null)).getQueryObject()));
	}
}
