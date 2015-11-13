/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import com.mysema.query.types.Predicate;
import java.util.Arrays;
import java.util.List;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.event.SimpleMappingEventListener;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link QueryDslMongoRepository}.
 * 
 * @author Thomas Darimont
 * @author Jordi Llach
 */
@ContextConfiguration(
		locations = "/org/springframework/data/mongodb/repository/PersonRepositoryIntegrationTests-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class QueryDslMongoRepositoryIntegrationTests {
    
    private static final String PERSON_COLLECTION_NAME = "person";
    private static final String USER_COLLECTION_NAME = "user";

	@Autowired MongoOperations operations;
    @Autowired ApplicationContext appContext;
	QueryDslMongoRepository<Person, String> repository;
    QueryDslMongoRepository<User, String>   repositoryUser;

	Person dave, oliver, carter;
    User creator, coworker;
	QPerson person;

	@Before
	public void setup() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(operations, appContext);
		MongoEntityInformation<Person, String> entityInformation     = factory.getEntityInformation(Person.class);
        MongoEntityInformation<User, String>   entityUserInformation = factory.getEntityInformation(User.class);
		repository     = new QueryDslMongoRepository<Person, String>(entityInformation, operations, appContext);
        repositoryUser = new QueryDslMongoRepository<User, String>(entityUserInformation, operations, appContext);

		operations.dropCollection(Person.class);
        operations.dropCollection(User.class);

		dave = new Person("Dave", "Matthews", 42);
		oliver = new Person("Oliver August", "Matthews", 4);
		carter = new Person("Carter", "Beauford", 49);

        creator = new User();
        creator.setId("creator");
        coworker = new User();
        coworker.setId("coworker");
        
		person = new QPerson("person");

        repositoryUser.save(Arrays.asList(creator, coworker));
        dave.setCoworker(coworker);
        dave.setCreator(creator);
		repository.save(Arrays.asList(oliver, dave, carter));
	}

	/**
	 * @see DATAMONGO-1146
	 */
	@Test
	public void shouldSupportExistsWithPredicate() throws Exception {

		assertThat(repository.exists(person.firstname.eq("Dave")), is(true));
		assertThat(repository.exists(person.firstname.eq("Unknown")), is(false));
		assertThat(repository.exists((Predicate) null), is(true));
	}

	/**
	 * @see DATAMONGO-1167
	 */
	@Test
	public void shouldSupportFindAllWithPredicateAndSort() {

		List<Person> users = repository.findAll(person.lastname.isNotNull(), new Sort(Direction.ASC, "firstname"));

		assertThat(users, hasSize(3));
		assertThat(users.get(0).getFirstname(), is(carter.getFirstname()));
		assertThat(users.get(2).getFirstname(), is(oliver.getFirstname()));
		assertThat(users, hasItems(carter, dave, oliver));
	}
    
    /**
     * @see DATAMONGO-1185
     * when merged with DATAMONGO-1271 this test will change
     */
    @Test
    public void testValidateQueryDslCallBacks() {
        SimpleMappingEventListener eventListener = (SimpleMappingEventListener)appContext.getBean("eventListener");
        eventListener.onAfterLoadEvents.clear();
        eventListener.onAfterConvertEvents.clear();
        assertThat(eventListener.onAfterLoadEvents.size(), is(0));
        assertThat(eventListener.onAfterConvertEvents.size(), is(0));
        Person findOne = repository.findOne(person.firstname.eq("Dave").and(person.lastname.eq("Matthews")));
        // remove two lines below post DATAMONGO-1271 merge
        assertThat(eventListener.onAfterLoadEvents.size(), is(1));
        assertThat(eventListener.onAfterConvertEvents.size(), is(1));
        // uncomment the following lines post DATAMONGO-1271 merge
//        assertThat(findOne.getCreator().getId(), is(creator.getId()));
//        assertThat(eventListener.onAfterLoadEvents.size(), is(2));
//        assertThat(eventListener.onAfterConvertEvents.size(), is(2));
//        
//        assertEquals(PERSON_COLLECTION_NAME, eventListener.onAfterLoadEvents.get(0).getCollectionName());
//        assertEquals(PERSON_COLLECTION_NAME, eventListener.onAfterConvertEvents.get(0).getCollectionName());
//        assertEquals(USER_COLLECTION_NAME, eventListener.onAfterLoadEvents.get(1).getCollectionName());
//        assertEquals(USER_COLLECTION_NAME, eventListener.onAfterConvertEvents.get(1).getCollectionName());
//        // is lazy so it triggers new events
//        assertThat(findOne.getCoworker().getId(), is("coworker"));
//        assertThat(eventListener.onAfterLoadEvents.size(), is(3));
//        assertThat(eventListener.onAfterConvertEvents.size(), is(3));
//        assertEquals(USER_COLLECTION_NAME, eventListener.onAfterLoadEvents.get(2).getCollectionName());
//        assertEquals(USER_COLLECTION_NAME, eventListener.onAfterLoadEvents.get(2).getCollectionName());
    }
}
