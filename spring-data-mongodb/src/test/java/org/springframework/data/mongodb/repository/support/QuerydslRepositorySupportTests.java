/*
 * Copyright 2011-2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import lombok.Data;

import java.util.Arrays;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.User;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Unit tests for {@link QuerydslRepositorySupport}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class QuerydslRepositorySupportTests {

	@Autowired MongoOperations operations;
	Person person;
	QuerydslRepositorySupport repoSupport;

	@Before
	public void setUp() {

		operations.remove(new Query(), Outer.class);
		operations.remove(new Query(), Person.class);

		person = new Person("Dave", "Matthews");
		operations.save(person);

		repoSupport = new QuerydslRepositorySupport(operations) {};
	}

	@Test
	public void providesMongoQuery() {

		QPerson p = QPerson.person;
		QuerydslRepositorySupport support = new QuerydslRepositorySupport(operations) {};
		SpringDataMongodbQuery<Person> query = support.from(p).where(p.lastname.eq("Matthews"));
		assertThat(query.fetchOne(), is(person));
	}

	@Test // DATAMONGO-1063
	public void shouldAllowAny() {

		person.setSkills(Arrays.asList("vocalist", "songwriter", "guitarist"));

		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> query = repoSupport.from(p).where(p.skills.any().in("guitarist"));

		assertThat(query.fetchOne(), is(person));
	}

	@Test // DATAMONGO-1394
	public void shouldAllowDbRefAgainstIdProperty() {

		User bart = new User();
		bart.setUsername("bart@simpson.com");
		operations.save(bart);

		person.setCoworker(bart);
		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> queryUsingIdField = repoSupport.from(p).where(p.coworker.id.eq(bart.getId()));
		SpringDataMongodbQuery<Person> queryUsingRefObject = repoSupport.from(p).where(p.coworker.eq(bart));

		assertThat(queryUsingIdField.fetchOne(), equalTo(person));
		assertThat(queryUsingIdField.fetchOne(), equalTo(queryUsingRefObject.fetchOne()));
	}

	@Test // DATAMONGO-1998
	public void shouldLeaveStringIdThatIsNoValidObjectIdAsItIs() {

		Outer outer = new Outer();
		outer.id = "outer-1";
		outer.inner = new Inner();
		outer.inner.id = "inner-1";
		outer.inner.value = "go climb a rock";

		operations.save(outer);

		QQuerydslRepositorySupportTests_Outer o = QQuerydslRepositorySupportTests_Outer.outer;
		SpringDataMongodbQuery<Outer> query = repoSupport.from(o).where(o.inner.id.eq(outer.inner.id));

		assertThat(query.fetchOne(), equalTo(outer));
	}

	@Test // DATAMONGO-1998
	public void shouldConvertStringIdThatIsAValidObjectIdIntoTheSuch() {

		Outer outer = new Outer();
		outer.id = new ObjectId().toHexString();
		outer.inner = new Inner();
		outer.inner.id = new ObjectId().toHexString();
		outer.inner.value = "eat sleep workout repeat";

		operations.save(outer);

		QQuerydslRepositorySupportTests_Outer o = QQuerydslRepositorySupportTests_Outer.outer;
		SpringDataMongodbQuery<Outer> query = repoSupport.from(o).where(o.inner.id.eq(outer.inner.id));

		assertThat(query.fetchOne(), equalTo(outer));
	}

	@Test // DATAMONGO-1810, DATAMONGO-1848
	public void shouldFetchObjectsViaStringWhenUsingInOnDbRef() {

		User bart = new User();
		DirectFieldAccessor dfa = new DirectFieldAccessor(bart);
		dfa.setPropertyValue("id", "bart");

		bart.setUsername("bart@simpson.com");
		operations.save(bart);

		User lisa = new User();
		dfa = new DirectFieldAccessor(lisa);
		dfa.setPropertyValue("id", "lisa");

		lisa.setUsername("lisa@simposon.com");
		operations.save(lisa);

		person.setCoworker(bart);
		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> queryUsingIdFieldWithinInClause = repoSupport.from(p)
				.where(p.coworker.id.in(Arrays.asList(bart.getId(), lisa.getId())));

		SpringDataMongodbQuery<Person> queryUsingRefObject = repoSupport.from(p).where(p.coworker.eq(bart));

		assertThat(queryUsingIdFieldWithinInClause.fetchOne(), equalTo(person));
		assertThat(queryUsingIdFieldWithinInClause.fetchOne(), equalTo(queryUsingRefObject.fetchOne()));
	}

	@Test // DATAMONGO-1810, DATAMONGO-1848
	public void shouldFetchObjectsViaStringStoredAsObjectIdWhenUsingInOnDbRef() {

		User bart = new User();
		bart.setUsername("bart@simpson.com");
		operations.save(bart);

		User lisa = new User();
		lisa.setUsername("lisa@simposon.com");
		operations.save(lisa);

		person.setCoworker(bart);
		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> queryUsingIdFieldWithinInClause = repoSupport.from(p)
				.where(p.coworker.id.in(Arrays.asList(bart.getId(), lisa.getId())));

		SpringDataMongodbQuery<Person> queryUsingRefObject = repoSupport.from(p).where(p.coworker.eq(bart));

		assertThat(queryUsingIdFieldWithinInClause.fetchOne(), equalTo(person));
		assertThat(queryUsingIdFieldWithinInClause.fetchOne(), equalTo(queryUsingRefObject.fetchOne()));
	}

	@Test // DATAMONGO-1848, DATAMONGO-2010
	public void shouldConvertStringIdThatIsAValidObjectIdWhenUsedInInPredicateIntoTheSuch() {

		Outer outer = new Outer();
		outer.id = new ObjectId().toHexString();
		outer.inner = new Inner();
		outer.inner.id = new ObjectId().toHexString();
		outer.inner.value = "eat sleep workout repeat";

		operations.save(outer);

		QQuerydslRepositorySupportTests_Outer o = QQuerydslRepositorySupportTests_Outer.outer;
		SpringDataMongodbQuery<Outer> query = repoSupport.from(o).where(o.inner.id.in(outer.inner.id, outer.inner.id));

		assertThat(query.fetchOne(), equalTo(outer));
	}

	@Data
	@Document
	public static class Outer {

		@Id String id;
		Inner inner;
	}

	@Data
	public static class Inner {

		@Id String id;
		String value;
	}
}
