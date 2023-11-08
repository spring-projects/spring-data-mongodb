/*
 * Copyright 2011-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Objects;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.User;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

/**
 * Unit tests for {@link QuerydslRepositorySupport}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
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
		assertThat(query.fetchOne()).isEqualTo(person);
	}

	@Test // DATAMONGO-1063
	public void shouldAllowAny() {

		person.setSkills(Arrays.asList("vocalist", "songwriter", "guitarist"));

		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> query = repoSupport.from(p).where(p.skills.any().in("guitarist"));

		assertThat(query.fetchOne()).isEqualTo(person);
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

		assertThat(queryUsingIdField.fetchOne()).isEqualTo(person);
		assertThat(queryUsingIdField.fetchOne()).isEqualTo(queryUsingRefObject.fetchOne());
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

		assertThat(query.fetchOne()).isEqualTo(outer);
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

		assertThat(query.fetchOne()).isEqualTo(outer);
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

		assertThat(queryUsingIdFieldWithinInClause.fetchOne()).isEqualTo(person);
		assertThat(queryUsingIdFieldWithinInClause.fetchOne()).isEqualTo(queryUsingRefObject.fetchOne());
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

		assertThat(queryUsingIdFieldWithinInClause.fetchOne()).isEqualTo(person);
		assertThat(queryUsingIdFieldWithinInClause.fetchOne()).isEqualTo(queryUsingRefObject.fetchOne());
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

		assertThat(query.fetchOne()).isEqualTo(outer);
	}

	@Test // DATAMONGO-1798
	public void shouldRetainIdPropertyTypeIfInvalidObjectId() {

		Outer outer = new Outer();
		outer.id = "foobar";

		operations.save(outer);

		QQuerydslRepositorySupportTests_Outer o = QQuerydslRepositorySupportTests_Outer.outer;
		SpringDataMongodbQuery<Outer> query = repoSupport.from(o).where(o.id.eq(outer.id));

		assertThat(query.fetchOne()).isEqualTo(outer);
	}

	@Test // DATAMONGO-1798
	public void shouldUseStringForValidObjectIdHexStrings() {

		WithMongoId document = new WithMongoId();
		document.id = new ObjectId().toHexString();

		operations.save(document);

		QQuerydslRepositorySupportTests_WithMongoId o = QQuerydslRepositorySupportTests_WithMongoId.withMongoId;
		SpringDataMongodbQuery<WithMongoId> eqQuery = repoSupport.from(o).where(o.id.eq(document.id));

		assertThat(eqQuery.fetchOne()).isEqualTo(document);

		SpringDataMongodbQuery<WithMongoId> inQuery = repoSupport.from(o).where(o.id.in(document.id));

		assertThat(inQuery.fetchOne()).isEqualTo(document);
	}

	@Test // DATAMONGO-2327
	public void toJsonShouldRenderQuery() {

		QPerson p = QPerson.person;
		SpringDataMongodbQuery<Person> query = repoSupport.from(p).where(p.lastname.eq("Matthews"))
				.orderBy(p.firstname.asc()).offset(1).limit(5);

		assertThat(StringUtils.trimAllWhitespace(query.toJson())).isEqualTo("{\"lastname\":\"Matthews\"}");
	}

	@Test // DATAMONGO-2327
	public void toStringShouldRenderQuery() {

		QPerson p = QPerson.person;
		User user = new User();
		user.setId("id");
		SpringDataMongodbQuery<Person> query = repoSupport.from(p)
				.where(p.lastname.eq("Matthews").and(p.coworker.eq(user)));

		assertThat(StringUtils.trimAllWhitespace(query.toString()))
				.isEqualTo("find({\"lastname\":\"Matthews\",\"coworker\":{\"$ref\":\"user\",\"$id\":\"id\"}})");

		query = query.orderBy(p.firstname.asc());
		assertThat(StringUtils.trimAllWhitespace(query.toString())).isEqualTo(
				"find({\"lastname\":\"Matthews\",\"coworker\":{\"$ref\":\"user\",\"$id\":\"id\"}}).sort({\"firstname\":1})");

		query = query.offset(1).limit(5);
		assertThat(StringUtils.trimAllWhitespace(query.toString())).isEqualTo(
				"find({\"lastname\":\"Matthews\",\"coworker\":{\"$ref\":\"user\",\"$id\":\"id\"}}).sort({\"firstname\":1}).skip(1).limit(5)");
	}

	@Document
	public static class Outer {

		@Id String id;
		Inner inner;

		public String getId() {
			return this.id;
		}

		public Inner getInner() {
			return this.inner;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setInner(Inner inner) {
			this.inner = inner;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Outer outer = (Outer) o;
			return Objects.equals(id, outer.id) && Objects.equals(inner, outer.inner);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, inner);
		}

		public String toString() {
			return "QuerydslRepositorySupportTests.Outer(id=" + this.getId() + ", inner=" + this.getInner() + ")";
		}
	}

	public static class Inner {

		@Id String id;
		String value;

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Inner inner = (Inner) o;
			return Objects.equals(id, inner.id) && Objects.equals(value, inner.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "QuerydslRepositorySupportTests.Inner(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	@Document
	public static class WithMongoId {

		@MongoId(FieldType.STRING) String id;

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WithMongoId that = (WithMongoId) o;
			return Objects.equals(id, that.id);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id);
		}

		public String toString() {
			return "QuerydslRepositorySupportTests.WithMongoId(id=" + this.getId() + ")";
		}
	}
}
