/*
 * Copyright 2011-2016 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.domain.Sort.Direction;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

/**
 * Unit tests for {@link BasicQuery}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author John Willemin
 */
public class BasicQueryUnitTests {

	@Test
	public void createsQueryFromPlainJson() {
		Query q = new BasicQuery("{ \"name\" : \"Thomas\"}");
		Document reference = new Document("name", "Thomas");
		assertThat(q.getQueryObject(), is(reference));
	}

	@Test
	public void addsCriteriaCorrectly() {
		Query q = new BasicQuery("{ \"name\" : \"Thomas\"}").addCriteria(where("age").lt(80));
		Document reference = new Document("name", "Thomas");
		reference.put("age", new Document("$lt", 80));
		assertThat(q.getQueryObject(), is(reference));
	}

	@Test
	public void overridesSortCorrectly() {

		BasicQuery query = new BasicQuery("{}");
		query.setSortObject(new Document("name", -1));
		query.with(new org.springframework.data.domain.Sort(Direction.ASC, "lastname"));

		Document sortReference = new Document("name", -1);
		sortReference.put("lastname", 1);
		assertThat(query.getSortObject(), is(sortReference));
	}

	/**
	 * @see DATAMONGO-1093
	 */
	@Test
	public void equalsContract() {

		BasicQuery query1 = new BasicQuery("{ \"name\" : \"Thomas\"}", "{\"name\":1, \"age\":1}");
		query1.setSortObject(new Document("name", -1));

		BasicQuery query2 = new BasicQuery("{ \"name\" : \"Oliver\"}", "{\"name\":1, \"address\":1}");
		query2.setSortObject(new Document("name", 1));

		EqualsVerifier.forExamples(query1, query2) //
				.withRedefinedSuperclass() //
				.suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS, Warning.STRICT_INHERITANCE) //
				.verify();
	}

	/**
	 * @see DATAMONGO-1093
	 */
	@Test
	public void handlesEqualsAndHashCodeCorrectlyForExactCopies() {

		String qry = "{ \"name\" : \"Thomas\"}";
		String fields = "{\"name\":1, \"age\":1}";

		BasicQuery query1 = new BasicQuery(qry, fields);
		query1.setSortObject(new Document("name", -1));

		BasicQuery query2 = new BasicQuery(qry, fields);
		query2.setSortObject(new Document("name", -1));

		assertThat(query1, is(equalTo(query1)));
		assertThat(query1, is(equalTo(query2)));
		assertThat(query1.hashCode(), is(query2.hashCode()));
	}

	/**
	 * @see DATAMONGO-1093
	 */
	@Test
	public void handlesEqualsAndHashCodeCorrectlyWhenBasicQuerySettingsDiffer() {

		String qry = "{ \"name\" : \"Thomas\"}";
		String fields = "{\"name\":1, \"age\":1}";

		BasicQuery query1 = new BasicQuery(qry, fields);
		query1.setSortObject(new Document("name", -1));

		BasicQuery query2 = new BasicQuery(qry, fields);
		query2.setSortObject(new Document("name", 1));

		assertThat(query1, is(not(equalTo(query2))));
		assertThat(query1.hashCode(), is(not(query2.hashCode())));
	}

	/**
	 * @see DATAMONGO-1093
	 */
	@Test
	public void handlesEqualsAndHashCodeCorrectlyWhenQuerySettingsDiffer() {

		String qry = "{ \"name\" : \"Thomas\"}";
		String fields = "{\"name\":1, \"age\":1}";

		BasicQuery query1 = new BasicQuery(qry, fields);
		query1.getMeta().setComment("foo");

		BasicQuery query2 = new BasicQuery(qry, fields);
		query2.getMeta().setComment("bar");

		assertThat(query1, is(not(equalTo(query2))));
		assertThat(query1.hashCode(), is(not(query2.hashCode())));
	}

	/**
	 * @see DATAMONGO-1387
	 */
	@Test
	public void returnsFieldsCorrectly() {

		String qry = "{ \"name\" : \"Thomas\"}";
		String fields = "{\"name\":1, \"age\":1}";

		BasicQuery query1 = new BasicQuery(qry, fields);

		assertThat(query1.getFieldsObject(), isBsonObject().containing("name").containing("age"));
	}

	/**
	 * @see DATAMONGO-1387
	 */
	@Test
	public void handlesFieldsIncludeCorrectly() {

		String qry = "{ \"name\" : \"Thomas\"}";

		BasicQuery query1 = new BasicQuery(qry);
		query1.fields().include("name");

		assertThat(query1.getFieldsObject(), isBsonObject().containing("name"));
	}

	/**
	 * @see DATAMONGO-1387
	 */
	@Test
	public void combinesFieldsIncludeCorrectly() {

		String qry = "{ \"name\" : \"Thomas\"}";
		String fields = "{\"name\":1, \"age\":1}";

		BasicQuery query1 = new BasicQuery(qry, fields);
		query1.fields().include("gender");

		assertThat(query1.getFieldsObject(), isBsonObject().containing("name").containing("age").containing("gender"));
	}

}
