/*
 * Copyright 2010-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.SpecialDoc;

/**
 * Unit tests for {@link Query}.
 *
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class QueryTests {

	@Test
	public void testSimpleQuery() {

		Query q = new Query(where("name").is("Thomas").and("age").lt(80));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse("{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}"));
	}

	@Test
	public void testQueryWithNot() {

		Query q = new Query(where("name").is("Thomas").and("age").not().mod(10, 0));
		assertThat(q.getQueryObject())
				.isEqualTo(Document.parse("{ \"name\" : \"Thomas\" , \"age\" : { \"$not\" : { \"$mod\" : [ 10 , 0]}}}"));
	}

	@Test
	public void testInvalidQueryWithNotIs() {

		assertThatExceptionOfType(InvalidMongoDbApiUsageException.class)
				.isThrownBy(() -> new Query(where("name").not().is("Thomas")));
	}

	@Test
	public void testOrQuery() {

		Query q = new Query(new Criteria().orOperator(where("name").is("Sven").and("age").lt(50), where("age").lt(50),
				where("name").is("Thomas")));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse(
				"{ \"$or\" : [ { \"name\" : \"Sven\" , \"age\" : { \"$lt\" : 50}} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}"));
	}

	@Test
	public void testAndQuery() {

		Query q = new Query(new Criteria().andOperator(where("name").is("Sven"), where("age").lt(50)));
		Document expected = Document.parse("{ \"$and\" : [ { \"name\" : \"Sven\"} , { \"age\" : { \"$lt\" : 50}}]}");
		Assert.assertEquals(expected, q.getQueryObject());
	}

	@Test
	public void testNorQuery() {

		Query q = new Query(
				new Criteria().norOperator(where("name").is("Sven"), where("age").lt(50), where("name").is("Thomas")));
		assertThat(q.getQueryObject()).isEqualTo(Document
				.parse("{ \"$nor\" : [ { \"name\" : \"Sven\"} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}"));
	}

	@Test
	public void testQueryWithLimit() {

		Query q = new Query(where("name").gte("M").lte("T").and("age").not().gt(22));
		q.limit(50);

		assertThat(q.getQueryObject()).isEqualTo(Document
				.parse("{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}"));
		Assert.assertEquals(50, q.getLimit());
	}

	@Test
	public void testQueryWithFieldsAndSlice() {

		Query q = new Query(where("name").gte("M").lte("T").and("age").not().gt(22));
		q.fields().exclude("address").include("name").slice("orders", 10);

		assertThat(q.getQueryObject()).isEqualTo(Document
				.parse("{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}"));

		assertThat(q.getFieldsObject())
				.isEqualTo(Document.parse("{ \"address\" : 0 , \"name\" : 1 , \"orders\" : { \"$slice\" : 10}}"));
	}

	@Test // DATAMONGO-652
	public void testQueryWithFieldsElemMatchAndPositionalOperator() {

		Query query = query(where("name").gte("M").lte("T").and("age").not().gt(22));
		query.fields().elemMatch("products", where("name").is("milk")).position("comments", 2);

		assertThat(query.getQueryObject()).isEqualTo(Document
				.parse("{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}"));
		assertThat(query.getFieldsObject())
				.isEqualTo(Document.parse("{ \"products\" : { \"$elemMatch\" : { \"name\" : \"milk\"}} , \"comments.$\" : 2}"));
	}

	@Test
	public void testSimpleQueryWithChainedCriteria() {

		Query q = new Query(where("name").is("Thomas").and("age").lt(80));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse("{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}"));
	}

	@Test
	public void testComplexQueryWithMultipleChainedCriteria() {

		Query q = new Query(
				where("name").regex("^T.*").and("age").gt(20).lt(80).and("city").in("Stockholm", "London", "New York"));
		assertThat(q.getQueryObject().toJson()).isEqualTo(Document.parse(
				"{ \"name\" : { \"$regex\" : \"^T.*\", \"$options\" : \"\" } , \"age\" : { \"$gt\" : 20 , \"$lt\" : 80} , "
						+ "\"city\" : { \"$in\" : [ \"Stockholm\" , \"London\" , \"New York\"]}}")
				.toJson());
	}

	@Test
	public void testAddCriteriaWithComplexQueryWithMultipleChainedCriteria() {

		Query q1 = new Query(
				where("name").regex("^T.*").and("age").gt(20).lt(80).and("city").in("Stockholm", "London", "New York"));
		Query q2 = new Query(where("name").regex("^T.*").and("age").gt(20).lt(80))
				.addCriteria(where("city").in("Stockholm", "London", "New York"));

		assertThat(q1.getQueryObject().toString()).isEqualTo(q2.getQueryObject().toString());

		Query q3 = new Query(where("name").regex("^T.*")).addCriteria(where("age").gt(20).lt(80))
				.addCriteria(where("city").in("Stockholm", "London", "New York"));
		assertThat(q1.getQueryObject().toString()).isEqualTo(q3.getQueryObject().toString());
	}

	@Test
	public void testQueryWithElemMatch() {

		Query q = new Query(where("openingHours").elemMatch(where("dayOfWeek").is("Monday").and("open").lte("1800")));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse(
				"{ \"openingHours\" : { \"$elemMatch\" : { \"dayOfWeek\" : \"Monday\" , \"open\" : { \"$lte\" : \"1800\"}}}}"));
	}

	@Test
	public void testQueryWithIn() {

		Query q = new Query(where("state").in("NY", "NJ", "PA"));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse("{ \"state\" : { \"$in\" : [ \"NY\" , \"NJ\" , \"PA\"]}}"));
	}

	@Test
	public void testQueryWithRegex() {

		Query q = new Query(where("name").regex("b.*"));
		assertThat(q.getQueryObject().toJson())
				.isEqualTo(Document.parse("{ \"name\" : { \"$regex\" : \"b.*\", \"$options\" : \"\" }}").toJson());
	}

	@Test
	public void testQueryWithRegexAndOption() {
		Query q = new Query(where("name").regex("b.*", "i"));
		assertThat(q.getQueryObject().toJson())
				.isEqualTo(Document.parse("{ \"name\" : { \"$regex\" : \"b.*\" , \"$options\" : \"i\"}}").toJson());
	}

	@Test // DATAMONGO-538
	public void addsSortCorrectly() {

		Query query = new Query().with(Sort.by(Direction.DESC, "foo"));
		assertThat(query.getSortObject()).isEqualTo(Document.parse("{ \"foo\" : -1}"));
	}

	@Test
	public void rejectsOrderWithIgnoreCase() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> new Query().with(Sort.by(Order.asc("foo").ignoreCase())));
	}

	@Test // DATAMONGO-709, DATAMONGO-1735, // DATAMONGO-2198
	public void shouldReturnClassHierarchyOfRestrictedTypes() {

		Query query = new Query(where("name").is("foo")).restrict(SpecialDoc.class);

		assertThat(query.getRestrictedTypes()).containsExactly(SpecialDoc.class);
	}

	@Test // DATAMONGO-1421
	public void addCriteriaForSamePropertyMultipleTimesShouldThrowAndSafelySerializeErrorMessage() {

		assertThatExceptionOfType(InvalidMongoDbApiUsageException.class).isThrownBy(() -> {

			Query query = new Query();
			query.addCriteria(where("value").is(EnumType.VAL_1));
			query.addCriteria(where("value").is(EnumType.VAL_2));
		}).withMessageContaining("second 'value' criteria")
				.withMessageContaining("already contains '{ \"value\" : { \"$java\" : VAL_1 } }'");

	}

	enum EnumType {
		VAL_1, VAL_2
	}
}
