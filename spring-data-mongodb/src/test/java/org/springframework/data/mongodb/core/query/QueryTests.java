/*
 * Copyright 2010-present the original author or authors.
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
import static org.junit.jupiter.api.Named.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.List;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.geo.Point;
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
class QueryTests {

	@Test
	void testSimpleQuery() {

		Query q = new Query(where("name").is("Thomas").and("age").lt(80));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse("{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}"));
	}

	@Test
	void testQueryWithNot() {

		Query q = new Query(where("name").is("Thomas").and("age").not().mod(10, 0));
		assertThat(q.getQueryObject())
				.isEqualTo(Document.parse("{ \"name\" : \"Thomas\" , \"age\" : { \"$not\" : { \"$mod\" : [ 10 , 0]}}}"));
	}

	@Test
	void testOrQuery() {

		Query q = new Query(new Criteria().orOperator(where("name").is("Sven").and("age").lt(50), where("age").lt(50),
				where("name").is("Thomas")));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse(
				"{ \"$or\" : [ { \"name\" : \"Sven\" , \"age\" : { \"$lt\" : 50}} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}"));
	}

	@Test
	void testAndQuery() {

		Query q = new Query(new Criteria().andOperator(where("name").is("Sven"), where("age").lt(50)));
		Document expected = Document.parse("{ \"$and\" : [ { \"name\" : \"Sven\"} , { \"age\" : { \"$lt\" : 50}}]}");
		assertThat(q.getQueryObject()).isEqualTo(expected);
	}

	@Test
	void testNorQuery() {

		Query q = new Query(
				new Criteria().norOperator(where("name").is("Sven"), where("age").lt(50), where("name").is("Thomas")));
		assertThat(q.getQueryObject()).isEqualTo(Document
				.parse("{ \"$nor\" : [ { \"name\" : \"Sven\"} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}"));
	}

	@Test // GH-4584
	void testQueryWithLimit() {

		Query q = new Query(where("name").gte("M").lte("T").and("age").not().gt(22));
		q.limit(50);

		assertThat(q.getQueryObject()).isEqualTo(Document
				.parse("{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}"));
		assertThat(q.getLimit()).isEqualTo(50);

		q.limit(Limit.unlimited());
		assertThat(q.getLimit()).isZero();
		assertThat(q.isLimited()).isFalse();

		q.limit(Limit.of(10));
		assertThat(q.getLimit()).isEqualTo(10);
		assertThat(q.isLimited()).isTrue();

		q.limit(Limit.of(-1));
		assertThat(q.getLimit()).isZero();
		assertThat(q.isLimited()).isFalse();

		Query other = new Query(where("name").gte("M")).limit(Limit.of(10));
		assertThat(new Query(where("name").gte("M")).limit(10)).isEqualTo(other).hasSameHashCodeAs(other);
	}

	@Test
	void testQueryWithFieldsAndSlice() {

		Query q = new Query(where("name").gte("M").lte("T").and("age").not().gt(22));
		q.fields().exclude("address").include("name").slice("orders", 10);

		assertThat(q.getQueryObject()).isEqualTo(Document
				.parse("{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}"));

		assertThat(q.getFieldsObject())
				.isEqualTo(Document.parse("{ \"address\" : 0 , \"name\" : 1 , \"orders\" : { \"$slice\" : 10}}"));
	}

	@Test // DATAMONGO-652
	void testQueryWithFieldsElemMatchAndPositionalOperator() {

		Query query = query(where("name").gte("M").lte("T").and("age").not().gt(22));
		query.fields().elemMatch("products", where("name").is("milk")).position("comments", 2);

		assertThat(query.getQueryObject()).isEqualTo(Document
				.parse("{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}"));
		assertThat(query.getFieldsObject())
				.isEqualTo(Document.parse("{ \"products\" : { \"$elemMatch\" : { \"name\" : \"milk\"}} , \"comments.$\" : 2}"));
	}

	@Test
	void testSimpleQueryWithChainedCriteria() {

		Query q = new Query(where("name").is("Thomas").and("age").lt(80));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse("{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}"));
	}

	@Test
	void testComplexQueryWithMultipleChainedCriteria() {

		Query q = new Query(
				where("name").regex("^T.*").and("age").gt(20).lt(80).and("city").in("Stockholm", "London", "New York"));
		assertThat(q.getQueryObject().toJson()).isEqualTo(Document.parse(
				"{ \"name\" : { \"$regex\" : \"^T.*\", \"$options\" : \"\" } , \"age\" : { \"$gt\" : 20 , \"$lt\" : 80} , "
						+ "\"city\" : { \"$in\" : [ \"Stockholm\" , \"London\" , \"New York\"]}}")
				.toJson());
	}

	@Test
	void testAddCriteriaWithComplexQueryWithMultipleChainedCriteria() {

		Query q1 = new Query(
				where("name").regex("^T.*").and("age").gt(20).lt(80).and("city").in("Stockholm", "London", "New York"));
		Query q2 = new Query(where("name").regex("^T.*").and("age").gt(20).lt(80))
				.addCriteria(where("city").in("Stockholm", "London", "New York"));

		assertThat(q1.getQueryObject()).hasToString(q2.getQueryObject().toString());

		Query q3 = new Query(where("name").regex("^T.*")).addCriteria(where("age").gt(20).lt(80))
				.addCriteria(where("city").in("Stockholm", "London", "New York"));
		assertThat(q1.getQueryObject()).hasToString(q3.getQueryObject().toString());
	}

	@Test
	void testQueryWithElemMatch() {

		Query q = new Query(where("openingHours").elemMatch(where("dayOfWeek").is("Monday").and("open").lte("1800")));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse(
				"{ \"openingHours\" : { \"$elemMatch\" : { \"dayOfWeek\" : \"Monday\" , \"open\" : { \"$lte\" : \"1800\"}}}}"));
	}

	@Test
	void testQueryWithIn() {

		Query q = new Query(where("state").in("NY", "NJ", "PA"));
		assertThat(q.getQueryObject()).isEqualTo(Document.parse("{ \"state\" : { \"$in\" : [ \"NY\" , \"NJ\" , \"PA\"]}}"));
	}

	@Test
	void testQueryWithRegex() {

		Query q = new Query(where("name").regex("b.*"));
		assertThat(q.getQueryObject().toJson())
				.isEqualTo(Document.parse("{ \"name\" : { \"$regex\" : \"b.*\", \"$options\" : \"\" }}").toJson());
	}

	@Test
	void testQueryWithRegexAndOption() {
		Query q = new Query(where("name").regex("b.*", "i"));
		assertThat(q.getQueryObject().toJson())
				.isEqualTo(Document.parse("{ \"name\" : { \"$regex\" : \"b.*\" , \"$options\" : \"i\"}}").toJson());
	}

	@Test // DATAMONGO-538
	void addsSortCorrectly() {

		Query query = new Query().with(Sort.by(Direction.DESC, "foo"));
		assertThat(query.getSortObject()).isEqualTo(Document.parse("{ \"foo\" : -1}"));
	}

	@Test
	void rejectsOrderWithIgnoreCase() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> new Query().with(Sort.by(Order.asc("foo").ignoreCase())));
	}

	@Test // DATAMONGO-709, DATAMONGO-1735, // DATAMONGO-2198
	void shouldReturnClassHierarchyOfRestrictedTypes() {

		Query query = new Query(where("name").is("foo")).restrict(SpecialDoc.class);

		assertThat(query.getRestrictedTypes()).containsExactly(SpecialDoc.class);
	}

	@ParameterizedTest // GH-5209
	@MethodSource
	void addCriteriaForSamePropertyShouldRenderAsAnd(Query query) {

		query.addCriteria(where("amount").lte(500));

		assertThat(query.getQueryObject())
				.isEqualTo(Document.parse("{ '$and' : [ { 'amount' : { '$gte' : 100 } }, { 'amount' : { '$lte' : 500 } } ] }"));
	}

	static Stream<Named<Query>> addCriteriaForSamePropertyShouldRenderAsAnd() {
		return Stream.of(named("Query", new Query(where("amount").gte(100))),
				named("BasicQuery", new BasicQuery("{ 'amount' : { '$gte' : 100 } }")),
				named("Query.of", Query.of(new BasicQuery("{ 'amount' : { '$gte' : 100 } }"))));
	}

	@ParameterizedTest // GH-5209
	@MethodSource
	void addCriteriaWithAndOperatorShouldExtendExistingAnd(Query query) {

		query.addCriteria(new Criteria().andOperator(where("b").is(2)));

		assertThat(query.getQueryObject()).isEqualTo(Document.parse("{ '$and' : [ { 'a' : 1 }, { 'b' : 2 } ] }"));
	}

	static Stream<Named<Query>> addCriteriaWithAndOperatorShouldExtendExistingAnd() {
		return Stream.of(named("Query", new Query(new Criteria().andOperator(where("a").is(1)))),
				named("BasicQuery", new BasicQuery("{ '$and' : [ { 'a' : 1 } ] }")),
				named("Query.of", Query.of(new Query(new Criteria().andOperator(where("a").is(1))))));
	}

	@ParameterizedTest // GH-5209
	@MethodSource
	void addCriteriaForSamePropertyShouldRetainEmptyAndOperator(Query query) {

		query.addCriteria(where("amount").lte(500));

		assertThat(query.getQueryObject()).isEqualTo(Document
				.parse("{ '$and' : [ { '$and' : [] }, { 'amount' : { '$gte' : 100 } }, { 'amount' : { '$lte' : 500 } } ] }"));
	}

	static Stream<Named<Query>> addCriteriaForSamePropertyShouldRetainEmptyAndOperator() {
		return Stream.of(
				named("Query", new Query(new Criteria().andOperator(List.of())).addCriteria(where("amount").gte(100))),
				named("BasicQuery", new BasicQuery("{ '$and' : [], 'amount' : { '$gte' : 100 } }")),
				named("Query.of", Query.of(new BasicQuery("{ '$and' : [], 'amount' : { '$gte' : 100 } }"))));
	}

	@ParameterizedTest // GH-5209
	@MethodSource
	void addCriteriaShouldRejectSecondNearForSameKey(Query query) {

		assertThatExceptionOfType(InvalidMongoDbApiUsageException.class)
				.isThrownBy(() -> query.addCriteria(where("location").near(new Point(2, 2))).getQueryObject())
				.withMessageContaining("$near");
	}

	static Stream<Named<Query>> addCriteriaShouldRejectSecondNearForSameKey() {
		return Stream.of(named("Query", new Query(where("location").near(new Point(1, 1)))),
				named("Query with $and", new Query(where("location").near(new Point(1, 1)).and("location").exists(true))),
				named("BasicQuery", new BasicQuery("{ 'location' : { '$near' : [1, 1] } }")),
				named("BasicQuery with $and",
						new BasicQuery("{ 'location' : { '$near' : [1, 1] } }").addCriteria(where("location").exists(true))),
				named("Query.of", Query.of(new Query(where("location").near(new Point(1, 1))))), named("Query.of with $and",
						Query.of(new Query(where("location").near(new Point(1, 1))).addCriteria(where("location").exists(true)))));
	}

	@ParameterizedTest // GH-5209
	@MethodSource
	void addCriteriaShouldRejectSecondText(Query query) {

		assertThatExceptionOfType(InvalidMongoDbApiUsageException.class)
				.isThrownBy(() -> query.addCriteria(TextCriteria.forDefaultLanguage().matching("data")).getQueryObject())
				.withMessageContaining("$text");
	}

	static Stream<Named<Query>> addCriteriaShouldRejectSecondText() {
		return Stream.of(named("Query", new Query(TextCriteria.forDefaultLanguage().matching("spring"))),
				named("BasicQuery", new BasicQuery("{ '$text' : { '$search' : 'spring' } }")),
				named("BasicQuery with $and", new BasicQuery("{ '$and' : [ { '$text' : { '$search' : 'spring' } } ] }")),
				named("Query.of", Query.of(new Query(TextCriteria.forDefaultLanguage().matching("spring")))));
	}

	@Test // GH-5209
	void addCriteriaWithDuplicateKeyShouldKeepNonConflictingCriteriaTopLevel() {

		Query query = new Query(where("_id").is("id-1"));
		query.addCriteria(where("amount").gte(100));
		query.addCriteria(where("amount").lte(500));

		assertThat(query.getQueryObject()).containsEntry("_id", "id-1");
	}

	@Test // GH-5209
	void addCriteriaShouldPreserveDuplicateKeyInChainedCriteria() {

		Query query = new Query();
		query.addCriteria(where("amount").gte(100).and("status").is("NEW"));
		query.addCriteria(where("amount").lte(500));

		assertThat(query.getQueryObject()).isEqualTo(new Document("status", "NEW").append("$and",
				List.of(new Document("amount", new Document("$gte", 100)), new Document("amount", new Document("$lte", 500)))));
	}

	@Test // GH-5209
	void addCriteriaWithDuplicateKeyShouldRetainExistingAndOperator() {

		Query query = new Query(new Criteria().andOperator(where("a").is(1), where("b").is(2)));
		query.addCriteria(where("amount").gte(100));
		query.addCriteria(where("amount").lte(500));

		assertThat(query.getQueryObject())
				.isEqualTo(new Document("$and", List.of(new Document("a", 1), new Document("b", 2),
						new Document("amount", new Document("$gte", 100)), new Document("amount", new Document("$lte", 500)))));
	}

	@Test // GH-5209
	void basicQueryRepeatedAddCriteriaForSamePropertyShouldRenderFlatAnd() {

		BasicQuery query = new BasicQuery("{ 'amount' : { '$gte' : 100 }}");
		query.addCriteria(where("amount").lte(500));
		query.addCriteria(where("amount").ne(200));
		query.addCriteria(where("amount").ne(300));

		assertThat(query.getQueryObject()).isEqualTo(new Document("$and",
				List.of(new Document("amount", new Document("$gte", 100)), new Document("amount", new Document("$lte", 500)),
						new Document("amount", new Document("$ne", 200)), new Document("amount", new Document("$ne", 300)))));
	}

	@Test // GH-5209
	void addCriteriaWithDuplicateKeyShouldKeepTextCriteriaTopLevel() {

		Query query = new Query(TextCriteria.forDefaultLanguage().matching("spring"));
		query.addCriteria(where("amount").gte(100));
		query.addCriteria(where("amount").lte(500));

		assertThat(query.getQueryObject()).containsKey("$text").containsKey("$and");
	}

	@Test // GH-5209
	void equalsShouldIgnoreOrderOfCriteriaForDistinctKeys() {

		Query ab = new Query(where("a").is(1)).addCriteria(where("b").is(2));
		Query ba = new Query(where("b").is(2)).addCriteria(where("a").is(1));

		assertThat(ab).isEqualTo(ba).hasSameHashCodeAs(ba);
	}

	@Test // GH-5209
	void equalsShouldConsiderOrderOfCriteriaForSameKey() {

		Query gteLte = new Query(where("a").gte(1)).addCriteria(where("a").lte(5));
		Query lteGte = new Query(where("a").lte(5)).addCriteria(where("a").gte(1));

		assertThat(gteLte).isNotEqualTo(lteGte);
	}

	@Test // GH-5209
	void equalsShouldIgnoreInterleavingOfCriteriaForDistinctKeys() {

		Query interleaved = new Query(where("a").gt(1)).addCriteria(where("b").gt(2)).addCriteria(where("a").lt(3));
		Query grouped = new Query(where("a").gt(1)).addCriteria(where("a").lt(3)).addCriteria(where("b").gt(2));
		Query reordered = new Query(where("a").lt(3)).addCriteria(where("b").gt(2)).addCriteria(where("a").gt(1));
		Query sameSize = new Query(where("a").gt(1)).addCriteria(where("b").gt(2)).addCriteria(where("b").gt(2));

		assertThat(interleaved).isEqualTo(grouped).hasSameHashCodeAs(grouped).isNotEqualTo(reordered)
				.isNotEqualTo(sameSize);
		assertThat(sameSize).isNotEqualTo(interleaved);
	}

	@Test // GH-5209
	void addCriteriaShouldRenderAndInInsertionOrder() {

		Query query = new Query(where("a").gt(1)).addCriteria(where("b").gt(2)).addCriteria(where("a").lt(3))
				.addCriteria(where("b").lt(4));

		assertThat(query.getQueryObject())
				.isEqualTo(where("a").gt(1).and("b").gt(2).and("a").lt(3).and("b").lt(4).getCriteriaObject());
		assertThat(query.getQueryObject().getList("$and", Document.class)).containsExactly(
				new Document("a", new Document("$gt", 1)), new Document("b", new Document("$gt", 2)),
				new Document("a", new Document("$lt", 3)), new Document("b", new Document("$lt", 4)));
	}

	@Test // GH-5209
	void clonedQueryShouldKeepRestrictedTypesOutsideAndWhenMergingDuplicateCriteria() {

		Query source = new Query(where("amount").gte(100)).restrict(SpecialDoc.class);
		Query target = Query.of(source);
		target.addCriteria(where("amount").lte(500));

		assertThat(target.getQueryObject()).isEqualTo(new Document("$and",
				List.of(new Document("amount", new Document("$gte", 100)), new Document("amount", new Document("$lte", 500))))
				.append("_$RESTRICTED_TYPES", source.getRestrictedTypes()));
	}

	@Test // DATAMONGO-1783
	void queryOfShouldCreateNewQueryWithEqualBehaviour() {

		Query source = new Query();
		source.addCriteria(where("This you must ken").is(EnumType.VAL_1));

		compareQueries(Query.of(source), source);
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldNotDependOnCriteriaFromSource() {

		Query source = new Query();
		source.addCriteria(where("From one make ten").is("and two let be."));
		Query target = Query.of(source);

		assertThat(target.getQueryObject()).containsAllEntriesOf(new Document("From one make ten", "and two let be."))
				.isNotSameAs(source.getQueryObject());
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldAppendCriteria() {

		Query source = new Query();
		source.addCriteria(where("Skip o'er the four").is("From five and six"));
		Query target = Query.of(source);

		compareQueries(target, source);
		target.addCriteria(where("the Witch's tricks").is("make seven and eight"));

		assertThat(target.getQueryObject()).isEqualTo(
				new Document("Skip o'er the four", "From five and six").append("the Witch's tricks", "make seven and eight"));
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldNotDependOnCollationFromSource() {

		Query source = new Query().collation(Collation.simple());
		Query target = Query.of(source);

		compareQueries(target, source);
		source.collation(Collation.of("Tis finished straight"));

		assertThat(target.getCollation()).contains(Collation.simple()).isNotEqualTo(source.getCollation());
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldNotDependOnSortFromSource() {

		Query source = new Query().with(Sort.by("And nine is one"));
		Query target = Query.of(source);

		compareQueries(target, source);
		source.with(Sort.by("And ten is none"));

		assertThat(target.getSortObject()).isEqualTo(new Document("And nine is one", 1))
				.isNotEqualTo(source.getSortObject());
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldNotDependOnFieldsFromSource() {

		Query source = new Query();
		source.fields().include("That is the witch's one-time-one");
		Query target = Query.of(source);

		compareQueries(target, source);
		source.fields().exclude("Goethe");

		assertThat(target.getFieldsObject()).isEqualTo(new Document("That is the witch's one-time-one", 1))
				.isNotEqualTo(source.getFieldsObject());
	}

	@Test // DATAMONGO-1783, DATAMONGO-2572
	void clonedQueryShouldNotDependOnMetaFromSource() {

		Query source = new Query().maxTimeMsec(100);
		Query target = Query.of(source);

		compareQueries(target, source);
		source.allowSecondaryReads();

		Meta meta = new Meta();
		meta.setMaxTimeMsec(100);
		assertThat(target.getMeta()).isEqualTo(meta).isNotEqualTo(source.getMeta());
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldNotDependOnRestrictedTypesFromSource() {

		Query source = new Query();
		source.restrict(EnumType.class);
		Query target = Query.of(source);

		compareQueries(target, source);
		source.restrict(Query.class);

		assertThat(target.getRestrictedTypes()).containsExactly(EnumType.class).isNotEqualTo(source.getRestrictedTypes());
	}

	@Test // DATAMONGO-1783
	void clonedQueryShouldApplyRestrictionsFromBasicQuery() {

		BasicQuery source = new BasicQuery("{ 'foo' : 'bar'}");
		Query target = Query.of(source);

		compareQueries(target, source);

		target.addCriteria(where("one").is("10"));
		assertThat(target.getQueryObject()).isEqualTo(new Document("foo", "bar").append("one", "10"))
				.isNotEqualTo(source.getQueryObject());
	}

	@Test // DATAMONGO-2478
	void queryOfShouldWorkOnProxiedObjects() {

		BasicQuery source = new BasicQuery("{ 'foo' : 'bar'}", "{ '_id' : -1, 'foo' : 1 }");
		source.withHint("the hint");
		source.limit(10);
		source.setSortObject(new Document("_id", 1));

		ProxyFactory proxyFactory = new ProxyFactory(source);
		proxyFactory.setInterfaces(new Class[0]);

		Query target = Query.of((Query) proxyFactory.getProxy());

		compareQueries(target, source);
	}

	@Test // GH-4771
	void appliesSortOfUnpagedPageable() {

		Query query = new Query();
		query.with(Pageable.unpaged(Sort.by("sortMe")));

		assertThat(query.isSorted()).isTrue();
	}

	@Test // GH-5135
	void shouldIncludeFields() {

		Query query = new Query();
		query.fields().include(Person::firstName, Person::lastName);

		assertThat(query.getFieldsObject()).containsEntry("firstName", 1).containsEntry("lastName", 1);
	}

	@Test // GH-5135
	void shouldExcludeFields() {

		Query query = new Query();
		query.fields().exclude(Person::firstName, Person::lastName);

		assertThat(query.getFieldsObject()).containsEntry("firstName", 0).containsEntry("lastName", 0);
	}

	private void compareQueries(Query actual, Query expected) {

		assertThat(actual.getCollation()).isEqualTo(expected.getCollation());
		assertThat(actual.getSortObject()).hasSameSizeAs(expected.getSortObject())
				.containsAllEntriesOf(expected.getSortObject());
		assertThat(actual.getFieldsObject()).hasSameSizeAs(expected.getFieldsObject())
				.containsAllEntriesOf(expected.getFieldsObject());
		assertThat(actual.getQueryObject()).hasSameSizeAs(expected.getQueryObject())
				.containsAllEntriesOf(expected.getQueryObject());
		assertThat(actual.getHint()).isEqualTo(expected.getHint());
		assertThat(actual.getLimit()).isEqualTo(expected.getLimit());
		assertThat(actual.getSkip()).isEqualTo(expected.getSkip());
		assertThat(actual.getMeta()).isEqualTo(expected.getMeta());
		assertThat(actual.getRestrictedTypes()).isEqualTo(expected.getRestrictedTypes());
	}

	enum EnumType {
		VAL_1, VAL_2
	}

	record Person(String firstName, String lastName) {

	}
}
