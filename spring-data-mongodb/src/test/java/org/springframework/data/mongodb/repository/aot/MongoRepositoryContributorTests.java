/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import static org.assertj.core.api.Assertions.*;

import example.aot.User;
import example.aot.UserProjection;
import example.aot.UserRepository;
import example.aot.UserRepository.UserAggregate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.StringUtils;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for the {@link UserRepository} AOT fragment.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
@SpringJUnitConfig(classes = MongoRepositoryContributorTests.MongoRepositoryContributorConfiguration.class)
class MongoRepositoryContributorTests {

	private static final String DB_NAME = "aot-repo-tests";

	@Client static MongoClient client;
	@Autowired UserRepository fragment;

	@Configuration
	static class MongoRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public MongoRepositoryContributorConfiguration() {
			super(UserRepository.class);
		}

		@Bean
		MongoOperations mongoOperations() {
			return new MongoTemplate(client, DB_NAME);
		}
	}

	@BeforeEach
	void beforeEach() {

		MongoTestUtils.flushCollection(DB_NAME, "user", client);
		initUsers();
	}

	@Test
	void testFindDerivedFinderSingleEntity() {

		User user = fragment.findOneByUsername("yoda");
		assertThat(user).isNotNull().extracting(User::getUsername).isEqualTo("yoda");
	}

	@Test
	void testFindDerivedFinderOptionalEntity() {

		Optional<User> user = fragment.findOptionalOneByUsername("yoda");
		assertThat(user).isNotNull().containsInstanceOf(User.class)
				.hasValueSatisfying(it -> assertThat(it).extracting(User::getUsername).isEqualTo("yoda"));
	}

	@Test
	void testDerivedCount() {

		assertThat(fragment.countUsersByLastname("Skywalker")).isEqualTo(2L);
		assertThat(fragment.countUsersAsIntByLastname("Skywalker")).isEqualTo(2);
	}

	@Test
	void testDerivedExists() {

		assertThat(fragment.existsUserByLastname("Skywalker")).isTrue();
	}

	@Test
	void testDerivedFinderWithoutArguments() {

		List<User> users = fragment.findUserNoArgumentsBy();
		assertThat(users).hasSize(7).hasOnlyElementsOfType(User.class);
	}

	@Test
	void testCountWorksAsExpected() {

		Long value = fragment.countUsersByLastname("Skywalker");
		assertThat(value).isEqualTo(2L);
	}

	@Test
	void testDerivedFinderReturningList() {

		List<User> users = fragment.findByLastnameStartingWith("S");
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("luke", "vader", "kylo", "han");
	}

	@Test
	void testEndingWith() {

		List<User> users = fragment.findByLastnameEndsWith("er");
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("luke", "vader");
	}

	@Test
	void testLike() {

		List<User> users = fragment.findByFirstnameLike("ei");
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("leia");
	}

	@Test
	void testNotLike() {

		List<User> users = fragment.findByFirstnameNotLike("ei");
		assertThat(users).extracting(User::getUsername).isNotEmpty().doesNotContain("leia");
	}

	@Test
	void testIn() {

		List<User> users = fragment.findByUsernameIn(List.of("chewbacca", "kylo"));
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("chewbacca", "kylo");
	}

	@Test
	void testNotIn() {

		List<User> users = fragment.findByUsernameNotIn(List.of("chewbacca", "kylo"));
		assertThat(users).extracting(User::getUsername).isNotEmpty().doesNotContain("chewbacca", "kylo");
	}

	@Test
	void testAnd() {

		List<User> users = fragment.findByFirstnameAndLastname("Han", "Solo");
		assertThat(users).extracting(User::getUsername).containsExactly("han");
	}

	@Test
	void testOr() {

		List<User> users = fragment.findByFirstnameOrLastname("Han", "Skywalker");
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("han", "vader", "luke");
	}

	@Test
	void testBetween() {

		List<User> users = fragment.findByVisitsBetween(10, 100);
		assertThat(users).extracting(User::getUsername).containsExactly("vader");
	}

	@Test
	void testTimeValue() {

		List<User> users = fragment.findByLastSeenGreaterThan(Instant.parse("2025-01-01T00:00:00.000Z"));
		assertThat(users).extracting(User::getUsername).containsExactly("luke");
	}

	@Test
	void testNot() {

		List<User> users = fragment.findByLastnameNot("Skywalker");
		assertThat(users).extracting(User::getUsername).isNotEmpty().doesNotContain("luke", "vader");
	}

	@Test
	void testExistsCriteria() {

		List<User> users = fragment.findByVisitsExists(false);
		assertThat(users).extracting(User::getUsername).contains("kylo");
	}

	@Test
	void testLimitedDerivedFinder() {

		List<User> users = fragment.findTop2ByLastnameStartingWith("S");
		assertThat(users).hasSize(2);
	}

	@Test
	void testSortedDerivedFinder() {

		List<User> users = fragment.findByLastnameStartingWithOrderByUsername("S");
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo", "luke", "vader");
	}

	@Test
	void testDerivedFinderWithLimitArgument() {

		List<User> users = fragment.findByLastnameStartingWith("S", Limit.of(2));
		assertThat(users).hasSize(2);
	}

	@Test
	void testDerivedFinderWithSort() {

		List<User> users = fragment.findByLastnameStartingWith("S", Sort.by("username"));
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo", "luke", "vader");
	}

	@Test
	void testDerivedFinderWithSortAndLimit() {

		List<User> users = fragment.findByLastnameStartingWith("S", Sort.by("username"), Limit.of(2));
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testDerivedFinderReturningListWithPageable() {

		List<User> users = fragment.findByLastnameStartingWith("S", PageRequest.of(0, 2, Sort.by("username")));
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testDerivedFinderReturningPage() {

		Page<User> page = fragment.findPageOfUsersByLastnameStartingWith("S", PageRequest.of(0, 2, Sort.by("username")));
		assertThat(page.getTotalElements()).isEqualTo(4);
		assertThat(page.getSize()).isEqualTo(2);
		assertThat(page.getContent()).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testDerivedFinderReturningSlice() {

		Slice<User> slice = fragment.findSliceOfUserByLastnameStartingWith("S", PageRequest.of(0, 2, Sort.by("username")));
		assertThat(slice.hasNext()).isTrue();
		assertThat(slice.getSize()).isEqualTo(2);
		assertThat(slice.getContent()).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test // GH-4970
	void testDerivedQueryReturningStream() {

		List<User> results = fragment.streamByLastnameStartingWith("S", Sort.by("username"), Limit.of(2)).toList();

		assertThat(results).hasSize(2);
		assertThat(results).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test // GH-4970
	void testDerivedQueryReturningWindowByOffset() {

		Window<User> window1 = fragment.findTop2WindowByLastnameStartingWithOrderByUsername("S", ScrollPosition.offset());
		assertThat(window1).extracting(User::getUsername).containsExactly("han", "kylo");
		assertThat(window1.positionAt(1)).isInstanceOf(OffsetScrollPosition.class);

		Window<User> window2 = fragment.findTop2WindowByLastnameStartingWithOrderByUsername("S", window1.positionAt(1));
		assertThat(window2).extracting(User::getUsername).containsExactly("luke", "vader");
	}

	@Test // GH-4970
	void testDerivedQueryReturningWindowByKeyset() {

		Window<User> window1 = fragment.findTop2WindowByLastnameStartingWithOrderByUsername("S", ScrollPosition.keyset());
		assertThat(window1).extracting(User::getUsername).containsExactly("han", "kylo");
		assertThat(window1.positionAt(1)).isInstanceOf(KeysetScrollPosition.class);

		Window<User> window2 = fragment.findTop2WindowByLastnameStartingWithOrderByUsername("S", window1.positionAt(1));
		assertThat(window2).extracting(User::getUsername).containsExactly("luke", "vader");
	}

	@Test
	void testAnnotatedFinderReturningSingleValueWithQuery() {

		User user = fragment.findAnnotatedQueryByUsername("yoda");
		assertThat(user).isNotNull().extracting(User::getUsername).isEqualTo("yoda");
	}

	@Test
	void testAnnotatedCount() {

		Long value = fragment.countAnnotatedQueryByLastname("Skywalker");
		assertThat(value).isEqualTo(2L);
	}

	@Test
	void testAnnotatedFinderReturningListWithQuery() {

		List<User> users = fragment.findAnnotatedQueryByLastname("S");
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("han", "kylo", "luke", "vader");
	}

	@Test
	void testAnnotatedMultilineFinderWithQuery() {

		List<User> users = fragment.findAnnotatedMultilineQueryByLastname("S");
		assertThat(users).extracting(User::getUsername).containsExactlyInAnyOrder("han", "kylo", "luke", "vader");
	}

	@Test
	void testAnnotatedFinderWithQueryAndLimit() {

		List<User> users = fragment.findAnnotatedQueryByLastname("S", Limit.of(2));
		assertThat(users).hasSize(2);
	}

	@Test
	void testAnnotatedFinderWithQueryAndSort() {

		List<User> users = fragment.findAnnotatedQueryByLastname("S", Sort.by("username"));
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo", "luke", "vader");
	}

	@Test
	void testAnnotatedFinderWithQueryLimitAndSort() {

		List<User> users = fragment.findAnnotatedQueryByLastname("S", Limit.of(2), Sort.by("username"));
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testAnnotatedFinderReturningListWithPageable() {

		List<User> users = fragment.findAnnotatedQueryByLastname("S", PageRequest.of(0, 2, Sort.by("username")));
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testAnnotatedFinderReturningPage() {

		Page<User> page = fragment.findAnnotatedQueryPageOfUsersByLastname("S", PageRequest.of(0, 2, Sort.by("username")));
		assertThat(page.getTotalElements()).isEqualTo(4);
		assertThat(page.getSize()).isEqualTo(2);
		assertThat(page.getContent()).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testAnnotatedFinderReturningSlice() {

		Slice<User> slice = fragment.findAnnotatedQuerySliceOfUsersByLastname("S",
				PageRequest.of(0, 2, Sort.by("username")));
		assertThat(slice.hasNext()).isTrue();
		assertThat(slice.getSize()).isEqualTo(2);
		assertThat(slice.getContent()).extracting(User::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testDeleteSingle() {

		User result = fragment.deleteByUsername("yoda");

		assertThat(result).isNotNull().extracting(User::getUsername).isEqualTo("yoda");
		assertThat(client.getDatabase(DB_NAME).getCollection("user").countDocuments()).isEqualTo(6L);
	}

	@Test
	void testDeleteSingleAnnotatedQuery() {

		User result = fragment.deleteAnnotatedQueryByUsername("yoda");

		assertThat(result).isNotNull().extracting(User::getUsername).isEqualTo("yoda");
		assertThat(client.getDatabase(DB_NAME).getCollection("user").countDocuments()).isEqualTo(6L);
	}

	@Test
	void testDerivedDeleteMultipleReturningDeleteCount() {

		Long result = fragment.deleteByLastnameStartingWith("S");

		assertThat(result).isEqualTo(4L);
		assertThat(client.getDatabase(DB_NAME).getCollection("user").countDocuments()).isEqualTo(3L);
	}

	@Test
	void testDerivedDeleteMultipleReturningDeleteCountAnnotatedQuery() {

		Long result = fragment.deleteAnnotatedQueryByLastnameStartingWith("S");

		assertThat(result).isEqualTo(4L);
		assertThat(client.getDatabase(DB_NAME).getCollection("user").countDocuments()).isEqualTo(3L);
	}

	@Test
	void testDerivedDeleteMultipleReturningDeleted() {

		List<User> result = fragment.deleteUsersByLastnameStartingWith("S");

		assertThat(result).extracting(User::getUsername).containsExactlyInAnyOrder("han", "kylo", "luke", "vader");
		assertThat(client.getDatabase(DB_NAME).getCollection("user").countDocuments()).isEqualTo(3L);
	}

	@Test
	void testDerivedDeleteMultipleReturningDeletedAnnotatedQuery() {

		List<User> result = fragment.deleteUsersAnnotatedQueryByLastnameStartingWith("S");

		assertThat(result).extracting(User::getUsername).containsExactlyInAnyOrder("han", "kylo", "luke", "vader");
		assertThat(client.getDatabase(DB_NAME).getCollection("user").countDocuments()).isEqualTo(3L);
	}

	@Test
	void testDerivedFinderWithAnnotatedSort() {

		List<User> users = fragment.findWithAnnotatedSortByLastnameStartingWith("S");
		assertThat(users).extracting(User::getUsername).containsExactly("han", "kylo", "luke", "vader");
	}

	@Test
	void testDerivedFinderWithAnnotatedFieldsProjection() {

		List<User> users = fragment.findWithAnnotatedFieldsProjectionByLastnameStartingWith("S");
		assertThat(users).allMatch(
				user -> StringUtils.hasText(user.getUsername()) && user.getLastname() == null && user.getFirstname() == null);
	}

	@Test
	void testReadPreferenceAppliedToQuery() {

		// check if it fails when trying to parse the read preference to indicate it would get applied
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> fragment.findWithReadPreferenceByUsername("S"))
				.withMessageContaining("No match for read preference");
	}

	@Test
	void testDerivedFinderReturningListOfProjections() {

		List<UserProjection> users = fragment.findUserProjectionByLastnameStartingWith("S");
		assertThat(users).extracting(UserProjection::getUsername).containsExactlyInAnyOrder("han", "kylo", "luke", "vader");
	}

	@Test
	void testDerivedFinderReturningPageOfProjections() {

		Page<UserProjection> users = fragment.findUserProjectionByLastnameStartingWith("S",
				PageRequest.of(0, 2, Sort.by("username")));
		assertThat(users).extracting(UserProjection::getUsername).containsExactly("han", "kylo");
	}

	@Test // GH-4970
	void testDerivedFinderReturningPageOfDynamicProjections() {

		Page<UserProjection> users = fragment.findUserProjectionByLastnameStartingWith("S",
				PageRequest.of(0, 2, Sort.by("username")), UserProjection.class);
		assertThat(users).extracting(UserProjection::getUsername).containsExactly("han", "kylo");
	}

	@Test
	void testUpdateWithDerivedQuery() {

		int modifiedCount = fragment.findUserAndIncrementVisitsByLastname("Organa", 42);

		assertThat(modifiedCount).isOne();
		assertThat(client.getDatabase(DB_NAME).getCollection("user").find(new Document("_id", "id-2")).first().get("visits",
				Integer.class)).isEqualTo(42);
	}

	@Test
	void testUpdateWithAnnotatedQuery() {

		int modifiedCount = fragment.updateAllByLastname("Organa", 42);

		assertThat(modifiedCount).isOne();
		assertThat(client.getDatabase(DB_NAME).getCollection("user").find(new Document("_id", "id-2")).first().get("visits",
				Integer.class)).isEqualTo(42);
	}

	@Test
	void testAggregationPipelineUpdate() {

		fragment.findAndIncrementVisitsViaPipelineByLastname("Organa", 42);

		assertThat(client.getDatabase(DB_NAME).getCollection("user").find(new Document("_id", "id-2")).first().get("visits",
				Integer.class)).isEqualTo(42);
	}

	@Test
	void testAggregationWithExtractedSimpleResults() {

		List<String> allLastnames = fragment.findAllLastnames();
		assertThat(allLastnames).containsExactlyInAnyOrder("Skywalker", "Solo", "Organa", "Solo", "Skywalker");
	}

	@Test
	void testAggregationWithProjectedResults() {

		List<UserAggregate> allLastnames = fragment.groupByLastnameAnd("first_name");
		assertThat(allLastnames).containsExactlyInAnyOrder(//
				new UserAggregate("Skywalker", List.of("Anakin", "Luke")), //
				new UserAggregate("Organa", List.of("Leia")), //
				new UserAggregate("Solo", List.of("Han", "Ben")));
	}

	@Test
	void testAggregationWithProjectedResultsLimitedByPageable() {

		List<UserAggregate> allLastnames = fragment.groupByLastnameAnd("first_name", PageRequest.of(1, 1, Sort.by("_id")));
		assertThat(allLastnames).containsExactly(//
				new UserAggregate("Skywalker", List.of("Anakin", "Luke")) //
		);
	}

	@Test
	void testAggregationWithProjectedResultsAsPage() {

		Slice<UserAggregate> allLastnames = fragment.groupByLastnameAndReturnPage("first_name",
				PageRequest.of(1, 1, Sort.by("_id")));
		assertThat(allLastnames.hasPrevious()).isTrue();
		assertThat(allLastnames.hasNext()).isTrue();
		assertThat(allLastnames.getContent()).containsExactly(//
				new UserAggregate("Skywalker", List.of("Anakin", "Luke")) //
		);
	}

	@Test
	void testAggregationWithProjectedResultsWrappedInAggregationResults() {

		AggregationResults<UserAggregate> allLastnames = fragment.groupByLastnameAndAsAggregationResults("first_name");
		assertThat(allLastnames.getMappedResults()).containsExactlyInAnyOrder(//
				new UserAggregate("Skywalker", List.of("Anakin", "Luke")), //
				new UserAggregate("Organa", List.of("Leia")), //
				new UserAggregate("Solo", List.of("Han", "Ben")));
	}

	@Test // GH-4970
	void testAggregationStreamWithProjectedResultsWrappedInAggregationResults() {

		List<UserAggregate> allLastnames = fragment.streamGroupByLastnameAndAsAggregationResults("first_name").toList();
		assertThat(allLastnames).containsExactlyInAnyOrder(//
				new UserAggregate("Skywalker", List.of("Anakin", "Luke")), //
				new UserAggregate("Organa", List.of("Leia")), //
				new UserAggregate("Solo", List.of("Han", "Ben")));
	}

	@Test
	void testAggregationWithSingleResultExtraction() {
		assertThat(fragment.sumPosts()).isEqualTo(5);
	}

	@Test
	void testAggregationWithHint() {
		assertThatException().isThrownBy(() -> fragment.findAllLastnamesUsingIndex())
				.withMessageContaining("hint provided does not correspond to an existing index");
	}

	@Test
	void testAggregationWithReadPreference() {
		assertThatException().isThrownBy(() -> fragment.findAllLastnamesWithReadPreference())
				.withMessageContaining("No match for read preference");
	}

	@Test
	void testAggregationWithCollation() {
		assertThatException().isThrownBy(() -> fragment.findAllLastnamesWithCollation())
				.withMessageContaining("'locale' is invalid");
	}

	private static void initUsers() {

		Document luke = Document.parse("""
				{
				  "_id": "id-1",
				  "username": "luke",
				  "first_name": "Luke",
				  "last_name": "Skywalker",
				  "visits" : 2,
				  "lastSeen" : {
				    "$date": "2025-04-01T00:00:00.000Z"
				   },
				  "posts": [
				    {
				      "message": "I have a bad feeling about this.",
				      "date": {
				        "$date": "2025-01-15T12:50:33.855Z"
				      }
				    }
				  ],
				  "_class": "example.springdata.aot.User"
				}""");

		Document leia = Document.parse("""
				{
				  "_id": "id-2",
				  "username": "leia",
				  "first_name": "Leia",
				  "last_name": "Organa",
				  "_class": "example.springdata.aot.User"
				}""");

		Document han = Document.parse("""
				{
				  "_id": "id-3",
				  "username": "han",
				  "first_name": "Han",
				  "last_name": "Solo",
				  "posts": [
				    {
				      "message": "It's the ship that made the Kessel Run in less than 12 Parsecs.",
				      "date": {
				        "$date": "2025-01-15T13:30:33.855Z"
				      }
				    }
				  ],
				  "_class": "example.springdata.aot.User"
				}""");

		Document chwebacca = Document.parse("""
				{
				  "_id": "id-4",
				  "username": "chewbacca",
				  "lastSeen" : {
				    "$date": "2025-01-01T00:00:00.000Z"
				   },
				  "_class": "example.springdata.aot.User"
				}""");

		Document yoda = Document.parse(
				"""
						{
						  "_id": "id-5",
						  "username": "yoda",
						  "visits" : 1000,
						  "posts": [
						    {
						      "message": "Do. Or do not. There is no try.",
						      "date": {
						        "$date": "2025-01-15T13:09:33.855Z"
						      }
						    },
						    {
						      "message": "Decide you must, how to serve them best. If you leave now, help them you could; but you would destroy all for which they have fought, and suffered.",
						      "date": {
						        "$date": "2025-01-15T13:53:33.855Z"
						      }
						    }
						  ]
						}""");

		Document vader = Document.parse("""
				{
				  "_id": "id-6",
				  "username": "vader",
				  "first_name": "Anakin",
				  "last_name": "Skywalker",
				  "visits" : 50,
				  "posts": [
				    {
				      "message": "I am your father",
				      "date": {
				        "$date": "2025-01-15T13:46:33.855Z"
				      }
				    }
				  ]
				}""");

		Document kylo = Document.parse("""
				{
				  "_id": "id-7",
				  "username": "kylo",
				  "first_name": "Ben",
				  "last_name": "Solo"
				}
				""");

		client.getDatabase(DB_NAME).getCollection("user")
				.insertMany(List.of(luke, leia, han, chwebacca, yoda, vader, kylo));
	}
}
