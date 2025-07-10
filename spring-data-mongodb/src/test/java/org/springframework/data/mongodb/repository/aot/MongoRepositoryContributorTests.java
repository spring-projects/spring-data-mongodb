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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import example.aot.User;
import example.aot.UserProjection;
import example.aot.UserRepository;
import example.aot.UserRepository.UserAggregate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Similarity;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.Window;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.test.util.AtlasContainer;
import org.springframework.data.mongodb.test.util.EnableIfVectorSearchAvailable;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.StringUtils;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;

/**
 * Integration tests for the {@link UserRepository} AOT fragment.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringJUnitConfig(classes = MongoRepositoryContributorTests.MongoRepositoryContributorConfiguration.class)
class MongoRepositoryContributorTests {

	private static final @Container AtlasContainer atlasLocal = AtlasContainer.bestMatch();
	private static final String DB_NAME = "aot-repo-tests";
	private static final String COLLECTION_NAME = "user";

	static MongoClient client;
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

	@BeforeAll
	static void beforeAll() throws InterruptedException {

		client = MongoClients.create(atlasLocal.getConnectionString());
		MongoCollection<Document> userCollection = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
		userCollection.createIndex(new Document("location.coordinates", "2d"), new IndexOptions());
		userCollection.createIndex(new Document("location.coordinates", "2dsphere"), new IndexOptions());

		Thread.sleep(250); // just wait a little or the index will be broken
	}

	/**
	 * Create the vector search index and wait till it is queryable and actually serving data. Since this may slow down
	 * tests quite a bit, better call it only when needed to run certain tests.
	 */
	private static void initializeVectorIndex() {

		String indexName = "embedding.vector_cos";

		Document searchIndex = new Document("fields",
				List.of(new Document("type", "vector").append("path", "embedding").append("numDimensions", 5)
						.append("similarity", "cosine"), new Document("type", "filter").append("path", "last_name")));

		MongoCollection<Document> userCollection = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
		userCollection.createSearchIndexes(
				List.of(new SearchIndexModel(indexName, searchIndex, SearchIndexType.of(new BsonString("vectorSearch")))));

		// wait for search index to be queryable

		Awaitility.await().atLeast(Duration.ofMillis(50)).atMost(Duration.ofSeconds(120))
				.pollInterval(Duration.ofMillis(250)).until(() -> {
					return MongoTestUtils.isSearchIndexReady(indexName, client, DB_NAME, COLLECTION_NAME);
				});

		Document $vectorSearch = new Document("$vectorSearch",
				new Document("index", indexName).append("limit", 1).append("numCandidates", 20).append("path", "embedding")
						.append("queryVector", List.of(1.0, 1.12345, 2.23456, 3.34567, 4.45678)));

		// wait for search index to serve data
		Awaitility.await().atLeast(Duration.ofMillis(50)).atMost(Duration.ofSeconds(120)).ignoreExceptions()
				.pollInterval(Duration.ofMillis(250)).until(() -> {
					try (MongoCursor<Document> cursor = userCollection.aggregate(List.of($vectorSearch)).iterator()) {
						if (cursor.hasNext()) {
							Document next = cursor.next();
							return true;
						}
						return false;
					}
				});
	}

	@BeforeEach
	void beforeEach() throws InterruptedException {
		initUsers();
	}

	@AfterEach
	void afterEach() {
		MongoTestUtils.flushCollection(DB_NAME, "user", client);
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

	@Test // GH-4939
	void testRegex() {

		List<User> lukes = fragment.findByFirstnameRegex(Pattern.compile(".*uk.*"));
		assertThat(lukes).extracting(User::getUsername).containsExactly("luke");
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

	@Test // GH-5006
	void testAnnotatedFinderWithExpressionUsingParameterIndex() {

		List<User> users = fragment.findWithExpressionUsingParameterIndex("Luke");
		assertThat(users).extracting(User::getUsername).containsExactly("luke");
	}

	@Test // GH-5006
	void testAnnotatedFinderWithExpressionUsingParameterName() {

		List<User> users = fragment.findWithExpressionUsingParameterName("Luke");
		assertThat(users).extracting(User::getUsername).containsExactly("luke");
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

	@Test // GH-5004
	void testNear() {

		List<User> users = fragment.findByLocationCoordinatesNear(new Point(-73.99171, 40.738868));
		assertThat(users).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void testNearWithGeoJson() {

		List<User> users = fragment.findByLocationCoordinatesNear(new GeoJsonPoint(-73.99171, 40.738868));
		assertThat(users).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void testGeoWithinCircle() {

		List<User> users = fragment.findByLocationCoordinatesWithin(new Circle(-78.99171, 45.738868, 170));
		assertThat(users).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void testWithinBox() {

		Box box = new Box(new Point(-78.99171, 35.738868), new Point(-68.99171, 45.738868));

		List<User> result = fragment.findByLocationCoordinatesWithin(box);
		assertThat(result).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void findsPeopleByLocationWithinPolygon() {

		Point first = new Point(-78.99171, 35.738868);
		Point second = new Point(-78.99171, 45.738868);
		Point third = new Point(-68.99171, 45.738868);
		Point fourth = new Point(-68.99171, 35.738868);

		List<User> result = fragment.findByLocationCoordinatesWithin(new Polygon(first, second, third, fourth));
		assertThat(result).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void findsPeopleByLocationWithinGeoJsonPolygon() {

		Point first = new Point(-78.99171, 35.738868);
		Point second = new Point(-78.99171, 45.738868);
		Point third = new Point(-68.99171, 45.738868);
		Point fourth = new Point(-68.99171, 35.738868);

		List<User> result = fragment
				.findByLocationCoordinatesWithin(new GeoJsonPolygon(first, second, third, fourth, first));
		assertThat(result).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void findsPeopleByLocationWithinSomeGenericGeoJsonObject() {

		Point first = new Point(-78.99171, 35.738868);
		Point second = new Point(-78.99171, 45.738868);
		Point third = new Point(-68.99171, 45.738868);
		Point fourth = new Point(-68.99171, 35.738868);

		List<User> result = fragment
				.findUserByLocationCoordinatesWithin(new GeoJsonPolygon(first, second, third, fourth, first));
		assertThat(result).extracting(User::getUsername).containsExactly("leia", "vader");
	}

	@Test // GH-5004
	void testNearWithGeoResult() {

		GeoResults<User> users = fragment.findByLocationCoordinatesNear(new Point(-73.99, 40.73),
				Distance.of(5, Metrics.KILOMETERS));
		assertThat(users).extracting(GeoResult::getContent).extracting(User::getUsername).containsExactly("leia");
	}

	@Test // GH-5004
	void testNearWithAdditionalFilterQueryAsGeoResult() {

		GeoResults<User> users = fragment.findByLocationCoordinatesNearAndLastname(new Point(-73.99, 40.73),
				Distance.of(50, Metrics.KILOMETERS), "Organa");
		assertThat(users).extracting(GeoResult::getContent).extracting(User::getUsername).containsExactly("leia");
	}

	@Test // GH-5004
	void testNearReturningListOfGeoResult() {

		List<GeoResult<User>> users = fragment.findUserAsListByLocationCoordinatesNear(new Point(-73.99, 40.73),
				Distance.of(5, Metrics.KILOMETERS));
		assertThat(users).extracting(GeoResult::getContent).extracting(User::getUsername).containsExactly("leia");
	}

	@Test // GH-5004
	void testNearWithRange() {

		Range<Distance> range = Distance.between(Distance.of(5, Metrics.KILOMETERS), Distance.of(2000, Metrics.KILOMETERS));
		GeoResults<User> users = fragment.findByLocationCoordinatesNear(new Point(-73.99, 40.73), range);

		assertThat(users).extracting(GeoResult::getContent).extracting(User::getUsername).containsExactly("vader");
	}

	@Test // GH-5004
	void testNearReturningGeoPage() {

		GeoPage<User> page1 = fragment.findByLocationCoordinatesNear(new Point(-73.99, 40.73),
				Distance.of(2000, Metrics.KILOMETERS), PageRequest.of(0, 1));

		assertThat(page1.hasNext()).isTrue();

		GeoPage<User> page2 = fragment.findByLocationCoordinatesNear(new Point(-73.99, 40.73),
				Distance.of(2000, Metrics.KILOMETERS), page1.nextPageable());
		assertThat(page2.hasNext()).isFalse();
	}

	@RetryingTest(3)
	@EnableIfVectorSearchAvailable(database = DB_NAME, collection = User.class)
	void vectorSearchFromAnnotation() {

		initializeVectorIndex();

		Vector vector = Vector.of(1.00000d, 1.12345d, 2.23456d, 3.34567d, 4.45678d);
		SearchResults<User> results = fragment.annotatedVectorSearch("Skywalker", vector, Score.of(0.99), Limit.of(10));

		assertThat(results).hasSize(1);
	}

	@RetryingTest(3)
	@EnableIfVectorSearchAvailable(database = DB_NAME, collection = User.class)
	void vectorSearchWithDerivedQuery() {

		initializeVectorIndex();

		Vector vector = Vector.of(1.00000d, 1.12345d, 2.23456d, 3.34567d, 4.45678d);
		SearchResults<User> results = fragment.searchCosineByLastnameAndEmbeddingNear("Skywalker", vector, Score.of(0.98),
				Limit.of(10));

		assertThat(results).hasSize(1);
	}

	@RetryingTest(3)
	@EnableIfVectorSearchAvailable(database = DB_NAME, collection = User.class)
	void vectorSearchReturningResultsAsList() {

		initializeVectorIndex();

		Vector vector = Vector.of(1.00000d, 1.12345d, 2.23456d, 3.34567d, 4.45678d);
		List<User> results = fragment.searchAsListByLastnameAndEmbeddingNear("Skywalker", vector, Limit.of(10));

		assertThat(results).hasSize(2);
	}

	@RetryingTest(3)
	@EnableIfVectorSearchAvailable(database = DB_NAME, collection = User.class)
	void vectorSearchWithLimitFromAnnotation() {

		initializeVectorIndex();

		Vector vector = Vector.of(1.00000d, 1.12345d, 2.23456d, 3.34567d, 4.45678d);
		SearchResults<User> results = fragment.searchByLastnameAndEmbeddingWithin("Skywalker", vector,
				Similarity.between(0.4, 0.99));

		assertThat(results).hasSize(1);
	}

	@RetryingTest(3)
	@EnableIfVectorSearchAvailable(database = DB_NAME, collection = User.class)
	void vectorSearchWithSorting() {

		initializeVectorIndex();

		Vector vector = Vector.of(1.00000d, 1.12345d, 2.23456d, 3.34567d, 4.45678d);
		SearchResults<User> results = fragment.searchByLastnameAndEmbeddingWithinOrderByFirstname("Skywalker", vector,
				Similarity.between(0.4, 1.0));

		assertThat(results).hasSize(2);
	}

	@RetryingTest(3)
	@EnableIfVectorSearchAvailable(database = DB_NAME, collection = User.class)
	void vectorSearchWithLimitFromDerivedQuery() {

		initializeVectorIndex();

		Vector vector = Vector.of(1.00000d, 1.12345d, 2.23456d, 3.34567d, 4.45678d);
		SearchResults<User> results = fragment.searchTop1ByLastnameAndEmbeddingWithin("Skywalker", vector,
				Similarity.between(0.4, 1.0));

		assertThat(results).hasSize(1);
	}

	/**
	 * GeoResults<Person> results = repository.findPersonByLocationNear(new Point(-73.99, 40.73), range);
	 */
	private static void initUsers() throws InterruptedException {

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
				  "embedding" : [1.00000, 1.12345, 2.23456, 3.34567, 4.45678],
				  "_class": "example.springdata.aot.User"
				}""");

		Document leia = Document.parse("""
				{
				  "_id": "id-2",
				  "username": "leia",
				  "first_name": "Leia",
				  "last_name": "Organa",
				  "location" : {
				    "planet" : "Coruscant",
				    "coordinates" : {
				      "x" : -73.99171, "y" : 40.738868
				    }
				  },
				  "embedding" : [1.0001, 2.12345, 3.23456, 4.34567, 5.45678],
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
				  "embedding" : [2.0002, 3.12345, 4.23456, 5.34567, 6.45678],
				  "_class": "example.springdata.aot.User"
				}""");

		Document chwebacca = Document.parse("""
				{
				  "_id": "id-4",
				  "username": "chewbacca",
				  "lastSeen" : {
				    "$date": "2025-01-01T00:00:00.000Z"
				   },
				   "embedding" : [3.0003, 4.12345, 5.23456, 6.34567, 7.45678],
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
						  ],
						  "embedding" : [4.0004, 5.12345, 6.23456, 7.34567, 8.45678]
						}""");

		Document vader = Document.parse("""
				{
				  "_id": "id-6",
				  "username": "vader",
				  "first_name": "Anakin",
				  "last_name": "Skywalker",
				  "location" : {
				    "planet" : "Death Star",
				    "coordinates" : {
				      "x" : -73.9, "y" : 40.7
				    }
				  },
				  "visits" : 50,
				  "posts": [
				    {
				      "message": "I am your father",
				      "date": {
				        "$date": "2025-01-15T13:46:33.855Z"
				      }
				    }
				  ],
				  "embedding" : [5.0005, 6.12345, 7.23456, 8.34567, 9.45678]
				}""");

		Document kylo = Document.parse("""
				{
				  "_id": "id-7",
				  "username": "kylo",
				  "first_name": "Ben",
				  "last_name": "Solo",
				  "embedding" : [6.0006, 7.12345, 8.23456, 9.34567, 10.45678]
				}
				""");

		client.getDatabase(DB_NAME).getCollection("user")
				.insertMany(List.of(luke, leia, han, chwebacca, yoda, vader, kylo));
	}
}
