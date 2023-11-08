/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.springframework.data.domain.Sort.Direction.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.io.BufferedInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Stream;

import org.assertj.core.data.Offset;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.TestEntities;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.aggregation.AggregationTests.CarDescriptor.Entry;
import org.springframework.data.mongodb.core.aggregation.BucketAutoOperation.Granularities;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.MongoVersion;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.MongoCollection;

/**
 * Tests for {@link MongoTemplate#aggregate(Aggregation, Class, Class)}.
 *
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Nikolay Bogdanov
 * @author Maninder Singh
 * @author Sergey Shcherbakov
 * @author Minsu Kim
 * @author Sangyong Choi
 * @author Julia Lee
 */
@ExtendWith(MongoTemplateExtension.class)
public class AggregationTests {

	private static final String INPUT_COLLECTION = "aggregation_test_collection";

	private static boolean initialized = false;
	private static List<Document> documents = parseDocuments();

	@Template //
	private static MongoTestTemplate mongoTemplate;

	@BeforeEach
	void setUp() {

		cleanDb();
		initSampleDataIfNecessary();
	}

	@AfterEach
	void cleanUp() {
		cleanDb();
	}

	private void cleanDb() {

		mongoTemplate.flush(Product.class, UserWithLikes.class, DATAMONGO753.class, Data.class, DATAMONGO788.class,
				User.class, Person.class, Reservation.class, Venue.class, MeterData.class, LineItem.class, InventoryItem.class,
				Sales.class, Sales2.class, Employee.class, Art.class, Venue.class, Item.class);

		mongoTemplate.dropCollection(INPUT_COLLECTION);
		mongoTemplate.dropCollection("personQueryTemp");
	}

	/**
	 * Imports the sample dataset (zips.json) if necessary (e.g. if it doesn't exist yet). The dataset can originally be
	 * found on the mongodb aggregation framework example website:
	 *
	 * @see <a href="https://docs.mongodb.org/manual/tutorial/aggregation-examples/">MongoDB Aggregation Examples</a>
	 */
	private void initSampleDataIfNecessary() {

		if (!initialized) {

			mongoTemplate.dropCollection(ZipInfo.class);

			mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ZipInfo.class).insert(documents).execute();

			long count = mongoTemplate.count(new Query(), ZipInfo.class);
			assertThat(count).isEqualTo(29467L);

			initialized = true;
		}
	}

	static List<Document> parseDocuments() {

		Scanner scanner = null;
		List<Document> documents = new ArrayList<>(30000);

		try {
			scanner = new Scanner(new BufferedInputStream(new ClassPathResource("zips.json").getInputStream()));
			while (scanner.hasNextLine()) {
				String zipInfoRecord = scanner.nextLine();
				documents.add(Document.parse(zipInfoRecord));
			}
		} catch (Exception e) {
			if (scanner != null) {
				scanner.close();
			}
			throw new RuntimeException("Could not load mongodb sample dataset", e);
		}

		return documents;
	}

	@Test // DATAMONGO-586
	void shouldHandleMissingInputCollection() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> mongoTemplate.aggregate(newAggregation(), (String) null, TagCount.class));
	}

	@Test // DATAMONGO-586
	void shouldHandleMissingAggregationPipeline() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> mongoTemplate.aggregate(null, INPUT_COLLECTION, TagCount.class));
	}

	@Test // DATAMONGO-586
	void shouldHandleMissingEntityClass() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> mongoTemplate.aggregate(newAggregation(), INPUT_COLLECTION, null));
	}

	@Test // DATAMONGO-586
	void shouldAggregate() {

		createTagDocuments();

		Aggregation agg = newAggregation( //
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("n"), //
				project("n") //
						.and("tag").previousOperation(), //
				sort(DESC, "n") //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(agg, INPUT_COLLECTION, TagCount.class);

		assertThat(results).isNotNull();

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount).isNotNull();
		assertThat(tagCount.size()).isEqualTo(3);

		assertTagCount("spring", 3, tagCount.get(0));
		assertTagCount("mongodb", 2, tagCount.get(1));
		assertTagCount("nosql", 1, tagCount.get(2));
	}

	@Test // DATAMONGO-1637
	void shouldAggregateAndStream() {

		createTagDocuments();

		Aggregation agg = newAggregation( //
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("n"), //
				project("n") //
						.and("tag").previousOperation(), //
				sort(DESC, "n") //
		).withOptions(new AggregationOptions(true, false, 1));

		try (Stream<TagCount> stream = mongoTemplate.aggregateStream(agg, INPUT_COLLECTION, TagCount.class)) {

			List<TagCount> tagCount = stream.toList();

			assertThat(tagCount).isNotNull();
			assertThat(tagCount.size()).isEqualTo(3);

			assertTagCount("spring", 3, tagCount.get(0));
			assertTagCount("mongodb", 2, tagCount.get(1));
			assertTagCount("nosql", 1, tagCount.get(2));
		}
	}

	@Test // DATAMONGO-586
	void shouldAggregateEmptyCollection() {

		Aggregation aggregation = newAggregation(//
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("n"), //
				project("n") //
						.and("tag").previousOperation(), //
				sort(DESC, "n") //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(aggregation, INPUT_COLLECTION, TagCount.class);

		assertThat(results).isNotNull();

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount).isNotNull();
		assertThat(tagCount.size()).isEqualTo(0);
	}

	@Test // DATAMONGO-1637
	void shouldAggregateEmptyCollectionAndStream() {

		Aggregation aggregation = newAggregation(//
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("n"), //
				project("n") //
						.and("tag").previousOperation(), //
				sort(DESC, "n") //
		);

		try (Stream<TagCount> stream = mongoTemplate.aggregateStream(aggregation, INPUT_COLLECTION, TagCount.class)) {

			List<TagCount> tagCount = stream.toList();

			assertThat(tagCount.size()).isEqualTo(0);
		}
	}

	@Test // DATAMONGO-1391
	void shouldUnwindWithIndex() {

		MongoCollection<Document> coll = mongoTemplate.getCollection(INPUT_COLLECTION);

		coll.insertOne(createDocument("Doc1", "spring", "mongodb", "nosql"));
		coll.insertOne(createDocument("Doc2"));

		Aggregation agg = newAggregation( //
				project("tags"), //
				unwind("tags", "n"), //
				project("n") //
						.and("tag").previousOperation(), //
				sort(DESC, "n") //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(agg, INPUT_COLLECTION, TagCount.class);

		assertThat(results).isNotNull();

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount).isNotNull();
		assertThat(tagCount.size()).isEqualTo(3);
	}

	@Test // DATAMONGO-1391
	@EnableIfMongoServerVersion(isLessThan = "6.0") // $sort does not seem to have an effect on $unwind
	void shouldUnwindPreserveEmpty() {

		MongoCollection<Document> coll = mongoTemplate.getCollection(INPUT_COLLECTION);

		coll.insertOne(createDocument("Doc1", "spring", "mongodb", "nosql"));
		coll.insertOne(createDocument("Doc2"));

		Aggregation agg = newAggregation( //
				project("tags"), //
				unwind("tags", "n", true), //
				sort(DESC, "n") //
		);

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, INPUT_COLLECTION, Document.class);

		assertThat(results).isNotNull();

		List<Document> tagCount = results.getMappedResults();

		assertThat(tagCount).isNotNull();
		assertThat(tagCount.size()).isEqualTo(4);
		assertThat(tagCount.get(0)).containsEntry("n", 2L);
		assertThat(tagCount.get(3)).containsEntry("n", null);
	}

	@Test // DATAMONGO-586
	void shouldDetectResultMismatch() {

		createTagDocuments();

		Aggregation aggregation = newAggregation( //
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("count"), // count field not present
				limit(2) //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(aggregation, INPUT_COLLECTION, TagCount.class);

		assertThat(results).isNotNull();

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount).isNotNull();
		assertThat(tagCount.size()).isEqualTo(2);
		assertTagCount(null, 0, tagCount.get(0));
		assertTagCount(null, 0, tagCount.get(1));
	}

	@Test // DATAMONGO-1637
	void shouldDetectResultMismatchWhileStreaming() {

		createTagDocuments();

		Aggregation aggregation = newAggregation( //
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("count"), // count field not present
				limit(2) //
		);

		try (Stream<TagCount> stream = mongoTemplate.aggregateStream(aggregation, INPUT_COLLECTION, TagCount.class)) {

			List<TagCount> tagCount = stream.toList();

			assertThat(tagCount.size()).isEqualTo(2);
			assertTagCount(null, 0, tagCount.get(0));
			assertTagCount(null, 0, tagCount.get(1));
		}
	}

	@Test // DATAMONGO-586
	void complexAggregationFrameworkUsageLargestAndSmallestCitiesByState() {
		/*
		 //complex mongodb aggregation framework example from https://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state
		db.zipInfo.aggregate(
			{
			   $group: {
			      _id: {
			         state: '$state',
			         city: '$city'
			      },
			      pop: {
			         $sum: '$pop'
			      }
			   }
			},
			{
			   $sort: {
			      pop: 1,
			      '_id.state': 1,
			      '_id.city': 1
			   }
			},
			{
			   $group: {
			      _id: '$_id.state',
			      biggestCity: {
			         $last: '$_id.city'
			      },
			      biggestPop: {
			         $last: '$pop'
			      },
			      smallestCity: {
			         $first: '$_id.city'
			      },
			      smallestPop: {
			         $first: '$pop'
			      }
			   }
			},
			{
			   $project: {
			      _id: 0,
			      state: '$_id',
			      biggestCity: {
			         name: '$biggestCity',
			         pop: '$biggestPop'
			      },
			      smallestCity: {
			         name: '$smallestCity',
			         pop: '$smallestPop'
			      }
			   }
			},
			{
			   $sort: {
			      state: 1
			   }
			}
		)
		*/

		TypedAggregation<ZipInfo> aggregation = newAggregation(ZipInfo.class, //
				group("state", "city").sum("population").as("pop"), //
				sort(ASC, "pop", "state", "city"), //
				group("state") //
						.last("city").as("biggestCity") //
						.last("pop").as("biggestPop") //
						.first("city").as("smallestCity") //
						.first("pop").as("smallestPop"), //
				project() //
						.and("state").previousOperation() //
						.and("biggestCity").nested(bind("name", "biggestCity").and("population", "biggestPop")) //
						.and("smallestCity").nested(bind("name", "smallestCity").and("population", "smallestPop")), //
				sort(ASC, "state") //
		);

		assertThat(aggregation).isNotNull();
		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<ZipInfoStats> result = mongoTemplate.aggregate(aggregation, ZipInfoStats.class);
		assertThat(result).isNotNull();
		assertThat(result.getMappedResults()).isNotNull();
		assertThat(result.getMappedResults().size()).isEqualTo(51);

		ZipInfoStats firstZipInfoStats = result.getMappedResults().get(0);
		assertThat(firstZipInfoStats).isNotNull();
		assertThat(firstZipInfoStats.id).isNull();
		assertThat(firstZipInfoStats.state).isEqualTo("AK");
		assertThat(firstZipInfoStats.smallestCity).isNotNull();
		assertThat(firstZipInfoStats.smallestCity.name).isEqualTo("CHEVAK");
		assertThat(firstZipInfoStats.smallestCity.population).isEqualTo(0);
		assertThat(firstZipInfoStats.biggestCity).isNotNull();
		assertThat(firstZipInfoStats.biggestCity.name).isEqualTo("ANCHORAGE");
		assertThat(firstZipInfoStats.biggestCity.population).isEqualTo(183987);

		ZipInfoStats lastZipInfoStats = result.getMappedResults().get(50);
		assertThat(lastZipInfoStats).isNotNull();
		assertThat(lastZipInfoStats.id).isNull();
		assertThat(lastZipInfoStats.state).isEqualTo("WY");
		assertThat(lastZipInfoStats.smallestCity).isNotNull();
		assertThat(lastZipInfoStats.smallestCity.name).isEqualTo("LOST SPRINGS");
		assertThat(lastZipInfoStats.smallestCity.population).isEqualTo(6);
		assertThat(lastZipInfoStats.biggestCity).isNotNull();
		assertThat(lastZipInfoStats.biggestCity.name).isEqualTo("CHEYENNE");
		assertThat(lastZipInfoStats.biggestCity.population).isEqualTo(70185);
	}

	@Test // DATAMONGO-586
	void findStatesWithPopulationOver10MillionAggregationExample() {
		/*
		 //complex mongodb aggregation framework example from
		 https://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state
		
		 db.zipcodes.aggregate(
			 	{
				   $group: {
				      _id:"$state",
				      totalPop:{ $sum:"$pop"}
		 			 }
				},
				{
		 			$sort: { _id: 1, "totalPop": 1 }
		 		},
				{
				   $match: {
				      totalPop: { $gte:10*1000*1000 }
				   }
				}
		)
		  */

		TypedAggregation<ZipInfo> agg = newAggregation(ZipInfo.class, //
				group("state") //
						.sum("population").as("totalPop"), //
				sort(ASC, previousOperation(), "totalPop"), //
				match(where("totalPop").gte(10 * 1000 * 1000)) //
		);

		assertThat(agg).isNotNull();
		assertThat(agg.toString()).isNotNull();

		AggregationResults<StateStats> result = mongoTemplate.aggregate(agg, StateStats.class);
		assertThat(result).isNotNull();
		assertThat(result.getMappedResults()).isNotNull();
		assertThat(result.getMappedResults().size()).isEqualTo(7);

		StateStats stateStats = result.getMappedResults().get(0);
		assertThat(stateStats).isNotNull();
		assertThat(stateStats.id).isEqualTo("CA");
		assertThat(stateStats.state).isNull();
		assertThat(stateStats.totalPopulation).isEqualTo(29760021);
	}

	/**
	 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/cond/#example">MongoDB Aggregation
	 *      Framework: $cond</a>
	 */
	@Test // DATAMONGO-861
	void aggregationUsingConditionalProjectionToCalculateDiscount() {

		/*
		db.inventory.aggregate(
		[
		  {
		     $project:
		       {
		         item: 1,
		         discount:
		           {
		             $cond: { if: { $gte: [ "$qty", 250 ] }, then: 30, else: 20 }
		           }
		       }
		  }
		]
		)
		 */

		mongoTemplate.insert(new InventoryItem(1, "abc1", 300));
		mongoTemplate.insert(new InventoryItem(2, "abc2", 200));
		mongoTemplate.insert(new InventoryItem(3, "xyz1", 250));

		TypedAggregation<InventoryItem> aggregation = newAggregation(InventoryItem.class, //
				project("item") //
						.and("discount")//
						.applyCondition(ConditionalOperators.Cond.newBuilder().when(Criteria.where("qty").gte(250)) //
								.then(30) //
								.otherwise(20)));

		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(3);

		Document first = result.getMappedResults().get(0);
		assertThat(first.get("_id")).isEqualTo((Object) 1);
		assertThat(first.get("discount")).isEqualTo((Object) 30);

		Document second = result.getMappedResults().get(1);
		assertThat(second.get("_id")).isEqualTo((Object) 2);
		assertThat(second.get("discount")).isEqualTo((Object) 20);

		Document third = result.getMappedResults().get(2);
		assertThat(third.get("_id")).isEqualTo((Object) 3);
		assertThat(third.get("discount")).isEqualTo((Object) 30);
	}

	/**
	 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/ifNull/#example">MongoDB Aggregation
	 *      Framework: $ifNull</a>
	 */
	@Test // DATAMONGO-861
	void aggregationUsingIfNullToProjectSaneDefaults() {

		/*
		db.inventory.aggregate(
		[
		  {
		     $project: {
		        item: 1,
		        description: { $ifNull: [ "$description", "Unspecified" ] }
		     }
		  }
		]
		)
		 */

		mongoTemplate.insert(new InventoryItem(1, "abc1", "product 1", 300));
		mongoTemplate.insert(new InventoryItem(2, "abc2", 200));
		mongoTemplate.insert(new InventoryItem(3, "xyz1", 250));

		TypedAggregation<InventoryItem> aggregation = newAggregation(InventoryItem.class, //
				project("item") //
						.and(ConditionalOperators.ifNull("description").then("Unspecified")) //
						.as("description")//
		);

		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(3);

		Document first = result.getMappedResults().get(0);
		assertThat(first.get("_id")).isEqualTo((Object) 1);
		assertThat(first.get("description")).isEqualTo((Object) "product 1");

		Document second = result.getMappedResults().get(1);
		assertThat(second.get("_id")).isEqualTo((Object) 2);
		assertThat(second.get("description")).isEqualTo((Object) "Unspecified");
	}

	@Test // DATAMONGO-861
	void aggregationUsingConditionalProjection() {

		TypedAggregation<ZipInfo> aggregation = newAggregation(ZipInfo.class, //
				project() //
						.and("largePopulation")//
						.applyCondition(ConditionalOperators.when(Criteria.where("population").gte(20000)) //
								.then(true) //
								.otherwise(false)) //
						.and("population").as("population"));

		assertThat(aggregation).isNotNull();
		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(29467);

		Document firstZipInfoStats = result.getMappedResults().get(0);
		assertThat(firstZipInfoStats.get("largePopulation")).isEqualTo((Object) false);
		assertThat(firstZipInfoStats.get("population")).isEqualTo((Object) 6055);
	}

	@Test // DATAMONGO-861
	void aggregationUsingNestedConditionalProjection() {

		TypedAggregation<ZipInfo> aggregation = newAggregation(ZipInfo.class, //
				project() //
						.and("size")//
						.applyCondition(ConditionalOperators.when(Criteria.where("population").gte(20000)) //
								.then(
										ConditionalOperators.when(Criteria.where("population").gte(200000)).then("huge").otherwise("small")) //
								.otherwise("small")) //
						.and("population").as("population"));

		assertThat(aggregation).isNotNull();
		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(29467);

		Document firstZipInfoStats = result.getMappedResults().get(0);
		assertThat(firstZipInfoStats.get("size")).isEqualTo((Object) "small");
		assertThat(firstZipInfoStats.get("population")).isEqualTo((Object) 6055);
	}

	@Test // DATAMONGO-861
	void aggregationUsingIfNullProjection() {

		mongoTemplate.insert(new LineItem("id", "caption", 0));
		mongoTemplate.insert(new LineItem("idonly", null, 0));

		TypedAggregation<LineItem> aggregation = newAggregation(LineItem.class, //
				project("id") //
						.and("caption")//
						.applyCondition(ConditionalOperators.ifNull("caption").then("unknown")),
				sort(ASC, "id"));

		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(2);

		Document id = result.getMappedResults().get(0);
		assertThat((String) id.get("caption")).isEqualTo("caption");

		Document idonly = result.getMappedResults().get(1);
		assertThat((String) idonly.get("caption")).isEqualTo("unknown");
	}

	@Test // DATAMONGO-861
	void aggregationUsingIfNullReplaceWithFieldReferenceProjection() {

		mongoTemplate.insert(new LineItem("id", "caption", 0));
		mongoTemplate.insert(new LineItem("idonly", null, 0));

		TypedAggregation<LineItem> aggregation = newAggregation(LineItem.class, //
				project("id") //
						.and("caption")//
						.applyCondition(ConditionalOperators.ifNull("caption").thenValueOf("id")),
				sort(ASC, "id"));

		assertThat(aggregation.toString()).isNotNull();

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(2);

		Document id = result.getMappedResults().get(0);
		assertThat((String) id.get("caption")).isEqualTo("caption");

		Document idonly = result.getMappedResults().get(1);
		assertThat((String) idonly.get("caption")).isEqualTo("idonly");
	}

	@Test // DATAMONGO-861
	void shouldAllowGroupingUsingConditionalExpressions() {

		mongoTemplate.dropCollection(CarPerson.class);

		CarPerson person1 = new CarPerson("first1", "last1", new CarDescriptor.Entry("MAKE1", "MODEL1", 2000),
				new CarDescriptor.Entry("MAKE1", "MODEL2", 2001));

		CarPerson person2 = new CarPerson("first2", "last2", new CarDescriptor.Entry("MAKE3", "MODEL4", 2014));
		CarPerson person3 = new CarPerson("first3", "last3", new CarDescriptor.Entry("MAKE2", "MODEL5", 2015));

		mongoTemplate.save(person1);
		mongoTemplate.save(person2);
		mongoTemplate.save(person3);

		TypedAggregation<CarPerson> agg = Aggregation.newAggregation(CarPerson.class,
				unwind("descriptors.carDescriptor.entries"), //
				project() //
						.and(ConditionalOperators //
								.when(Criteria.where("descriptors.carDescriptor.entries.make").is("MAKE1")).then("good")
								.otherwise("meh"))
						.as("make") //
						.and("descriptors.carDescriptor.entries.model").as("model") //
						.and("descriptors.carDescriptor.entries.year").as("year"), //
				group("make").avg(ConditionalOperators //
						.when(Criteria.where("year").gte(2012)) //
						.then(1) //
						.otherwise(9000)) //
						.as("score"),
				sort(ASC, "score"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);

		assertThat(result.getMappedResults()).hasSize(2);

		Document meh = result.getMappedResults().get(0);
		assertThat((String) meh.get("_id")).isEqualTo("meh");
		assertThat(((Number) meh.get("score")).longValue()).isEqualTo(1L);

		Document good = result.getMappedResults().get(1);
		assertThat((String) good.get("_id")).isEqualTo("good");
		assertThat(((Number) good.get("score")).longValue()).isEqualTo(9000L);
	}

	@Test // DATAMONGO-1784, DATAMONGO-2264
	void shouldAllowSumUsingConditionalExpressions() {

		mongoTemplate.dropCollection(CarPerson.class);

		CarPerson person1 = new CarPerson("first1", "last1", new CarDescriptor.Entry("MAKE1", "MODEL1", 2000),
				new CarDescriptor.Entry("MAKE1", "MODEL2", 2001));

		CarPerson person2 = new CarPerson("first2", "last2", new CarDescriptor.Entry("MAKE3", "MODEL4", 2014));
		CarPerson person3 = new CarPerson("first3", "last3", new CarDescriptor.Entry("MAKE2", "MODEL5", 2015));

		mongoTemplate.save(person1);
		mongoTemplate.save(person2);
		mongoTemplate.save(person3);

		TypedAggregation<CarPerson> agg = Aggregation.newAggregation(CarPerson.class,
				unwind("descriptors.carDescriptor.entries"), //
				project() //
						.and(ConditionalOperators //
								.when(Criteria.where("descriptors.carDescriptor.entries.make").is("MAKE1")).then("good")
								.otherwise("meh"))
						.as("make") //
						.and("descriptors.carDescriptor.entries.model").as("model") //
						.and("descriptors.carDescriptor.entries.year").as("year"), //
				group("make").sum(ConditionalOperators //
						.when(Criteria.where("year").gte(2012)) //
						.then(1) //
						.otherwise(9000)).as("score"),
				sort(ASC, "score"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);

		assertThat(result.getMappedResults()).hasSize(2);

		Document meh = result.getMappedResults().get(0);
		assertThat(meh.get("_id")).isEqualTo("meh");
		assertThat(((Number) meh.get("score")).longValue()).isEqualTo(2L);

		Document good = result.getMappedResults().get(1);
		assertThat(good.get("_id")).isEqualTo("good");
		assertThat(((Number) good.get("score")).longValue()).isEqualTo(18000L);
	}

	/**
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/tutorial/aggregation-with-user-preference-data/#return-the-five-most-common-likes">Return
	 *      the Five Most Common “Likes”</a>
	 */
	@Test // DATAMONGO-586
	void returnFiveMostCommonLikesAggregationFrameworkExample() {

		createUserWithLikesDocuments();

		TypedAggregation<UserWithLikes> agg = createUsersWithCommonLikesAggregation();

		assertThat(agg).isNotNull();
		assertThat(agg.toString()).isNotNull();

		AggregationResults<LikeStats> result = mongoTemplate.aggregate(agg, LikeStats.class);
		assertThat(result).isNotNull();
		assertThat(result.getMappedResults()).isNotNull();
		assertThat(result.getMappedResults().size()).isEqualTo(5);

		assertLikeStats(result.getMappedResults().get(0), "a", 4);
		assertLikeStats(result.getMappedResults().get(1), "b", 2);
		assertLikeStats(result.getMappedResults().get(2), "c", 4);
		assertLikeStats(result.getMappedResults().get(3), "d", 2);
		assertLikeStats(result.getMappedResults().get(4), "e", 3);
	}

	TypedAggregation<UserWithLikes> createUsersWithCommonLikesAggregation() {
		return newAggregation(UserWithLikes.class, //
				unwind("likes"), //
				group("likes").count().as("number"), //
				sort(DESC, "number"), //
				limit(5), //
				sort(ASC, previousOperation()) //
		);
	}

	@Test // DATAMONGO-586
	void arithmenticOperatorsInProjectionExample() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		mongoTemplate.insert(product);

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.and("netPrice").plus(1).as("netPricePlus1") //
						.and("netPrice").minus(1).as("netPriceMinus1") //
						.and("netPrice").multiply(2).as("netPriceMul2") //
						.and("netPrice").divide(1.19).as("netPriceDiv119") //
						.and("spaceUnits").mod(2).as("spaceUnitsMod2") //
						.and("spaceUnits").plus("spaceUnits").as("spaceUnitsPlusSpaceUnits") //
						.and("spaceUnits").minus("spaceUnits").as("spaceUnitsMinusSpaceUnits") //
						.and("spaceUnits").multiply("spaceUnits").as("spaceUnitsMultiplySpaceUnits") //
						.and("spaceUnits").divide("spaceUnits").as("spaceUnitsDivideSpaceUnits") //
						.and("spaceUnits").mod("spaceUnits").as("spaceUnitsModSpaceUnits") //
		);

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);
		List<Document> resultList = result.getMappedResults();

		assertThat(resultList).isNotNull();
		assertThat((String) resultList.get(0).get("_id")).isEqualTo(product.id);
		assertThat((String) resultList.get(0).get("name")).isEqualTo(product.name);
		assertThat((Double) resultList.get(0).get("netPricePlus1")).isEqualTo(product.netPrice + 1);
		assertThat((Double) resultList.get(0).get("netPriceMinus1")).isEqualTo(product.netPrice - 1);
		assertThat((Double) resultList.get(0).get("netPriceMul2")).isEqualTo(product.netPrice * 2);
		assertThat((Double) resultList.get(0).get("netPriceDiv119")).isEqualTo(product.netPrice / 1.19);
		assertThat((Integer) resultList.get(0).get("spaceUnitsMod2")).isEqualTo(product.spaceUnits % 2);
		assertThat((Integer) resultList.get(0).get("spaceUnitsPlusSpaceUnits"))
				.isEqualTo(product.spaceUnits + product.spaceUnits);
		assertThat((Integer) resultList.get(0).get("spaceUnitsMinusSpaceUnits"))
				.isEqualTo(product.spaceUnits - product.spaceUnits);
		assertThat((Integer) resultList.get(0).get("spaceUnitsMultiplySpaceUnits"))
				.isEqualTo(product.spaceUnits * product.spaceUnits);
		assertThat((Double) resultList.get(0).get("spaceUnitsDivideSpaceUnits"))
				.isEqualTo((double) (product.spaceUnits / product.spaceUnits));
		assertThat((Integer) resultList.get(0).get("spaceUnitsModSpaceUnits"))
				.isEqualTo(product.spaceUnits % product.spaceUnits);
	}

	@Test // DATAMONGO-774
	void expressionsInProjectionExample() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		mongoTemplate.insert(product);

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.andExpression("netPrice + 1").as("netPricePlus1") //
						.andExpression("netPrice - 1").as("netPriceMinus1") //
						.andExpression("netPrice / 2").as("netPriceDiv2") //
						.andExpression("netPrice * 1.19").as("grossPrice") //
						.andExpression("spaceUnits % 2").as("spaceUnitsMod2") //
						.andExpression("(netPrice * 0.8  + 1.2) * 1.19").as("grossPriceIncludingDiscountAndCharge") //

		);

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);
		List<Document> resultList = result.getMappedResults();

		assertThat(resultList).isNotNull();
		assertThat((String) resultList.get(0).get("_id")).isEqualTo(product.id);
		assertThat((String) resultList.get(0).get("name")).isEqualTo(product.name);
		assertThat((Double) resultList.get(0).get("netPricePlus1")).isEqualTo(product.netPrice + 1);
		assertThat((Double) resultList.get(0).get("netPriceMinus1")).isEqualTo(product.netPrice - 1);
		assertThat((Double) resultList.get(0).get("netPriceDiv2")).isEqualTo(product.netPrice / 2);
		assertThat((Double) resultList.get(0).get("grossPrice")).isEqualTo(product.netPrice * 1.19);
		assertThat((Integer) resultList.get(0).get("spaceUnitsMod2")).isEqualTo(product.spaceUnits % 2);
		assertThat((Double) resultList.get(0).get("grossPriceIncludingDiscountAndCharge"))
				.isEqualTo((product.netPrice * 0.8 + 1.2) * 1.19);
	}

	@Test // DATAMONGO-774
	void stringExpressionsInProjectionExample() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		mongoTemplate.insert(product);

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.andExpression("concat(name, '_bubu')").as("name_bubu") //
		);

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);
		List<Document> resultList = result.getMappedResults();

		assertThat(resultList).isNotNull();
		assertThat((String) resultList.get(0).get("_id")).isEqualTo(product.id);
		assertThat((String) resultList.get(0).get("name")).isEqualTo(product.name);
		assertThat((String) resultList.get(0).get("name_bubu")).isEqualTo(product.name + "_bubu");
	}

	@Test // DATAMONGO-774
	void expressionsInProjectionExampleShowcase() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		mongoTemplate.insert(product);

		double shippingCosts = 1.2;

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.andExpression("(netPrice * (1-discountRate)  + [0]) * (1+taxRate)", shippingCosts).as("salesPrice") //
		);

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);
		List<Document> resultList = result.getMappedResults();

		assertThat(resultList).isNotNull();
		Document firstItem = resultList.get(0);
		assertThat((String) firstItem.get("_id")).isEqualTo(product.id);
		assertThat((String) firstItem.get("name")).isEqualTo(product.name);
		assertThat((Double) firstItem.get("salesPrice"))
				.isEqualTo((product.netPrice * (1 - product.discountRate) + shippingCosts) * (1 + product.taxRate));
	}

	/**
	 * @see <a href=
	 *      "https://stackoverflow.com/questions/18653574/spring-data-mongodb-aggregation-framework-invalid-reference-in-group-operati">Spring
	 *      Data MongoDB - Aggregation Framework - invalid reference in group Operation</a>
	 */
	@Test // DATAMONGO-753
	void allowsNestedFieldReferencesAsGroupIdsInGroupExpressions() {

		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("A", 1), new PD("B", 1), new PD("C", 1)));
		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("B", 1), new PD("B", 1), new PD("C", 1)));

		TypedAggregation<DATAMONGO753> agg = newAggregation(DATAMONGO753.class, //
				unwind("pd"), //
				group("pd.pDch") // the nested field expression
						.sum("pd.up").as("uplift"), //
				project("_id", "uplift"), //
				sort(Sort.by("uplift")));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);
		List<Document> stats = result.getMappedResults();

		assertThat(stats.size()).isEqualTo(3);
		assertThat(stats.get(0).get("_id").toString()).isEqualTo("A");
		assertThat((Integer) stats.get(0).get("uplift")).isEqualTo(1);
		assertThat(stats.get(1).get("_id").toString()).isEqualTo("C");
		assertThat((Integer) stats.get(1).get("uplift")).isEqualTo(2);
		assertThat(stats.get(2).get("_id").toString()).isEqualTo("B");
		assertThat((Integer) stats.get(2).get("uplift")).isEqualTo(3);
	}

	/**
	 * @see <a href=
	 *      "https://stackoverflow.com/questions/18653574/spring-data-mongodb-aggregation-framework-invalid-reference-in-group-operati">Spring
	 *      Data MongoDB - Aggregation Framework - invalid reference in group Operation</a>
	 */
	@Test // DATAMONGO-753
	void aliasesNestedFieldInProjectionImmediately() {

		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("A", 1), new PD("B", 1), new PD("C", 1)));
		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("B", 1), new PD("B", 1), new PD("C", 1)));

		TypedAggregation<DATAMONGO753> agg = newAggregation(DATAMONGO753.class, //
				unwind("pd"), //
				project().and("pd.up").as("up"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, Document.class);
		List<Document> mappedResults = results.getMappedResults();

		assertThat(mappedResults).hasSize(6);
		for (Document element : mappedResults) {
			assertThat(element.get("up")).isEqualTo((Object) 1);
		}
	}

	@Test // DATAMONGO-774
	void shouldPerformDateProjectionOperatorsCorrectly() throws ParseException {

		Data data = new Data();
		data.stringValue = "ABC";
		mongoTemplate.insert(data);

		TypedAggregation<Data> agg = newAggregation(Data.class, project() //
				.andExpression("concat(stringValue, 'DE')").as("concat") //
				.andExpression("strcasecmp(stringValue,'XYZ')").as("strcasecmp") //
				.andExpression("substr(stringValue,1,1)").as("substr") //
				.andExpression("toLower(stringValue)").as("toLower") //
				.andExpression("toUpper(toLower(stringValue))").as("toUpper") //
		);

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, Document.class);
		Document document = results.getUniqueMappedResult();

		assertThat(document).isNotNull();
		assertThat((String) document.get("concat")).isEqualTo("ABCDE");
		assertThat((Integer) document.get("strcasecmp")).isEqualTo(-1);
		assertThat((String) document.get("substr")).isEqualTo("B");
		assertThat((String) document.get("toLower")).isEqualTo("abc");
		assertThat((String) document.get("toUpper")).isEqualTo("ABC");
	}

	@Test // DATAMONGO-774
	void shouldPerformStringProjectionOperatorsCorrectly() throws ParseException {

		Data data = new Data();
		data.dateValue = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSSZ").parse("29.08.1983 12:34:56.789+0000");
		mongoTemplate.insert(data);

		TypedAggregation<Data> agg = newAggregation(Data.class, project() //
				.andExpression("dayOfYear(dateValue)").as("dayOfYear") //
				.andExpression("dayOfMonth(dateValue)").as("dayOfMonth") //
				.andExpression("dayOfWeek(dateValue)").as("dayOfWeek") //
				.andExpression("year(dateValue)").as("year") //
				.andExpression("month(dateValue)").as("month") //
				.andExpression("week(dateValue)").as("week") //
				.andExpression("hour(dateValue)").as("hour") //
				.andExpression("minute(dateValue)").as("minute") //
				.andExpression("second(dateValue)").as("second") //
				.andExpression("millisecond(dateValue)").as("millisecond") //
		);

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, Document.class);
		Document document = results.getUniqueMappedResult();

		assertThat(document).isNotNull();
		assertThat((Integer) document.get("dayOfYear")).isEqualTo(241);
		assertThat((Integer) document.get("dayOfMonth")).isEqualTo(29);
		assertThat((Integer) document.get("dayOfWeek")).isEqualTo(2);
		assertThat((Integer) document.get("year")).isEqualTo(1983);
		assertThat((Integer) document.get("month")).isEqualTo(8);
		assertThat((Integer) document.get("week")).isEqualTo(35);
		assertThat((Integer) document.get("hour")).isEqualTo(12);
		assertThat((Integer) document.get("minute")).isEqualTo(34);
		assertThat((Integer) document.get("second")).isEqualTo(56);
		assertThat((Integer) document.get("millisecond")).isEqualTo(789);
	}

	@Test // DATAMONGO-1550
	void shouldPerformReplaceRootOperatorCorrectly() throws ParseException {

		Data data = new Data();
		DataItem dataItem = new DataItem();
		dataItem.primitiveIntValue = 42;
		data.item = dataItem;
		mongoTemplate.insert(data);

		TypedAggregation<Data> agg = newAggregation(Data.class, project("item"), //
				replaceRoot("item"), //
				project().and("primitiveIntValue").as("my_primitiveIntValue"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, Document.class);
		Document resultDocument = results.getUniqueMappedResult();

		assertThat(resultDocument).isNotNull();
		assertThat((Integer) resultDocument.get("my_primitiveIntValue")).isEqualTo(42);
		assertThat((Integer) resultDocument.keySet().size()).isEqualTo(1);
	}

	@Test // DATAMONGO-788, DATAMONGO-2264
	void referencesToGroupIdsShouldBeRenderedProperly() {

		mongoTemplate.insert(new DATAMONGO788(1, 1));
		mongoTemplate.insert(new DATAMONGO788(1, 1));
		mongoTemplate.insert(new DATAMONGO788(1, 1));
		mongoTemplate.insert(new DATAMONGO788(2, 1));
		mongoTemplate.insert(new DATAMONGO788(2, 1));

		AggregationOperation projectFirst = Aggregation.project("x", "y").and("xField").as("x").and("yField").as("y");
		AggregationOperation group = Aggregation.group("x", "y").count().as("xPerY");
		AggregationOperation project = Aggregation.project("xPerY", "x", "y").andExclude("_id");

		TypedAggregation<DATAMONGO788> aggregation = Aggregation.newAggregation(DATAMONGO788.class, projectFirst, group,
				project, Aggregation.sort(Sort.by("xPerY")));
		AggregationResults<Document> aggResults = mongoTemplate.aggregate(aggregation, Document.class);
		List<Document> items = aggResults.getMappedResults();

		assertThat(items.size()).isEqualTo(2);
		assertThat((Integer) items.get(0).get("xPerY")).isEqualTo(2);
		assertThat((Integer) items.get(0).get("x")).isEqualTo(2);
		assertThat((Integer) items.get(0).get("y")).isEqualTo(1);
		assertThat((Integer) items.get(1).get("xPerY")).isEqualTo(3);
		assertThat((Integer) items.get(1).get("x")).isEqualTo(1);
		assertThat((Integer) items.get(1).get("y")).isEqualTo(1);
	}

	@Test // DATAMONGO-806
	void shouldAllowGroupByIdFields() {

		mongoTemplate.dropCollection(User.class);

		Instant now = Instant.now();

		User user1 = new User("u1", new PushMessage("1", "aaa", now));
		User user2 = new User("u2", new PushMessage("2", "bbb", now.minus(2, ChronoUnit.DAYS)));
		User user3 = new User("u3", new PushMessage("3", "ccc", now.minus(1, ChronoUnit.DAYS)));

		mongoTemplate.save(user1);
		mongoTemplate.save(user2);
		mongoTemplate.save(user3);

		Aggregation agg = newAggregation( //
				project("id", "msgs"), //
				unwind("msgs"), //
				match(where("msgs.createDate").gt(Date.from(now.minus(1, ChronoUnit.DAYS)))), //
				group("id").push("msgs").as("msgs") //
		);

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, User.class, Document.class);

		List<Document> mappedResults = results.getMappedResults();

		Document firstItem = mappedResults.get(0);
		assertThat(firstItem.get("_id")).isNotNull();
		assertThat(String.valueOf(firstItem.get("_id"))).isEqualTo("u1");
	}

	@Test // DATAMONGO-840
	void shouldAggregateOrderDataToAnInvoice() {

		mongoTemplate.dropCollection(Order.class);

		double taxRate = 0.19;

		LineItem product1 = new LineItem("1", "p1", 1.23);
		LineItem product2 = new LineItem("2", "p2", 0.87, 2);
		LineItem product3 = new LineItem("3", "p3", 5.33);

		Order order = new Order("o4711", "c42", new Date()).addItem(product1).addItem(product2).addItem(product3);

		mongoTemplate.save(order);

		AggregationResults<Invoice> results = mongoTemplate.aggregate(newAggregation(Order.class, //
				match(where("id").is(order.getId())), unwind("items"), //
				project("id", "customerId", "items") //
						.andExpression("items.price * items.quantity").as("lineTotal"), //
				group("id") //
						.sum("lineTotal").as("netAmount") //
						.addToSet("items").as("items"), //
				project("id", "items", "netAmount") //
						.and("orderId").previousOperation() //
						.andExpression("netAmount * [0]", taxRate).as("taxAmount") //
						.andExpression("netAmount * (1 + [0])", taxRate).as("totalAmount") //
		), Invoice.class);

		Invoice invoice = results.getUniqueMappedResult();

		assertThat(invoice).isNotNull();
		assertThat(invoice.getOrderId()).isEqualTo(order.getId());
		assertThat(invoice.getNetAmount()).isCloseTo(8.3, Offset.offset(000001D));
		assertThat(invoice.getTaxAmount()).isCloseTo(1.577, Offset.offset(000001D));
		assertThat(invoice.getTotalAmount()).isCloseTo(9.877, Offset.offset(000001D));
	}

	@Test // DATAMONGO-924
	void shouldAllowGroupingByAliasedFieldDefinedInFormerAggregationStage() {

		mongoTemplate.dropCollection(CarPerson.class);

		CarPerson person1 = new CarPerson("first1", "last1", new CarDescriptor.Entry("MAKE1", "MODEL1", 2000),
				new CarDescriptor.Entry("MAKE1", "MODEL2", 2001), new CarDescriptor.Entry("MAKE2", "MODEL3", 2010),
				new CarDescriptor.Entry("MAKE3", "MODEL4", 2014));

		CarPerson person2 = new CarPerson("first2", "last2", new CarDescriptor.Entry("MAKE3", "MODEL4", 2014));

		CarPerson person3 = new CarPerson("first3", "last3", new CarDescriptor.Entry("MAKE2", "MODEL5", 2011));

		mongoTemplate.save(person1);
		mongoTemplate.save(person2);
		mongoTemplate.save(person3);

		TypedAggregation<CarPerson> agg = Aggregation.newAggregation(CarPerson.class,
				unwind("descriptors.carDescriptor.entries"), //
				project() //
						.and("descriptors.carDescriptor.entries.make").as("make") //
						.and("descriptors.carDescriptor.entries.model").as("model") //
						.and("firstName").as("firstName") //
						.and("lastName").as("lastName"), //
				group("make"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);

		assertThat(result.getMappedResults()).hasSize(3);
	}

	@Test // DATAMONGO-960
	void returnFiveMostCommonLikesAggregationFrameworkExampleWithSortOnDiskOptionEnabled() {

		createUserWithLikesDocuments();

		TypedAggregation<UserWithLikes> agg = createUsersWithCommonLikesAggregation() //
				.withOptions(newAggregationOptions().allowDiskUse(true).build());

		assertThat(agg).isNotNull();
		assertThat(agg.toString()).isNotNull();

		AggregationResults<LikeStats> result = mongoTemplate.aggregate(agg, LikeStats.class);
		assertThat(result).isNotNull();
		assertThat(result.getMappedResults()).isNotNull();
		assertThat(result.getMappedResults().size()).isEqualTo(5);

		assertLikeStats(result.getMappedResults().get(0), "a", 4);
		assertLikeStats(result.getMappedResults().get(1), "b", 2);
		assertLikeStats(result.getMappedResults().get(2), "c", 4);
		assertLikeStats(result.getMappedResults().get(3), "d", 2);
		assertLikeStats(result.getMappedResults().get(4), "e", 3);
	}

	@Test // DATAMONGO-1637
	void returnFiveMostCommonLikesAggregationFrameworkExampleWithSortOnDiskOptionEnabledWhileStreaming() {

		createUserWithLikesDocuments();

		TypedAggregation<UserWithLikes> agg = createUsersWithCommonLikesAggregation() //
				.withOptions(newAggregationOptions().allowDiskUse(true).build());

		assertThat(agg).isNotNull();
		assertThat(agg.toString()).isNotNull();

		try (Stream<LikeStats> stream = mongoTemplate.aggregateStream(agg, LikeStats.class)) {

			List<LikeStats> result = stream.toList();

			assertThat(result.size()).isEqualTo(5);

			assertLikeStats(result.get(0), "a", 4);
			assertLikeStats(result.get(1), "b", 2);
			assertLikeStats(result.get(2), "c", 4);
			assertLikeStats(result.get(3), "d", 2);
			assertLikeStats(result.get(4), "e", 3);
		}
	}

	@Test // DATAMONGO-960
	void returnFiveMostCommonLikesShouldReturnStageExecutionInformationWithExplainOptionEnabled() {

		createUserWithLikesDocuments();

		TypedAggregation<UserWithLikes> agg = createUsersWithCommonLikesAggregation() //
				.withOptions(newAggregationOptions().explain(true).build());

		AggregationResults<LikeStats> result = mongoTemplate.aggregate(agg, LikeStats.class);

		assertThat(result.getMappedResults()).isEmpty();

		Document rawResult = result.getRawResults();

		assertThat(rawResult).isNotNull();
		assertThat(rawResult.containsKey("stages")).isEqualTo(true);
	}

	@Test // DATAMONGO-954, DATAMONGO-2264
	void shouldSupportReturningCurrentAggregationRoot() {

		mongoTemplate.save(new Person("p1_first", "p1_last", 25));
		mongoTemplate.save(new Person("p2_first", "p2_last", 32));
		mongoTemplate.save(new Person("p3_first", "p3_last", 25));
		mongoTemplate.save(new Person("p4_first", "p4_last", 15));

		List<Document> personsWithAge25 = mongoTemplate.find(Query.query(where("age").is(25)), Document.class,
				mongoTemplate.getCollectionName(Person.class));

		Aggregation agg = newAggregation(group("age").push(Aggregation.ROOT).as("users"), sort(Sort.by("_id")));
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Person.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(3);
		Document o = result.getMappedResults().get(1);

		assertThat(o.get("_id")).isEqualTo((Object) 25);
		assertThat((List<?>) o.get("users")).hasSize(2);
		assertThat((List) o.get("users")).contains(personsWithAge25.toArray());
	}

	/**
	 * {@link https://stackoverflow.com/questions/24185987/using-root-inside-spring-data-mongodb-for-retrieving-whole-document}
	 */
	@Test // DATAMONGO-954, DATAMONGO-2264
	void shouldSupportReturningCurrentAggregationRootInReference() {

		mongoTemplate.save(new Reservation("0123", "42", 100));
		mongoTemplate.save(new Reservation("0360", "43", 200));
		mongoTemplate.save(new Reservation("0360", "44", 300));

		Aggregation agg = newAggregation( //
				match(where("hotelCode").is("0360")), //
				sort(Direction.DESC, "confirmationNumber", "timestamp"), //
				group("confirmationNumber") //
						.first("timestamp").as("timestamp") //
						.first(Aggregation.ROOT).as("reservationImage") //
		);
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Reservation.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(2);
	}

	@Test // DATAMONGO-1549
	void shouldApplyCountCorrectly() {

		mongoTemplate.save(new Reservation("0123", "42", 100));
		mongoTemplate.save(new Reservation("0360", "43", 200));
		mongoTemplate.save(new Reservation("0360", "44", 300));

		Aggregation agg = newAggregation( //
				count().as("documents"), //
				project("documents") //
						.andExpression("documents * 2").as("twice"));
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Reservation.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(1);

		Document document = result.getMappedResults().get(0);
		assertThat(document).containsEntry("documents", 3).containsEntry("twice", 6);
	}

	@Test // DATAMONGO-975
	void shouldRetrieveDateTimeFragementsCorrectly() throws Exception {

		mongoTemplate.dropCollection(ObjectWithDate.class);

		ZonedDateTime dateTime = ZonedDateTime.of(LocalDateTime.of(LocalDate.of(2014, 2, 7), LocalTime.of(3, 4, 5, 6)),
				ZoneId.of("UTC"));

		ObjectWithDate owd = new ObjectWithDate(Date.from(dateTime.toInstant()));
		mongoTemplate.insert(owd);

		ProjectionOperation dateProjection = Aggregation.project() //
				.and("dateValue").extractHour().as("hour") //
				.and("dateValue").extractMinute().as("min") //
				.and("dateValue").extractSecond().as("second") //
				.and("dateValue").extractMillisecond().as("millis") //
				.and("dateValue").extractYear().as("year") //
				.and("dateValue").extractMonth().as("month") //
				.and("dateValue").extractWeek().as("week") //
				.and("dateValue").extractDayOfYear().as("dayOfYear") //
				.and("dateValue").extractDayOfMonth().as("dayOfMonth") //
				.and("dateValue").extractDayOfWeek().as("dayOfWeek") //
				.andExpression("dateValue + 86400000").extractDayOfYear().as("dayOfYearPlus1Day") //
				.andExpression("dateValue + 86400000").project("dayOfYear").as("dayOfYearPlus1DayManually") //
		;

		Aggregation agg = newAggregation(dateProjection);
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, ObjectWithDate.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(1);
		Document document = result.getMappedResults().get(0);

		assertThat(document.get("hour")).isEqualTo((Object) dateTime.getHour());
		assertThat(document.get("min")).isEqualTo((Object) dateTime.getMinute());
		assertThat(document.get("second")).isEqualTo((Object) dateTime.getSecond());
		assertThat(document.get("millis")).isEqualTo((Object) dateTime.get(ChronoField.MILLI_OF_SECOND));
		assertThat(document.get("year")).isEqualTo((Object) dateTime.getYear());
		assertThat(document.get("month")).isEqualTo((Object) dateTime.getMonthValue());
		// dateTime.getWeekOfWeekyear()) returns 6 since for MongoDB the week starts on sunday and not on monday.
		assertThat(document.get("week")).isEqualTo((Object) 5);
		assertThat(document.get("dayOfYear")).isEqualTo((Object) dateTime.getDayOfYear());
		assertThat(document.get("dayOfMonth")).isEqualTo((Object) dateTime.getDayOfMonth());

		// dateTime.getDayOfWeek()
		assertThat(document.get("dayOfWeek")).isEqualTo((Object) 6);
		assertThat(document.get("dayOfYearPlus1Day")).isEqualTo((Object) dateTime.plusDays(1).getDayOfYear());
		assertThat(document.get("dayOfYearPlus1DayManually")).isEqualTo((Object) dateTime.plusDays(1).getDayOfYear());
	}

	@Test // DATAMONGO-1127
	void shouldSupportGeoNearQueriesForAggregationWithDistanceField() {

		mongoTemplate.insertAll(Arrays.asList(TestEntities.geolocation().pennStation(),
				TestEntities.geolocation().tenGenOffice(), TestEntities.geolocation().flatironBuilding()));

		mongoTemplate.indexOps(Venue.class).ensureIndex(new GeospatialIndex("location"));

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).limit(10).maxDistance(150);

		Aggregation agg = newAggregation(Aggregation.geoNear(geoNear, "distance"));
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Venue.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(3);

		Document firstResult = result.getMappedResults().get(0);
		assertThat(firstResult.containsKey("distance")).isEqualTo(true);
		assertThat((Double) firstResult.get("distance")).isCloseTo(117.620092203928, Offset.offset(0.00001D));
	}

	@Test // DATAMONGO-1348
	void shouldSupportGeoJsonInGeoNearQueriesForAggregationWithDistanceField() {

		mongoTemplate.insert(new Venue("Penn Station", -73.99408, 40.75057));
		mongoTemplate.insert(new Venue("10gen Office", -73.99171, 40.738868));
		mongoTemplate.insert(new Venue("Flatiron Building", -73.988135, 40.741404));

		mongoTemplate.indexOps(Venue.class)
				.ensureIndex(new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE));

		NearQuery geoNear = NearQuery.near(new GeoJsonPoint(-73, 40), Metrics.KILOMETERS).limit(10).maxDistance(150);

		Aggregation agg = newAggregation(Aggregation.geoNear(geoNear, "distance"));
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Venue.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(3);

		Document firstResult = result.getMappedResults().get(0);
		assertThat(firstResult.containsKey("distance")).isEqualTo(true);
		assertThat((Double) firstResult.get("distance")).isCloseTo(117.61940988193759, Offset.offset(0.00001D));
	}

	@Test // DATAMONGO-1348
	void shouldSupportGeoJsonInGeoNearQueriesForAggregationWithDistanceFieldInMiles() {

		mongoTemplate.insert(new Venue("Penn Station", -73.99408, 40.75057));
		mongoTemplate.insert(new Venue("10gen Office", -73.99171, 40.738868));
		mongoTemplate.insert(new Venue("Flatiron Building", -73.988135, 40.741404));

		mongoTemplate.indexOps(Venue.class)
				.ensureIndex(new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE));

		NearQuery geoNear = NearQuery.near(new GeoJsonPoint(-73, 40), Metrics.KILOMETERS).limit(10).maxDistance(150)
				.inMiles();

		Aggregation agg = newAggregation(Aggregation.geoNear(geoNear, "distance"));
		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Venue.class, Document.class);

		assertThat(result.getMappedResults()).hasSize(3);

		Document firstResult = result.getMappedResults().get(0);
		assertThat(firstResult.containsKey("distance")).isEqualTo(true);
		assertThat((Double) firstResult.get("distance")).isCloseTo(73.08517, Offset.offset(0.00001D));
	}

	@Test // DATAMONGO-1133
	void shouldHonorFieldAliasesForFieldReferences() {

		mongoTemplate.insert(new MeterData("m1", "counter1", 42));
		mongoTemplate.insert(new MeterData("m1", "counter1", 13));
		mongoTemplate.insert(new MeterData("m1", "counter1", 45));

		TypedAggregation<MeterData> agg = newAggregation(MeterData.class, //
				match(where("resourceId").is("m1")), //
				group("counterName").sum("counterVolume").as("totalValue"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, Document.class);

		assertThat(results.getMappedResults()).hasSize(1);
		Document result = results.getMappedResults().get(0);

		assertThat(result.get("_id")).isEqualTo("counter1");
		assertThat(result.get("totalValue")).isEqualTo(100.0);
	}

	@Test // DATAMONGO-1326
	void shouldLookupPeopleCorectly() {

		createUsersWithReferencedPersons();

		TypedAggregation<User> agg = newAggregation(User.class, //
				lookup("person", "_id", "firstname", "linkedPerson"), //
				sort(ASC, "id"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, User.class, Document.class);

		List<Document> mappedResults = results.getMappedResults();

		Document firstItem = mappedResults.get(0);

		assertThat(firstItem).containsEntry("_id", "u1");
		assertThat(firstItem).containsEntry("linkedPerson.[0].firstname", "u1");
	}

	@Test // GH-3322
	@EnableIfMongoServerVersion(isGreaterThanEqual = "5.0")
	void shouldLookupPeopleCorrectlyWithPipeline() {
		createUsersWithReferencedPersons();

		TypedAggregation<User> agg = newAggregation(User.class, //
				lookup().from("person").localField("_id").foreignField("firstname").pipeline(match(where("firstname").is("u1"))).as("linkedPerson"), //
				sort(ASC, "id"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, User.class, Document.class);

		List<Document> mappedResults = results.getMappedResults();

		Document firstItem = mappedResults.get(0);

		assertThat(firstItem).containsEntry("_id", "u1");
		assertThat(firstItem).containsEntry("linkedPerson.[0].firstname", "u1");
	}

	@Test // GH-3322
	@EnableIfMongoServerVersion(isGreaterThanEqual = "5.0")
	void shouldLookupPeopleCorrectlyWithPipelineAndLet() {
		createUsersWithReferencedPersons();

		TypedAggregation<User> agg = newAggregation(User.class, //
				lookup().from("person").localField("_id").foreignField("firstname").let(Let.ExpressionVariable.newVariable("the_id").forField("_id")).pipeline(
						match(ctx -> new Document("$expr", new Document("$eq", List.of("$$the_id", "u1"))))).as("linkedPerson"),
				sort(ASC, "id"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, User.class, Document.class);

		List<Document> mappedResults = results.getMappedResults();

		Document firstItem = mappedResults.get(0);

		assertThat(firstItem).containsEntry("_id", "u1");
		assertThat(firstItem).containsEntry("linkedPerson.[0].firstname", "u1");
	}

	@Test // DATAMONGO-1326
	void shouldGroupByAndLookupPeopleCorrectly() {

		createUsersWithReferencedPersons();

		TypedAggregation<User> agg = newAggregation(User.class, //
				group().min("id").as("foreignKey"), //
				lookup("person", "foreignKey", "firstname", "linkedPerson"), //
				sort(ASC, "foreignKey", "linkedPerson.firstname"));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, User.class, Document.class);

		List<Document> mappedResults = results.getMappedResults();

		Document firstItem = mappedResults.get(0);

		assertThat(firstItem).containsEntry("foreignKey", "u1");
		assertThat(firstItem).containsEntry("linkedPerson.[0].firstname", "u1");
	}

	@Test // DATAMONGO-1418, DATAMONGO-1824
	@MongoVersion(asOf = "2.6")
	void shouldCreateOutputCollection() {

		createPersonDocuments();

		String tempOutCollection = "personQueryTemp";
		TypedAggregation<Person> agg = newAggregation(Person.class, //
				group("sex").count().as("count"), //
				sort(DESC, "count"), //
				out(tempOutCollection));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, Document.class);

		assertThat(results.getMappedResults()).hasSize(2);

		List<Document> list = mongoTemplate.findAll(Document.class, tempOutCollection);

		assertThat(list).hasSize(2);
		assertThat(list.get(0)).containsEntry("_id", "MALE").containsEntry("count", 3);
		assertThat(list.get(1)).containsEntry("_id", "FEMALE").containsEntry("count", 2);

		mongoTemplate.dropCollection(tempOutCollection);
	}

	@Test // DATAMONGO-1637
	void shouldCreateOutputCollectionWhileStreaming() {

		createPersonDocuments();

		String tempOutCollection = "personQueryTemp";
		TypedAggregation<Person> agg = newAggregation(Person.class, //
				group("sex").count().as("count"), //
				sort(DESC, "count"), //
				out(tempOutCollection));

		mongoTemplate.aggregateStream(agg, Document.class).close();

		List<Document> list = mongoTemplate.findAll(Document.class, tempOutCollection);

		assertThat(list).hasSize(2);
		assertThat(list.get(0)).containsEntry("_id", "MALE").containsEntry("count", 3);
		assertThat(list.get(1)).containsEntry("_id", "FEMALE").containsEntry("count", 2);

		mongoTemplate.dropCollection(tempOutCollection);
	}

	@Test // DATAMONGO-1637
	void shouldReturnDocumentsWithOutputCollectionWhileStreaming() {

		createPersonDocuments();

		String tempOutCollection = "personQueryTemp";
		TypedAggregation<Person> agg = newAggregation(Person.class, //
				group("sex").count().as("count"), //
				sort(DESC, "count"), //
				out(tempOutCollection));

		try (Stream<Document> stream = mongoTemplate.aggregateStream(agg, Document.class)) {

			List<Document> result = stream.toList();

			assertThat(result).hasSize(2);
			assertThat(result.get(0)).containsEntry("_id", "MALE").containsEntry("count", 3);
			assertThat(result.get(1)).containsEntry("_id", "FEMALE").containsEntry("count", 2);
		}

		mongoTemplate.dropCollection(tempOutCollection);
	}

	private void createPersonDocuments() {

		mongoTemplate.save(new Person("Anna", "Ivanova", 21, Person.Sex.FEMALE));
		mongoTemplate.save(new Person("Pavel", "Sidorov", 36, Person.Sex.MALE));
		mongoTemplate.save(new Person("Anastasia", "Volochkova", 29, Person.Sex.FEMALE));
		mongoTemplate.save(new Person("Igor", "Stepanov", 31, Person.Sex.MALE));
		mongoTemplate.save(new Person("Leoniv", "Yakubov", 55, Person.Sex.MALE));
	}

	@Test // DATAMONGO-1418, DATAMONGO-2536
	void outShouldOutBeTheLastOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> newAggregation(match(new Criteria()), //
				group("field1").count().as("totalCount"), //
				out("collection1"), //
				skip(100L)).toPipeline(DEFAULT_CONTEXT));
	}

	@Test // DATAMONGO-1325
	void shouldApplySampleCorrectly() {

		createUserWithLikesDocuments();

		TypedAggregation<UserWithLikes> agg = newAggregation(UserWithLikes.class, //
				unwind("likes"), //
				sample(3) //
		);

		assertThat(agg.toString()).isNotNull();

		AggregationResults<LikeStats> result = mongoTemplate.aggregate(agg, LikeStats.class);
		assertThat(result.getMappedResults().size()).isEqualTo(3);
	}

	@Test // DATAMONGO-1457
	@MongoVersion(asOf = "3.2")
	void sliceShouldBeAppliedCorrectly() {

		createUserWithLikesDocuments();

		TypedAggregation<UserWithLikes> agg = newAggregation(UserWithLikes.class, match(new Criteria()),
				project().and("likes").slice(2));

		AggregationResults<UserWithLikes> result = mongoTemplate.aggregate(agg, UserWithLikes.class);

		assertThat(result.getMappedResults()).hasSize(9);
		for (UserWithLikes user : result) {
			assertThat(user.likes.size() <= 2).isEqualTo(true);
		}
	}

	@Test // DATAMONGO-1491
	void filterShouldBeAppliedCorrectly() {

		Item item43 = Item.builder().itemId("43").quantity(2).price(2L).build();
		Item item2 = Item.builder().itemId("2").quantity(1).price(240L).build();
		Sales sales1 = Sales.builder().id("0").items(Arrays.asList( //
				item43, item2)) //
				.build();

		Item item23 = Item.builder().itemId("23").quantity(3).price(110L).build();
		Item item103 = Item.builder().itemId("103").quantity(4).price(5L).build();
		Item item38 = Item.builder().itemId("38").quantity(1).price(300L).build();
		Sales sales2 = Sales.builder().id("1").items(Arrays.asList( //
				item23, item103, item38)).build();

		Item item4 = Item.builder().itemId("4").quantity(1).price(23L).build();
		Sales sales3 = Sales.builder().id("2").items(Arrays.asList( //
				item4)).build();

		mongoTemplate.insert(Arrays.asList(sales1, sales2, sales3), Sales.class);

		TypedAggregation<Sales> agg = newAggregation(Sales.class, project().and("items")
				.filter("item", ComparisonOperators.valueOf("item.price").greaterThanEqualToValue(100)).as("items"));

		assertThat(mongoTemplate.aggregate(agg, Sales.class).getMappedResults()).contains(
				Sales.builder().id("0").items(Collections.singletonList(item2)).build(),
				Sales.builder().id("1").items(Arrays.asList(item23, item38)).build(),
				Sales.builder().id("2").items(Collections.<Item> emptyList()).build());
	}

	@Test // DATAMONGO-1538
	void letShouldBeAppliedCorrectly() {

		Sales2 sales1 = Sales2.builder().id("1").price(10).tax(0.5F).applyDiscount(true).build();
		Sales2 sales2 = Sales2.builder().id("2").price(10).tax(0.25F).applyDiscount(false).build();

		mongoTemplate.insert(Arrays.asList(sales1, sales2), Sales2.class);

		ExpressionVariable total = ExpressionVariable.newVariable("total")
				.forExpression(ArithmeticOperators.valueOf("price").sum().and("tax"));
		ExpressionVariable discounted = ExpressionVariable.newVariable("discounted")
				.forExpression(ConditionalOperators.Cond.when("applyDiscount").then(0.9D).otherwise(1.0D));

		TypedAggregation<Sales2> agg = Aggregation.newAggregation(Sales2.class,
				Aggregation.project().and(VariableOperators.Let.define(total, discounted)
						.andApply(ArithmeticOperators.valueOf("total").multiplyBy("discounted"))).as("finalTotal"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);
		assertThat(result.getMappedResults()).contains(new Document("_id", "1").append("finalTotal", 9.450000000000001D),
				new Document("_id", "2").append("finalTotal", 10.25D));
	}

	@Test // DATAMONGO-1551, DATAMONGO-2264
	void graphLookupShouldBeAppliedCorrectly() {

		Employee em1 = Employee.builder().id(1).name("Dev").build();
		Employee em2 = Employee.builder().id(2).name("Eliot").reportsTo("Dev").build();
		Employee em4 = Employee.builder().id(4).name("Andrew").reportsTo("Eliot").build();

		mongoTemplate.insert(Arrays.asList(em1, em2, em4), Employee.class);

		TypedAggregation<Employee> agg = Aggregation.newAggregation(Employee.class,
				match(Criteria.where("name").is("Andrew")), //
				Aggregation.graphLookup("employee") //
						.startWith("reportsTo") //
						.connectFrom("reportsTo") //
						.connectTo("name") //
						.depthField("depth") //
						.maxDepth(5) //
						.as("reportingHierarchy"), //
				project("id", "depth", "name", "reportsTo", "reportingHierarchy"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, Document.class);

		Document object = result.getUniqueMappedResult();
		List<Object> list = (List<Object>) object.get("reportingHierarchy");

		assertThat(object).containsEntry("name", "Andrew").containsEntry("reportsTo", "Eliot");
		assertThat(list).containsOnly(
				new Document("_id", 2).append("name", "Eliot").append("reportsTo", "Dev").append("depth", 0L).append("_class",
						Employee.class.getName()),
				new Document("_id", 1).append("name", "Dev").append("depth", 1L).append("_class", Employee.class.getName()));
	}

	@Test // DATAMONGO-1552
	void bucketShouldCollectDocumentsIntoABucket() {

		Art a1 = Art.builder().id(1).title("The Pillars of Society").artist("Grosz").year(1926).price(199.99).build();
		Art a2 = Art.builder().id(2).title("Melancholy III").artist("Munch").year(1902).price(280.00).build();
		Art a3 = Art.builder().id(3).title("Dancer").artist("Miro").year(1925).price(76.04).build();
		Art a4 = Art.builder().id(4).title("The Great Wave off Kanagawa").artist("Hokusai").price(167.30).build();

		mongoTemplate.insert(Arrays.asList(a1, a2, a3, a4), Art.class);

		TypedAggregation<Art> aggregation = newAggregation(Art.class, //
				bucket("price") //
						.withBoundaries(0, 100, 200) //
						.withDefaultBucket("other") //
						.andOutputCount().as("count") //
						.andOutput("title").push().as("titles") //
						.andOutputExpression("price * 10").sum().as("sum"));

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(3);

		// { "_id" : 0 , "count" : 1 , "titles" : [ "Dancer"] , "sum" : 760.4000000000001}
		Document bound0 = result.getMappedResults().get(0);
		assertThat(bound0).containsEntry("count", 1).containsEntry("titles.[0]", "Dancer");
		assertThat((Double) bound0.get("sum")).isCloseTo(760.40, Offset.offset(0.1D));

		// { "_id" : 100 , "count" : 2 , "titles" : [ "The Pillars of Society" , "The Great Wave off Kanagawa"] , "sum" :
		// 3672.9}
		Document bound100 = result.getMappedResults().get(1);
		assertThat(bound100).containsEntry("count", 2).containsEntry("_id", 100);
		assertThat((List<String>) bound100.get("titles")).contains("The Pillars of Society", "The Great Wave off Kanagawa");
		assertThat((Double) bound100.get("sum")).isCloseTo(3672.9, Offset.offset(0.1D));
	}

	@Test // DATAMONGO-1552, DATAMONGO-2437
	void bucketAutoShouldCollectDocumentsIntoABucket() {

		Art a1 = Art.builder().id(1).title("The Pillars of Society").artist("Grosz").year(1926).price(199.99).build();
		Art a2 = Art.builder().id(2).title("Melancholy III").artist("Munch").year(1902).price(280.00).build();
		Art a3 = Art.builder().id(3).title("Dancer").artist("Miro").year(1925).price(76.04).build();
		Art a4 = Art.builder().id(4).title("The Great Wave off Kanagawa").artist("Hokusai").price(167.30).build();

		mongoTemplate.insert(Arrays.asList(a1, a2, a3, a4), Art.class);

		TypedAggregation<Art> aggregation = newAggregation(Art.class, //
				bucketAuto(ArithmeticOperators.Multiply.valueOf("price").multiplyBy(10), 3) //
						.withGranularity(Granularities.E12) //
						.andOutputCount().as("count") //
						.andOutput("title").push().as("titles") //
						.andOutputExpression("price * 10").sum().as("sum"));

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(3);

		// { "_id" : { "min" : 680.0 , "max" : 820.0 }, "count" : 1 , "titles" : [ "Dancer"] , "sum" : 760.4000000000001}
		Document bound0 = result.getMappedResults().get(0);
		assertThat(bound0).containsEntry("count", 1).containsEntry("titles.[0]", "Dancer").containsEntry("_id.min", 680.0)
				.containsKey("_id.max");

		// { "_id" : { "min" : 820.0 , "max" : 1800.0 }, "count" : 1 , "titles" : [ "The Great Wave off Kanagawa"] , "sum" :
		// 1673.0}
		Document bound1 = result.getMappedResults().get(1);
		assertThat(bound1).containsEntry("count", 1).containsEntry("_id.min", 820.0);
		assertThat((List<String>) bound1.get("titles")).contains("The Great Wave off Kanagawa");
		assertThat((Double) bound1.get("sum")).isCloseTo(1673.0, Offset.offset(0.1D));
	}

	@Test // DATAMONGO-1552
	void facetShouldCreateFacets() {

		Art a1 = Art.builder().id(1).title("The Pillars of Society").artist("Grosz").year(1926).price(199.99).build();
		Art a2 = Art.builder().id(2).title("Melancholy III").artist("Munch").year(1902).price(280.00).build();
		Art a3 = Art.builder().id(3).title("Dancer").artist("Miro").year(1925).price(76.04).build();
		Art a4 = Art.builder().id(4).title("The Great Wave off Kanagawa").artist("Hokusai").price(167.30).build();

		mongoTemplate.insert(Arrays.asList(a1, a2, a3, a4), Art.class);

		BucketAutoOperation bucketPrice = bucketAuto(ArithmeticOperators.Multiply.valueOf("price").multiplyBy(10), 3) //
				.withGranularity(Granularities.E12) //
				.andOutputCount().as("count") //
				.andOutput("title").push().as("titles") //
				.andOutputExpression("price * 10") //
				.sum().as("sum");

		TypedAggregation<Art> aggregation = newAggregation(Art.class, //
				project("title", "artist", "year", "price"), //
				facet(bucketPrice).as("categorizeByPrice") //
						.and(bucketAuto("year", 3)).as("categorizeByYear"));

		AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, Document.class);
		assertThat(result.getMappedResults().size()).isEqualTo(1);

		Document mappedResult = result.getUniqueMappedResult();

		// [ { "_id" : { "min" : 680.0 , "max" : 820.0} , "count" : 1 , "titles" : [ "Dancer"] , "sum" : 760.4000000000001}
		// ,
		// { "_id" : { "min" : 820.0 , "max" : 1800.0} , "count" : 1 , "titles" : [ "The Great Wave off Kanagawa"] , "sum" :
		// 1673.0} ,
		// { "_id" : { "min" : 1800.0 , "max" : 3300.0} , "count" : 2 , "titles" : [ "The Pillars of Society" , "Melancholy
		// III"] , "sum" : 4799.9}]
		List<Object> categorizeByPrice = (List<Object>) mappedResult.get("categorizeByPrice");
		assertThat(categorizeByPrice).hasSize(3);

		// [ { "_id" : { "min" : null , "max" : 1902} , "count" : 1} ,
		// { "_id" : { "min" : 1902-2018 , "max" : 1925} , "count" : 1} ,
		// { "_id" : { "min" : 1925-2018 , "max" : 1926} , "count" : 2}]
		List<Object> categorizeByYear = (List<Object>) mappedResult.get("categorizeByYear");
		assertThat(categorizeByYear).hasSize(3);
	}

	@Test // GH-4473
	@EnableIfMongoServerVersion(isGreaterThanEqual = "7.0")
	void percentileShouldBeAppliedCorrectly() {

		DATAMONGO788 objectToSave = new DATAMONGO788(62, 81, 80);
		DATAMONGO788 objectToSave2 = new DATAMONGO788(60, 83, 79);

		mongoTemplate.insert(objectToSave);
		mongoTemplate.insert(objectToSave2);

		Aggregation agg = Aggregation.newAggregation(
				project().and(ArithmeticOperators.valueOf("x").percentile(0.9, 0.4).and("y").and("xField"))
				.as("percentileValues"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, DATAMONGO788.class, Document.class);

		// MongoDB server returns $percentile as an array of doubles
		List<Document> rawResults = (List<Document>) result.getRawResults().get("results");
		assertThat((List<Object>) rawResults.get(0).get("percentileValues")).containsExactly(81.0, 80.0);
		assertThat((List<Object>) rawResults.get(1).get("percentileValues")).containsExactly(83.0, 79.0);
	}

	@Test // GH-4472
	@EnableIfMongoServerVersion(isGreaterThanEqual = "7.0")
	void medianShouldBeAppliedCorrectly() {

		DATAMONGO788 objectToSave = new DATAMONGO788(62, 81, 80);
		DATAMONGO788 objectToSave2 = new DATAMONGO788(60, 83, 79);

		mongoTemplate.insert(objectToSave);
		mongoTemplate.insert(objectToSave2);

		Aggregation agg = Aggregation.newAggregation(
				project().and(ArithmeticOperators.valueOf("x").median().and("y").and("xField"))
				.as("medianValue"));

		AggregationResults<Document> result = mongoTemplate.aggregate(agg, DATAMONGO788.class, Document.class);

		// MongoDB server returns $median a Double
		List<Document> rawResults = (List<Document>) result.getRawResults().get("results");
		assertThat(rawResults.get(0).get("medianValue")).isEqualTo(80.0);
		assertThat(rawResults.get(1).get("medianValue")).isEqualTo(79.0);
	}

	@Test // DATAMONGO-1986
	void runMatchOperationCriteriaThroughQueryMapperForTypedAggregation() {

		mongoTemplate.insertAll(TestEntities.geolocation().newYork());

		Aggregation aggregation = newAggregation(Venue.class,
				match(Criteria.where("location")
						.within(new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404)))),
				project("id", "location", "name"));

		AggregationResults<Document> groupResults = mongoTemplate.aggregate(aggregation, "newyork", Document.class);

		assertThat(groupResults.getMappedResults().size()).isEqualTo(4);
	}

	@Test // DATAMONGO-1986
	void runMatchOperationCriteriaThroughQueryMapperForUntypedAggregation() {

		mongoTemplate.insertAll(TestEntities.geolocation().newYork());

		Aggregation aggregation = newAggregation(
				match(Criteria.where("location")
						.within(new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404)))),
				project("id", "location", "name"));

		AggregationResults<Document> groupResults = mongoTemplate.aggregate(aggregation, "newyork", Document.class);

		assertThat(groupResults.getMappedResults().size()).isEqualTo(4);
	}

	@Test // DATAMONGO-2437
	void shouldReadComplexIdValueCorrectly() {

		WithComplexId source = new WithComplexId();
		source.id = new ComplexId();
		source.id.p1 = "v1";
		source.id.p2 = "v2";

		mongoTemplate.save(source);

		AggregationResults<WithComplexId> result = mongoTemplate.aggregate(newAggregation(project("id")),
				WithComplexId.class, WithComplexId.class);
		assertThat(result.getMappedResults()).containsOnly(source);
	}

	@Test // DATAMONGO-2536
	void skipOutputDoesNotReadBackAggregationResults() {

		createTagDocuments();

		Aggregation agg = newAggregation( //
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("n"), //
				project("n") //
						.and("tag").previousOperation(), //
				sort(DESC, "n") //
		).withOptions(AggregationOptions.builder().skipOutput().build());

		AggregationResults<TagCount> results = mongoTemplate.aggregate(agg, INPUT_COLLECTION, TagCount.class);

		assertThat(results.getMappedResults()).isEmpty();
		assertThat(results.getRawResults()).isEmpty();
	}

	@Test // DATAMONGO-2635
	void mapsEnumsInMatchClauseUsingInCriteriaCorrectly() {

		WithEnum source = new WithEnum();
		source.enumValue = MyEnum.TWO;
		source.id = "id-1";

		mongoTemplate.save(source);

		Aggregation agg = newAggregation(match(where("enumValue").in(Collections.singletonList(MyEnum.TWO))));

		AggregationResults<Document> results = mongoTemplate.aggregate(agg, mongoTemplate.getCollectionName(WithEnum.class),
				Document.class);
		assertThat(results.getMappedResults()).hasSize(1);
	}

	@Test // GH-4043
	void considersMongoIdWithinTypedCollections() {

		UserRef userRef = new UserRef();
		userRef.id = "4ee921aca44fd11b3254e001";
		userRef.name = "u-1";

		Widget widget = new Widget();
		widget.id = "w-1";
		widget.users = List.of(userRef);

		mongoTemplate.save(widget);

		Criteria criteria = Criteria.where("users").elemMatch(Criteria.where("id").is("4ee921aca44fd11b3254e001"));
		AggregationResults<Widget> aggregate = mongoTemplate.aggregate(newAggregation(match(criteria)), Widget.class, Widget.class);
		assertThat(aggregate.getMappedResults()).contains(widget);
	}

	@Test // GH-4443
	void shouldHonorFieldAliasesForFieldReferencesUsingFieldExposingOperation() {

		Item item1 = Item.builder().itemId("1").tags(Arrays.asList("a", "b")).build();
		Item item2 = Item.builder().itemId("1").tags(Arrays.asList("a", "c")).build();
		mongoTemplate.insert(Arrays.asList(item1, item2), Item.class);

		TypedAggregation<Item> aggregation = newAggregation(Item.class,
				match(where("itemId").is("1")),
				unwind("tags"),
				match(where("itemId").is("1").and("tags").is("c")));
		AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, Document.class);
		List<Document> mappedResults = results.getMappedResults();
		assertThat(mappedResults).hasSize(1);
		assertThat(mappedResults.get(0)).containsEntry("item_id", "1");
	}

	@Test // GH-4443
	void projectShouldResetContextToAvoidMappingFieldsAgainstANoLongerExistingTarget() {

		Item item1 = Item.builder().itemId("1").tags(Arrays.asList("a", "b")).build();
		Item item2 = Item.builder().itemId("1").tags(Arrays.asList("a", "c")).build();
		mongoTemplate.insert(Arrays.asList(item1, item2), Item.class);

		TypedAggregation<Item> aggregation = newAggregation(Item.class,
				match(where("itemId").is("1")),
				unwind("tags"),
				project().and("itemId").as("itemId").and("tags").as("tags"),
				match(where("itemId").is("1").and("tags").is("c")));

		AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, Document.class);
		List<Document> mappedResults = results.getMappedResults();
		assertThat(mappedResults).hasSize(1);
		assertThat(mappedResults.get(0)).containsEntry("itemId", "1");
	}

	private void createUsersWithReferencedPersons() {

		mongoTemplate.dropCollection(User.class);
		mongoTemplate.dropCollection(Person.class);

		User user1 = new User("u1");
		User user2 = new User("u2");
		User user3 = new User("u3");

		mongoTemplate.save(user1);
		mongoTemplate.save(user2);
		mongoTemplate.save(user3);

		Person person1 = new Person("u1", "User 1");
		Person person2 = new Person("u2", "User 2");

		mongoTemplate.save(person1);
		mongoTemplate.save(person2);
		mongoTemplate.save(user3);
	}

	private void assertLikeStats(LikeStats like, String id, long count) {

		assertThat(like).isNotNull();
		assertThat(like.id).isEqualTo(id);
		assertThat(like.count).isEqualTo(count);
	}

	private void createUserWithLikesDocuments() {
		mongoTemplate.insert(new UserWithLikes("u1", new Date(), "a", "b", "c"));
		mongoTemplate.insert(new UserWithLikes("u2", new Date(), "a"));
		mongoTemplate.insert(new UserWithLikes("u3", new Date(), "b", "c"));
		mongoTemplate.insert(new UserWithLikes("u4", new Date(), "c", "d", "e"));
		mongoTemplate.insert(new UserWithLikes("u5", new Date(), "a", "e", "c"));
		mongoTemplate.insert(new UserWithLikes("u6", new Date()));
		mongoTemplate.insert(new UserWithLikes("u7", new Date(), "a"));
		mongoTemplate.insert(new UserWithLikes("u8", new Date(), "x", "e"));
		mongoTemplate.insert(new UserWithLikes("u9", new Date(), "y", "d"));
	}

	private void createTagDocuments() {

		MongoCollection<Document> coll = mongoTemplate.getCollection(INPUT_COLLECTION);

		coll.insertOne(createDocument("Doc1", "spring", "mongodb", "nosql"));
		coll.insertOne(createDocument("Doc2", "spring", "mongodb"));
		coll.insertOne(createDocument("Doc3", "spring"));
	}

	private static Document createDocument(String title, String... tags) {

		Document doc = new Document("title", title);
		List<String> tagList = new ArrayList<String>();

		for (String tag : tags) {
			tagList.add(tag);
		}

		doc.put("tags", tagList);
		return doc;
	}

	private static void assertTagCount(String tag, int n, TagCount tagCount) {

		assertThat(tagCount.getTag()).isEqualTo(tag);
		assertThat(tagCount.getN()).isEqualTo(n);
	}

	static class DATAMONGO753 {

		PD[] pd;

		DATAMONGO753 withPDs(PD... pds) {
			this.pd = pds;
			return this;
		}
	}

	static class PD {

		String pDch;
		@org.springframework.data.mongodb.core.mapping.Field("alias") int up;

		PD(String pDch, int up) {
			this.pDch = pDch;
			this.up = up;
		}
	}

	static class DATAMONGO788 {

		int x;
		int y;
		int xField;
		int yField;

		public DATAMONGO788() {}

		DATAMONGO788(int x, int y) {
			this.x = x;
			this.xField = x;
			this.y = y;
			this.yField = y;
		}

		public DATAMONGO788(int x, int y, int xField) {
			this.x = x;
			this.y = y;
			this.xField = xField;
		}
	}

	// DATAMONGO-806
	static class User {

		@Id String id;
		List<PushMessage> msgs;

		public User() {}

		User(String id, PushMessage... msgs) {
			this.id = id;
			this.msgs = Arrays.asList(msgs);
		}
	}

	// DATAMONGO-806
	static class PushMessage {

		@Id String id;
		String content;
		Date createDate;

		public PushMessage() {}

		PushMessage(String id, String content, Instant createDate) {
			this(id, content, Date.from(createDate));
		}

		PushMessage(String id, String content, Date createDate) {
			this.id = id;
			this.content = content;
			this.createDate = createDate;
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document
	static class CarPerson {

		@Id private String id;
		private String firstName;
		private String lastName;
		private Descriptors descriptors;

		CarPerson(String firstname, String lastname, Entry... entries) {
			this.firstName = firstname;
			this.lastName = lastname;

			this.descriptors = new Descriptors();

			this.descriptors.carDescriptor = new CarDescriptor(entries);
		}
	}

	@SuppressWarnings("unused")
	static class Descriptors {

		private CarDescriptor carDescriptor;
	}

	static class CarDescriptor {

		private List<Entry> entries = new ArrayList<AggregationTests.CarDescriptor.Entry>();

		CarDescriptor(Entry... entries) {

			for (Entry entry : entries) {
				this.entries.add(entry);
			}
		}

		@SuppressWarnings("unused")
		static class Entry {

			private String make;
			private String model;
			private int year;

			public Entry() {}

			Entry(String make, String model, int year) {
				this.make = make;
				this.model = model;
				this.year = year;
			}
		}
	}

	static class Reservation {

		String hotelCode;
		String confirmationNumber;
		int timestamp;

		public Reservation() {}

		Reservation(String hotelCode, String confirmationNumber, int timestamp) {
			this.hotelCode = hotelCode;
			this.confirmationNumber = confirmationNumber;
			this.timestamp = timestamp;
		}
	}

	static class ObjectWithDate {

		Date dateValue;

		ObjectWithDate(Date dateValue) {
			this.dateValue = dateValue;
		}
	}

	// DATAMONGO-861
	@org.springframework.data.mongodb.core.mapping.Document(collection = "inventory")
	static class InventoryItem {

		int id;
		String item;
		String description;
		int qty;

		public InventoryItem() {}

		InventoryItem(int id, String item, int qty) {

			this.id = id;
			this.item = item;
			this.qty = qty;
		}

		InventoryItem(int id, String item, String description, int qty) {

			this.id = id;
			this.item = item;
			this.description = description;
			this.qty = qty;
		}
	}

	// DATAMONGO-1491
	static class Sales {

		@Id String id;
		List<Item> items;

		Sales(String id, List<Item> items) {
			this.id = id;
			this.items = items;
		}

		public static SalesBuilder builder() {
			return new SalesBuilder();
		}

		public String getId() {
			return this.id;
		}

		public List<Item> getItems() {
			return this.items;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setItems(List<Item> items) {
			this.items = items;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Sales sales = (Sales) o;
			return Objects.equals(id, sales.id) && Objects.equals(items, sales.items);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, items);
		}

		public String toString() {
			return "AggregationTests.Sales(id=" + this.getId() + ", items=" + this.getItems() + ")";
		}

		public static class SalesBuilder {

			private String id;
			private List<Item> items;

			SalesBuilder() {}

			public SalesBuilder id(String id) {
				this.id = id;
				return this;
			}

			public SalesBuilder items(List<Item> items) {
				this.items = items;
				return this;
			}

			public Sales build() {
				return new Sales(id, items);
			}

			public String toString() {
				return "AggregationTests.Sales.SalesBuilder(id=" + this.id + ", items=" + this.items + ")";
			}
		}
	}

	// DATAMONGO-1491, GH-4443
	static class Item {

		@org.springframework.data.mongodb.core.mapping.Field("item_id") //
		String itemId;
		Integer quantity;
		Long price;
		List<String> tags = new ArrayList<>();

		Item(String itemId, Integer quantity, Long price, List<String> tags) {

			this.itemId = itemId;
			this.quantity = quantity;
			this.price = price;
			this.tags = tags;
		}

		public static ItemBuilder builder() {
			return new ItemBuilder();
		}

		public String getItemId() {
			return this.itemId;
		}

		public Integer getQuantity() {
			return this.quantity;
		}

		public Long getPrice() {
			return this.price;
		}

		public void setItemId(String itemId) {
			this.itemId = itemId;
		}

		public void setQuantity(Integer quantity) {
			this.quantity = quantity;
		}

		public void setPrice(Long price) {
			this.price = price;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Item item = (Item) o;
			return Objects.equals(itemId, item.itemId) && Objects.equals(quantity, item.quantity)
					&& Objects.equals(price, item.price);
		}

		@Override
		public int hashCode() {
			return Objects.hash(itemId, quantity, price);
		}

		public String toString() {
			return "AggregationTests.Item(itemId=" + this.getItemId() + ", quantity=" + this.getQuantity() + ", price="
					+ this.getPrice() + ")";
		}

		public static class ItemBuilder {

			private String itemId;
			private Integer quantity;
			private Long price;
			private List<String> tags;

			ItemBuilder() {}

			public ItemBuilder itemId(String itemId) {
				this.itemId = itemId;
				return this;
			}

			public ItemBuilder quantity(Integer quantity) {
				this.quantity = quantity;
				return this;
			}

			public ItemBuilder price(Long price) {
				this.price = price;
				return this;
			}

			public ItemBuilder tags(List<String> tags) {
				this.tags = tags;
				return this;
			}

			public Item build() {
				return new Item(itemId, quantity, price, tags);
			}

			public String toString() {
				return "AggregationTests.Item.ItemBuilder(itemId=" + this.itemId + ", quantity=" + this.quantity + ", price="
						+ this.price + ")";
			}
		}
	}

	// DATAMONGO-1538
	static class Sales2 {

		String id;
		Integer price;
		Float tax;
		boolean applyDiscount;

		Sales2(String id, Integer price, Float tax, boolean applyDiscount) {

			this.id = id;
			this.price = price;
			this.tax = tax;
			this.applyDiscount = applyDiscount;
		}

		public static Sales2Builder builder() {
			return new Sales2Builder();
		}

		public String getId() {
			return this.id;
		}

		public Integer getPrice() {
			return this.price;
		}

		public Float getTax() {
			return this.tax;
		}

		public boolean isApplyDiscount() {
			return this.applyDiscount;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setPrice(Integer price) {
			this.price = price;
		}

		public void setTax(Float tax) {
			this.tax = tax;
		}

		public void setApplyDiscount(boolean applyDiscount) {
			this.applyDiscount = applyDiscount;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Sales2 sales2 = (Sales2) o;
			return applyDiscount == sales2.applyDiscount && Objects.equals(id, sales2.id)
					&& Objects.equals(price, sales2.price) && Objects.equals(tax, sales2.tax);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, price, tax, applyDiscount);
		}

		public String toString() {
			return "AggregationTests.Sales2(id=" + this.getId() + ", price=" + this.getPrice() + ", tax=" + this.getTax()
					+ ", applyDiscount=" + this.isApplyDiscount() + ")";
		}

		public static class Sales2Builder {

			private String id;
			private Integer price;
			private Float tax;
			private boolean applyDiscount;

			public Sales2Builder id(String id) {
				this.id = id;
				return this;
			}

			public Sales2Builder price(Integer price) {
				this.price = price;
				return this;
			}

			public Sales2Builder tax(Float tax) {
				this.tax = tax;
				return this;
			}

			public Sales2Builder applyDiscount(boolean applyDiscount) {
				this.applyDiscount = applyDiscount;
				return this;
			}

			public Sales2 build() {
				return new Sales2(id, price, tax, applyDiscount);
			}

			public String toString() {
				return "AggregationTests.Sales2.Sales2Builder(id=" + this.id + ", price=" + this.price + ", tax=" + this.tax
						+ ", applyDiscount=" + this.applyDiscount + ")";
			}
		}
	}

	// DATAMONGO-1551
	static class Employee {

		int id;
		String name;
		String reportsTo;

		Employee(int id, String name, String reportsTo) {

			this.id = id;
			this.name = name;
			this.reportsTo = reportsTo;
		}

		public static EmployeeBuilder builder() {
			return new EmployeeBuilder();
		}

		public int getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public String getReportsTo() {
			return this.reportsTo;
		}

		public void setId(int id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setReportsTo(String reportsTo) {
			this.reportsTo = reportsTo;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Employee employee = (Employee) o;
			return id == employee.id && Objects.equals(name, employee.name) && Objects.equals(reportsTo, employee.reportsTo);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, reportsTo);
		}

		public String toString() {
			return "AggregationTests.Employee(id=" + this.getId() + ", name=" + this.getName() + ", reportsTo="
					+ this.getReportsTo() + ")";
		}

		public static class EmployeeBuilder {

			private int id;
			private String name;
			private String reportsTo;

			public EmployeeBuilder id(int id) {
				this.id = id;
				return this;
			}

			public EmployeeBuilder name(String name) {
				this.name = name;
				return this;
			}

			public EmployeeBuilder reportsTo(String reportsTo) {
				this.reportsTo = reportsTo;
				return this;
			}

			public Employee build() {
				return new Employee(id, name, reportsTo);
			}

			public String toString() {
				return "AggregationTests.Employee.EmployeeBuilder(id=" + this.id + ", name=" + this.name + ", reportsTo="
						+ this.reportsTo + ")";
			}
		}
	}

	// DATAMONGO-1552
	static class Art {

		int id;
		String title;
		String artist;
		Integer year;
		double price;

		Art(int id, String title, String artist, Integer year, double price) {

			this.id = id;
			this.title = title;
			this.artist = artist;
			this.year = year;
			this.price = price;
		}

		public static ArtBuilder builder() {
			return new ArtBuilder();
		}

		public int getId() {
			return this.id;
		}

		public String getTitle() {
			return this.title;
		}

		public String getArtist() {
			return this.artist;
		}

		public Integer getYear() {
			return this.year;
		}

		public double getPrice() {
			return this.price;
		}

		public void setId(int id) {
			this.id = id;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setArtist(String artist) {
			this.artist = artist;
		}

		public void setYear(Integer year) {
			this.year = year;
		}

		public void setPrice(double price) {
			this.price = price;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Art art = (Art) o;
			return id == art.id && Double.compare(art.price, price) == 0 && Objects.equals(title, art.title)
					&& Objects.equals(artist, art.artist) && Objects.equals(year, art.year);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, title, artist, year, price);
		}

		public String toString() {
			return "AggregationTests.Art(id=" + this.getId() + ", title=" + this.getTitle() + ", artist=" + this.getArtist()
					+ ", year=" + this.getYear() + ", price=" + this.getPrice() + ")";
		}

		public static class ArtBuilder {

			private int id;
			private String title;
			private String artist;
			private Integer year;
			private double price;

			public ArtBuilder id(int id) {
				this.id = id;
				return this;
			}

			public ArtBuilder title(String title) {
				this.title = title;
				return this;
			}

			public ArtBuilder artist(String artist) {
				this.artist = artist;
				return this;
			}

			public ArtBuilder year(Integer year) {
				this.year = year;
				return this;
			}

			public ArtBuilder price(double price) {
				this.price = price;
				return this;
			}

			public Art build() {
				return new Art(id, title, artist, year, price);
			}

			public String toString() {
				return "AggregationTests.Art.ArtBuilder(id=" + this.id + ", title=" + this.title + ", artist=" + this.artist
						+ ", year=" + this.year + ", price=" + this.price + ")";
			}
		}
	}

	static class WithComplexId {
		@Id ComplexId id;

		public ComplexId getId() {
			return this.id;
		}

		public void setId(ComplexId id) {
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
			WithComplexId that = (WithComplexId) o;
			return Objects.equals(id, that.id);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id);
		}

		public String toString() {
			return "AggregationTests.WithComplexId(id=" + this.getId() + ")";
		}
	}

	static class ComplexId {

		String p1;
		String p2;

		public String getP1() {
			return this.p1;
		}

		public String getP2() {
			return this.p2;
		}

		public void setP1(String p1) {
			this.p1 = p1;
		}

		public void setP2(String p2) {
			this.p2 = p2;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ComplexId complexId = (ComplexId) o;
			return Objects.equals(p1, complexId.p1) && Objects.equals(p2, complexId.p2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(p1, p2);
		}

		public String toString() {
			return "AggregationTests.ComplexId(p1=" + this.getP1() + ", p2=" + this.getP2() + ")";
		}
	}

	static enum MyEnum {
		ONE, TWO
	}

	static class WithEnum {

		@Id String id;
		MyEnum enumValue;

		public WithEnum() {}

		public String getId() {
			return this.id;
		}

		public MyEnum getEnumValue() {
			return this.enumValue;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setEnumValue(MyEnum enumValue) {
			this.enumValue = enumValue;
		}

		public String toString() {
			return "AggregationTests.WithEnum(id=" + this.getId() + ", enumValue=" + this.getEnumValue() + ")";
		}
	}

	static class Widget {

		@Id String id;
		List<UserRef> users;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public List<UserRef> getUsers() {
			return users;
		}

		public void setUsers(List<UserRef> users) {
			this.users = users;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Widget widget = (Widget) o;

			if (!ObjectUtils.nullSafeEquals(id, widget.id)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(users, widget.users);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(id);
			result = 31 * result + ObjectUtils.nullSafeHashCode(users);
			return result;
		}
	}

	static class UserRef {

		@MongoId String id;
		String name;

		public UserRef() {}

		public String getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {

			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			UserRef userRef = (UserRef) o;
			return Objects.equals(id, userRef.id) && Objects.equals(name, userRef.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}

		public String toString() {
			return "AggregationTests.UserRef(id=" + this.getId() + ", name=" + this.getName() + ")";
		}
	}
}
