/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.domain.Sort.Direction.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * Tests for {@link MongoTemplate#aggregate(String, AggregationPipeline, Class)}.
 * 
 * @see DATAMONGO-586
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class AggregationTests {

	private static final String INPUT_COLLECTION = "aggregation_test_collection";
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationTests.class);

	private static boolean initialized = false;

	@Autowired MongoTemplate mongoTemplate;

	@Before
	public void setUp() {

		cleanDb();
		initSampleDataIfNecessary();
	}

	@After
	public void cleanUp() {
		cleanDb();
	}

	private void cleanDb() {
		mongoTemplate.dropCollection(INPUT_COLLECTION);
		mongoTemplate.dropCollection(Product.class);
		mongoTemplate.dropCollection(UserWithLikes.class);
		mongoTemplate.dropCollection(DATAMONGO753.class);
		mongoTemplate.dropCollection(DATAMONGO788.class);
	}

	/**
	 * Imports the sample dataset (zips.json) if necessary (e.g. if it doen't exist yet). The dataset can originally be
	 * found on the mongodb aggregation framework example website:
	 * 
	 * @see http://docs.mongodb.org/manual/tutorial/aggregation-examples/.
	 */
	private void initSampleDataIfNecessary() {

		if (!initialized) {

			CommandResult result = mongoTemplate.executeCommand("{ buildInfo: 1 }");
			Object version = result.get("version");
			LOGGER.debug("Server uses MongoDB Version: {}", version);

			mongoTemplate.dropCollection(ZipInfo.class);
			mongoTemplate.execute(ZipInfo.class, new CollectionCallback<Void>() {

				@Override
				public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {

					Scanner scanner = null;
					try {
						scanner = new Scanner(new BufferedInputStream(new ClassPathResource("zips.json").getInputStream()));
						while (scanner.hasNextLine()) {
							String zipInfoRecord = scanner.nextLine();
							collection.save((DBObject) JSON.parse(zipInfoRecord));
						}
					} catch (Exception e) {
						if (scanner != null) {
							scanner.close();
						}
						throw new RuntimeException("Could not load mongodb sample dataset!", e);
					}

					return null;
				}
			});

			long count = mongoTemplate.count(new Query(), ZipInfo.class);
			assertThat(count, is(29467L));

			initialized = true;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingInputCollection() {
		mongoTemplate.aggregate(newAggregation(), (String) null, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingAggregationPipeline() {
		mongoTemplate.aggregate(null, INPUT_COLLECTION, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingEntityClass() {
		mongoTemplate.aggregate(newAggregation(), INPUT_COLLECTION, null);
	}

	@Test
	public void shouldAggregate() {

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

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(3));

		assertTagCount("spring", 3, tagCount.get(0));
		assertTagCount("mongodb", 2, tagCount.get(1));
		assertTagCount("nosql", 1, tagCount.get(2));
	}

	@Test
	public void shouldAggregateEmptyCollection() {

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

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(0));
	}

	@Test
	public void shouldDetectResultMismatch() {

		createTagDocuments();

		Aggregation aggregation = newAggregation( //
				project("tags"), //
				unwind("tags"), //
				group("tags") //
						.count().as("count"), // count field not present
				limit(2) //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(aggregation, INPUT_COLLECTION, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getMappedResults();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(2));
		assertTagCount(null, 0, tagCount.get(0));
		assertTagCount(null, 0, tagCount.get(1));
	}

	@Test
	public void complexAggregationFrameworkUsageLargestAndSmallestCitiesByState() {
		/*
		 //complex mongodb aggregation framework example from http://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state
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

		assertThat(aggregation, is(notNullValue()));
		assertThat(aggregation.toString(), is(notNullValue()));

		AggregationResults<ZipInfoStats> result = mongoTemplate.aggregate(aggregation, ZipInfoStats.class);
		assertThat(result, is(notNullValue()));
		assertThat(result.getMappedResults(), is(notNullValue()));
		assertThat(result.getMappedResults().size(), is(51));

		ZipInfoStats firstZipInfoStats = result.getMappedResults().get(0);
		assertThat(firstZipInfoStats, is(notNullValue()));
		assertThat(firstZipInfoStats.id, is(nullValue()));
		assertThat(firstZipInfoStats.state, is("AK"));
		assertThat(firstZipInfoStats.smallestCity, is(notNullValue()));
		assertThat(firstZipInfoStats.smallestCity.name, is("CHEVAK"));
		assertThat(firstZipInfoStats.smallestCity.population, is(0));
		assertThat(firstZipInfoStats.biggestCity, is(notNullValue()));
		assertThat(firstZipInfoStats.biggestCity.name, is("ANCHORAGE"));
		assertThat(firstZipInfoStats.biggestCity.population, is(183987));

		ZipInfoStats lastZipInfoStats = result.getMappedResults().get(50);
		assertThat(lastZipInfoStats, is(notNullValue()));
		assertThat(lastZipInfoStats.id, is(nullValue()));
		assertThat(lastZipInfoStats.state, is("WY"));
		assertThat(lastZipInfoStats.smallestCity, is(notNullValue()));
		assertThat(lastZipInfoStats.smallestCity.name, is("LOST SPRINGS"));
		assertThat(lastZipInfoStats.smallestCity.population, is(6));
		assertThat(lastZipInfoStats.biggestCity, is(notNullValue()));
		assertThat(lastZipInfoStats.biggestCity.name, is("CHEYENNE"));
		assertThat(lastZipInfoStats.biggestCity.population, is(70185));
	}

	@Test
	public void findStatesWithPopulationOver10MillionAggregationExample() {
		/*
		 //complex mongodb aggregation framework example from 
		 http://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state
		 
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

		assertThat(agg, is(notNullValue()));
		assertThat(agg.toString(), is(notNullValue()));

		AggregationResults<StateStats> result = mongoTemplate.aggregate(agg, StateStats.class);
		assertThat(result, is(notNullValue()));
		assertThat(result.getMappedResults(), is(notNullValue()));
		assertThat(result.getMappedResults().size(), is(7));

		StateStats stateStats = result.getMappedResults().get(0);
		assertThat(stateStats, is(notNullValue()));
		assertThat(stateStats.id, is("CA"));
		assertThat(stateStats.state, is(nullValue()));
		assertThat(stateStats.totalPopulation, is(29760021));
	}

	/**
	 * @see http://docs.mongodb.org/manual/tutorial/aggregation-examples/#return-the-five-most-common-likes
	 */
	@Test
	public void returnFiveMostCommonLikesAggregationFrameworkExample() {

		createUserWithLikesDocuments();

		/*
		 ...
		  $group: {
				      _id:"$like",
				      number:{ $sum:1}
		 			 }
		  ...
		 
		 */

		TypedAggregation<UserWithLikes> agg = newAggregation(UserWithLikes.class, //
				unwind("likes"), //
				group("likes").count().as("number"), //
				sort(DESC, "number"), //
				limit(5), //
				sort(ASC, previousOperation()) //
		);

		assertThat(agg, is(notNullValue()));
		assertThat(agg.toString(), is(notNullValue()));

		AggregationResults<LikeStats> result = mongoTemplate.aggregate(agg, LikeStats.class);
		assertThat(result, is(notNullValue()));
		assertThat(result.getMappedResults(), is(notNullValue()));
		assertThat(result.getMappedResults().size(), is(5));

		assertLikeStats(result.getMappedResults().get(0), "a", 4);
		assertLikeStats(result.getMappedResults().get(1), "b", 2);
		assertLikeStats(result.getMappedResults().get(2), "c", 4);
		assertLikeStats(result.getMappedResults().get(3), "d", 2);
		assertLikeStats(result.getMappedResults().get(4), "e", 3);
	}

	@Test
	public void arithmenticOperatorsInProjectionExample() {

		double taxRate = 0.19;
		double netPrice = 1.99;
		double discountRate = 0.05;
		int spaceUnits = 3;
		String productId = "P1";
		String productName = "A";
		mongoTemplate.insert(new Product(productId, productName, netPrice, spaceUnits, discountRate, taxRate));

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

		AggregationResults<DBObject> result = mongoTemplate.aggregate(agg, DBObject.class);
		List<DBObject> resultList = result.getMappedResults();

		assertThat(resultList, is(notNullValue()));
		assertThat((String) resultList.get(0).get("_id"), is(productId));
		assertThat((String) resultList.get(0).get("name"), is(productName));
		assertThat((Double) resultList.get(0).get("netPricePlus1"), is(netPrice + 1));
		assertThat((Double) resultList.get(0).get("netPriceMinus1"), is(netPrice - 1));
		assertThat((Double) resultList.get(0).get("netPriceMul2"), is(netPrice * 2));
		assertThat((Double) resultList.get(0).get("netPriceDiv119"), is(netPrice / 1.19));
		assertThat((Integer) resultList.get(0).get("spaceUnitsMod2"), is(spaceUnits % 2));
		assertThat((Integer) resultList.get(0).get("spaceUnitsPlusSpaceUnits"), is(spaceUnits + spaceUnits));
		assertThat((Integer) resultList.get(0).get("spaceUnitsMinusSpaceUnits"), is(spaceUnits - spaceUnits));
		assertThat((Integer) resultList.get(0).get("spaceUnitsMultiplySpaceUnits"), is(spaceUnits * spaceUnits));
		assertThat((Double) resultList.get(0).get("spaceUnitsDivideSpaceUnits"), is((double) (spaceUnits / spaceUnits)));
		assertThat((Integer) resultList.get(0).get("spaceUnitsModSpaceUnits"), is(spaceUnits % spaceUnits));
	}

	/**
	 * @see DATAMONGO-753
	 * @see http 
	 *      ://stackoverflow.com/questions/18653574/spring-data-mongodb-aggregation-framework-invalid-reference-in-group
	 *      -operati
	 */
	@Test
	public void allowsNestedFieldReferencesAsGroupIdsInGroupExpressions() {

		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("A", 1), new PD("B", 1), new PD("C", 1)));
		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("B", 1), new PD("B", 1), new PD("C", 1)));

		TypedAggregation<DATAMONGO753> agg = newAggregation(DATAMONGO753.class, //
				unwind("pd"), //
				group("pd.pDch") // the nested field expression
						.sum("pd.up").as("uplift"), //
				project("_id", "uplift"));

		AggregationResults<DBObject> result = mongoTemplate.aggregate(agg, DBObject.class);
		List<DBObject> stats = result.getMappedResults();

		assertThat(stats.size(), is(3));
		assertThat(stats.get(0).get("_id").toString(), is("C"));
		assertThat((Integer) stats.get(0).get("uplift"), is(2));
		assertThat(stats.get(1).get("_id").toString(), is("B"));
		assertThat((Integer) stats.get(1).get("uplift"), is(3));
		assertThat(stats.get(2).get("_id").toString(), is("A"));
		assertThat((Integer) stats.get(2).get("uplift"), is(1));
	}

	/**
	 * @see DATAMONGO-753
	 * @see http 
	 *      ://stackoverflow.com/questions/18653574/spring-data-mongodb-aggregation-framework-invalid-reference-in-group
	 *      -operati
	 */
	@Test
	public void aliasesNestedFieldInProjectionImmediately() {

		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("A", 1), new PD("B", 1), new PD("C", 1)));
		mongoTemplate.insert(new DATAMONGO753().withPDs(new PD("B", 1), new PD("B", 1), new PD("C", 1)));

		TypedAggregation<DATAMONGO753> agg = newAggregation(DATAMONGO753.class, //
				unwind("pd"), //
				project().and("pd.up").as("up"));

		AggregationResults<DBObject> results = mongoTemplate.aggregate(agg, DBObject.class);
		List<DBObject> mappedResults = results.getMappedResults();

		assertThat(mappedResults, hasSize(6));
		for (DBObject element : mappedResults) {
			assertThat(element.get("up"), is((Object) 1));
		}
	}

	/**
	 * @see DATAMONGO-788
	 */
	@Test
	public void referencesToGroupIdsShouldBeRenderedProperly() {

		mongoTemplate.insert(new DATAMONGO788(1, 1));
		mongoTemplate.insert(new DATAMONGO788(1, 1));
		mongoTemplate.insert(new DATAMONGO788(1, 1));
		mongoTemplate.insert(new DATAMONGO788(2, 1));
		mongoTemplate.insert(new DATAMONGO788(2, 1));

		AggregationOperation projectFirst = Aggregation.project("x", "y").and("xField").as("x").and("yField").as("y");
		AggregationOperation group = Aggregation.group("x", "y").count().as("xPerY");
		AggregationOperation project = Aggregation.project("xPerY", "x", "y").andExclude("_id");

		TypedAggregation<DATAMONGO788> aggregation = Aggregation.newAggregation(DATAMONGO788.class, projectFirst, group,
				project);
		AggregationResults<DBObject> aggResults = mongoTemplate.aggregate(aggregation, DBObject.class);
		List<DBObject> items = aggResults.getMappedResults();

		assertThat(items.size(), is(2));
		assertThat((Integer) items.get(0).get("xPerY"), is(2));
		assertThat((Integer) items.get(0).get("x"), is(2));
		assertThat((Integer) items.get(0).get("y"), is(1));
		assertThat((Integer) items.get(1).get("xPerY"), is(3));
		assertThat((Integer) items.get(1).get("x"), is(1));
		assertThat((Integer) items.get(1).get("y"), is(1));
	}

	private void assertLikeStats(LikeStats like, String id, long count) {

		assertThat(like, is(notNullValue()));
		assertThat(like.id, is(id));
		assertThat(like.count, is(count));
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

		DBCollection coll = mongoTemplate.getCollection(INPUT_COLLECTION);

		coll.insert(createDocument("Doc1", "spring", "mongodb", "nosql"));
		coll.insert(createDocument("Doc2", "spring", "mongodb"));
		coll.insert(createDocument("Doc3", "spring"));
	}

	private static DBObject createDocument(String title, String... tags) {

		DBObject doc = new BasicDBObject("title", title);
		List<String> tagList = new ArrayList<String>();

		for (String tag : tags) {
			tagList.add(tag);
		}

		doc.put("tags", tagList);
		return doc;
	}

	private static void assertTagCount(String tag, int n, TagCount tagCount) {

		assertThat(tagCount.getTag(), is(tag));
		assertThat(tagCount.getN(), is(n));
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

		public PD(String pDch, int up) {
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

		public DATAMONGO788(int x, int y) {
			this.x = x;
			this.xField = x;
			this.y = y;
			this.yField = y;
		}
	}

}
