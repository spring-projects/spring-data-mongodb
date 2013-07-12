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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.domain.Sort.Direction.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.DSL.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.data.mongodb.core.DbCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
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
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class AggregationTests {

	private static final String INPUT_COLLECTION = "aggregation_test_collection";

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
	}

	private void initSampleDataIfNecessary() {

		if (!mongoTemplate.collectionExists(ZipInfo.class)) {
			mongoTemplate.dropCollection(ZipInfo.class);
			mongoTemplate.execute(new DbCallback<Void>() {
				@Override
				public Void doInDB(DB db) throws MongoException, DataAccessException {

					DBCollection zipInfoCollection = db.createCollection(
							MongoCollectionUtils.getPreferredCollectionName(ZipInfo.class), null);
					Scanner scanner = null;
					try {
						scanner = new Scanner(new BufferedInputStream(new ClassPathResource("zips.json").getInputStream()));
						while (scanner.hasNextLine()) {
							String zipInfoRecord = scanner.nextLine();
							zipInfoCollection.save((DBObject) JSON.parse(zipInfoRecord));
						}
					} catch (Exception e) {
						// ignore
						scanner.close();
					}

					return null;
				}
			});
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingInputCollection() {
		mongoTemplate.aggregate((String) null, new Aggregation<Object, TagCount>(), TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingAggregationPipeline() {
		mongoTemplate.aggregate(INPUT_COLLECTION, null, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingEntityClass() {
		mongoTemplate.aggregate(INPUT_COLLECTION, new Aggregation<Object, TagCount>(), null);
	}

	@Test
	public void shouldAggregate() {

		createDocuments();

		Aggregation<Object, TagCount> agg = newAggregation(INPUT_COLLECTION, //
				project("tags"), //
				unwind("tags"), //
				group($("tags")).count("n"), //
				project().field("tag", $id()).field("n", 1), //
				sort(DESC, "n") //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(agg, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(3));

		assertTagCount("spring", 3, tagCount.get(0));
		assertTagCount("mongodb", 2, tagCount.get(1));
		assertTagCount("nosql", 1, tagCount.get(2));
	}

	@Test
	public void shouldAggregateEmptyCollection() {

		Aggregation<Object, TagCount> agg = newAggregation(INPUT_COLLECTION, //
				project("tags"), //
				unwind("tags"), //
				group($("tags")).count("n"), //
				project().field("tag", $id()).field("n", 1), //
				sort(DESC, "n") //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, agg, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(0));
	}

	@Test
	public void shouldDetectResultMismatch() {

		createDocuments();
		Aggregation<Object, TagCount> agg = newAggregation(INPUT_COLLECTION, //
				project("tags"), //
				unwind("tags"), //
				group($("tags")).count("count"), //
				limit(2) //
		);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(agg, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(2));
		assertTagCount(null, 0, tagCount.get(0));
		assertTagCount(null, 0, tagCount.get(1));
	}

	@Test
	public void fieldsFactoryMethod() {

		Fields fields = fields("a", "b").and("c").and("d", 42);
		assertThat(fields, is(notNullValue()));
		assertThat(fields.getValues(), is(notNullValue()));
		assertThat(fields.getValues().size(), is(4));
		assertThat(fields.getValues().get("a"), is((Object) "$a"));
		assertThat(fields.getValues().get("b"), is((Object) "$b"));
		assertThat(fields.getValues().get("c"), is((Object) "$c"));
		assertThat(fields.getValues().get("d"), is((Object) 42));
	}

	@Test
	public void groupFactoryMethodWithFieldsAndSumOperation() {

		Fields fields = fields("a", "b").and("c").and("d", 42);
		GroupOperation groupOperation = group(fields).sum("e");

		assertThat(groupOperation, is(notNullValue()));
		assertThat(groupOperation.toDbObject(), is(notNullValue()));
		assertThat(groupOperation.id, is(notNullValue()));
		assertThat(groupOperation.id, is((Object) fields.getValues()));
		assertThat(groupOperation.fields, is(notNullValue()));
		assertThat(groupOperation.fields.size(), is(1));
		assertThat(groupOperation.fields.containsKey("e"), is(true));
		assertThat(groupOperation.fields.get("e"), is(notNullValue()));
		assertThat(groupOperation.fields.get("e").get("$sum"), is(notNullValue()));
		assertThat(groupOperation.fields.get("e").get("$sum"), is((Object) "$e"));
	}

	@Test
	public void complexAggregationFrameworkUsageLargestAndSmallestCitiesByState() {
		/*
		 //complex mongodb aggregation framework example from http://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state
		db.zipcodes.aggregate( 
		 { 
			$group:	{ 
				_id: { state: "$state", city: "$city"},
		  	pop: { $sum: "$pop" } 
		   } 
		 },
		 { 
		 	$sort: { pop: 1 } 
		 },
		{ 
			$group: { 
				_id: "$_id.state",
		    biggestCity:  { $last: "$_id.city" },
		    biggestPop:   { $last: "$pop" },
		    smallestCity: { $first: "$_id.city" },
		    smallestPop:  { $first: "$pop" } 
		   } 
		 },
			// the following $project is optional, and
			// modifies the output format.
		 { 
			 $project: {
				 _id: 0,
		  	 state: "$_id",
		  	 biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" },
		  	 smallestCity: { name: "$smallestCity", pop: "$smallestPop" } 
		  	} 
		  } 
		)
		  */

		Aggregation<ZipInfo, ZipInfoStats> agg = newAggregation(ZipInfo.class, //
				group(fields("state", "city")).sum("pop"), // fields("state", "city") -> state: $state, city: $city
				sort(ASC, "pop"), //
				group($id("state")) // $id("state") -> _id : $_id.state
						.last("biggestCity", $id("city")) //
						.last("biggestPop", $("pop")) //
						.first("smallestCity", $id("city")) //
						.first("smallestPop", $("pop")), //
				project(ZipInfoStats.class) //
						.field("_id", 0) //
						.field("state", $id()) // $id() -> $_id
						.field("biggestCity", fields().and("name", $("biggestCity")).and("population", $("biggestPop"))) //
						.field("smallestCity", fields().and("name", $("smallestCity")).and("population", $("smallestPop"))) //
		);

		assertThat(agg, is(notNullValue()));
		assertThat(agg.toString(), is(notNullValue()));

		AggregationResults<ZipInfoStats> result = mongoTemplate.aggregate(agg, ZipInfoStats.class);
		assertThat(result, is(notNullValue()));
		assertThat(result.getAggregationResult(), is(notNullValue()));
		assertThat(result.getAggregationResult().size(), is(51));

		ZipInfoStats firstZipInfoStats = result.getAggregationResult().get(0);
		assertThat(firstZipInfoStats, is(notNullValue()));
		assertThat(firstZipInfoStats.id, is(nullValue()));
		assertThat(firstZipInfoStats.state, is("RI"));
		assertThat(firstZipInfoStats.smallestCity, is(notNullValue()));
		assertThat(firstZipInfoStats.smallestCity.name, is("CLAYVILLE"));
		assertThat(firstZipInfoStats.smallestCity.population, is(45));
		assertThat(firstZipInfoStats.biggestCity, is(notNullValue()));
		assertThat(firstZipInfoStats.biggestCity.name, is("CRANSTON"));
		assertThat(firstZipInfoStats.biggestCity.population, is(176404));
	}

	@Test
	public void findStatesWithPopulationOver10MillionAggregationExample() {
		/*
		 //complex mongodb aggregation framework example from http://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state
		 db.zipcodes.aggregate( 
			 	{
				   $group:{
				      _id:"$state",
				      totalPop:{ $sum:"$pop"}
		 			 }
				},
				{
				   $match:{
				      totalPop: { $gte:10*1000*1000 }
				   }
				}
		)
		  */

		Aggregation<ZipInfo, StateStats> agg = newAggregation(ZipInfo.class, //
				group($("state")).sum("totalPop", $("pop")), // fields("state", "city") -> state: $state, city: $city
				match(where("totalPop").gte(10 * 1000 * 1000)) //
		);

		assertThat(agg, is(notNullValue()));
		assertThat(agg.toString(), is(notNullValue()));

		AggregationResults<StateStats> result = mongoTemplate.aggregate(agg, StateStats.class);
		assertThat(result, is(notNullValue()));
		assertThat(result.getAggregationResult(), is(notNullValue()));
		assertThat(result.getAggregationResult().size(), is(7));

		StateStats stateStats = result.getAggregationResult().get(0);
		assertThat(stateStats, is(notNullValue()));
		assertThat(stateStats.id, is("PA"));
		assertThat(stateStats.state, is(nullValue()));
	}

	private void createDocuments() {

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

}
