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

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.DbCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;
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

					DBCollection zipInfoCollection = db.createCollection(ZipInfo.class.getSimpleName(), null);
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
		mongoTemplate.aggregate(null, new AggregationPipeline(), TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingAggregationPipeline() {
		mongoTemplate.aggregate(INPUT_COLLECTION, null, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldHandleMissingEntityClass() {
		mongoTemplate.aggregate(INPUT_COLLECTION, new AggregationPipeline(), null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldDetectIllegalJsonInOperation() {

		AggregationPipeline pipeline = new AggregationPipeline().project("{ foo bar");
		mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);
	}

	@Test
	public void shouldAggregate() {

		createDocuments();

		AggregationPipeline pipeline = new AggregationPipeline(). //
				project("{_id:0,tags:1}}"). //
				unwind("tags"). //
				group("{_id:\"$tags\", n:{$sum:1}}"). //
				project("{tag: \"$_id\", n:1, _id:0}"). //
				sort(new Sort(new Sort.Order(Direction.DESC, "n")));

		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(3));

		assertTagCount("spring", 3, tagCount.get(0));
		assertTagCount("mongodb", 2, tagCount.get(1));
		assertTagCount("nosql", 1, tagCount.get(2));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectIllegalAggregationOperation() {

		createDocuments();
		AggregationPipeline pipeline = new AggregationPipeline().project("{$foobar:{_id:0,tags:1}}");
		mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);
	}

	@Test
	public void shouldAggregateEmptyCollection() {

		AggregationPipeline pipeline = new AggregationPipeline(). //
				project("{_id:0,tags:1}}"). //
				unwind("$tags").//
				group("{_id:\"$tags\", n:{$sum:1}}").//
				project("{tag: \"$_id\", n:1, _id:0}").//
				sort("{n:-1}");

		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(0));
	}

	@Test
	public void shouldDetectResultMismatch() {

		createDocuments();
		AggregationPipeline pipeline = new AggregationPipeline(). //
				project("{_id:0,tags:1}}"). //
				unwind("$tags"). //
				group("{_id:\"$tags\", count:{$sum:1}}"). //
				limit(2);

		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		assertThat(results, is(notNullValue()));
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();

		assertThat(tagCount, is(notNullValue()));
		assertThat(tagCount.size(), is(2));
		assertTagCount(null, 0, tagCount.get(0));
		assertTagCount(null, 0, tagCount.get(1));
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
		AggregationPipeline pipeline = new AggregationPipeline()
				. //
				group("{ \"_id\": { \"state\": \"$state\", \"city\": \"$city\"}, \"pop\": { \"$sum\": \"$pop\"}}")
				.//
				sort("{ \"pop\": 1}")
				. //
				group(
						"{\"_id\":\"$_id.state\", \"biggestCity\": { \"$last\": \"$_id.city\" }, \"biggestPop\": { $last: \"$pop\" }, \"smallestCity\": { $first: \"$_id.city\" }, \"smallestPop\": { \"$first\": \"$pop\" }}")
				.//
				project(
						"{ \"_id\": 0, \"state\": \"$_id\", \"biggestCity\":{ \"name\": \"$biggestCity\", \"pop\": \"$biggestPop\"}, \"smallestCity\":{ \"name\": \"$smallestCity\", \"pop\": \"$smallestPop\"}}");

		AggregationResults<ZipInfoStats> result = mongoTemplate.aggregate(ZipInfo.class.getSimpleName(), pipeline,
				ZipInfoStats.class);
		assertThat(result, is(notNullValue()));
		assertThat(result.getAggregationResult(), is(notNullValue()));
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

	/**
	 * Data model from mongodb reference data set
	 * 
	 * @see http://docs.mongodb.org/manual/tutorial/aggregation-examples/
	 * @see http://media.mongodb.org/zips.json
	 */
	static class ZipInfo {

		String id;
		String city;
		String state;
		@Field("pop") int population;
		@Field("loc") double[] location;

		public String toString() {
			return "ZipInfo [id=" + id + ", city=" + city + ", state=" + state + ", population=" + population + ", location="
					+ Arrays.toString(location) + "]";
		}
	}

	static class ZipInfoStats {

		String id;
		String state;
		City biggestCity;
		City smallestCity;

		public String toString() {
			return "ZipInfoStats [id=" + id + ", state=" + state + ", biggestCity=" + biggestCity + ", smallestCity="
					+ smallestCity + "]";
		}
	}

	static class City {

		String name;
		@Field("pop") int population;

		public String toString() {
			return "City [name=" + name + ", population=" + population + "]";
		}
	}
}
