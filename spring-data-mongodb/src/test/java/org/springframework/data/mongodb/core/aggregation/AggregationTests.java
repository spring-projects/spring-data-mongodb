/*
 * Copyright 2011-2012 the original author or authors.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Tests for {@link MongoTemplate#aggregate(String, AggregationPipeline, Class)}.
 * 
 * @author Tobias Trelle
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class AggregationTests {

	private static final String INPUT_COLLECTION = "aggregation_test_collection";

	@Autowired
	MongoTemplate mongoTemplate;

	@Before
	public void setUp() {
		cleanDb();
	}

	@After
	public void cleanUp() {
		cleanDb();
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
		// given
		AggregationPipeline pipeline = new AggregationPipeline().project("{ foo bar");

		// when
		mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		// then: throw expected exception
	}

	@Test
	public void shouldAggregate() {
		// given
		createDocuments();
		AggregationPipeline pipeline = new AggregationPipeline()
			.project("{_id:0,tags:1}}")
			.unwind("$tags")
			.group("{_id:\"$tags\", n:{$sum:1}}")
			.project("{tag: \"$_id\", n:1, _id:0}")
			.sort("{n:-1}");

		// when
		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		// then
		assertThat(results, notNullValue());
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();
		assertThat(tagCount, notNullValue());
		assertThat(tagCount.size(), is(3));
		assertTagCount("spring", 3, tagCount.get(0));
		assertTagCount("mongodb", 2, tagCount.get(1));
		assertTagCount("nosql", 1, tagCount.get(2));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectIllegalAggregationOperation() {
		// given
		createDocuments();
		AggregationPipeline pipeline = new AggregationPipeline().project("{$foobar:{_id:0,tags:1}}");

		// when
		mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		// then: throw expected exception
	}

	@Test
	public void shouldAggregateEmptyCollection() {
		// given
		AggregationPipeline pipeline = new AggregationPipeline()
		.project("{_id:0,tags:1}}")
		.unwind("$tags")
		.group("{_id:\"$tags\", n:{$sum:1}}")
		.project("{tag: \"$_id\", n:1, _id:0}")
		.sort("{n:-1}");
		
		// when
		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		// then
		assertThat(results, notNullValue());
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();
		assertThat(tagCount, notNullValue());
		assertThat(tagCount.size(), is(0));
	}

	@Test
	public void shouldDetectResultMismatch() {
		// given
		createDocuments();	
		AggregationPipeline pipeline = new AggregationPipeline()
		.project("{_id:0,tags:1}}")
		.unwind("$tags")
		.group("{_id:\"$tags\", count:{$sum:1}}")
		.limit(2);
		
		// when
		AggregationResults<TagCount> results = mongoTemplate.aggregate(INPUT_COLLECTION, pipeline, TagCount.class);

		// then
		assertThat(results, notNullValue());
		assertThat(results.getServerUsed(), is("/127.0.0.1:27017"));

		List<TagCount> tagCount = results.getAggregationResult();
		assertThat(tagCount, notNullValue());
		assertThat(tagCount.size(), is(2));
		assertTagCount(null, 0, tagCount.get(0));
		assertTagCount(null, 0, tagCount.get(1));
	}
	
	protected void cleanDb() {
		mongoTemplate.dropCollection(INPUT_COLLECTION);
	}

	private void createDocuments() {
		DBCollection coll = mongoTemplate.getCollection(INPUT_COLLECTION);
		coll.insert(createDocument("Doc1", "spring", "mongodb", "nosql"));
		coll.insert(createDocument("Doc2", "spring", "mongodb"));
		coll.insert(createDocument("Doc3", "spring"));
	}

	private DBObject createDocument(String title, String... tags) {
		DBObject doc = new BasicDBObject("title", title);
		List<String> tagList = new ArrayList<String>();
		for (String tag : tags) {
			tagList.add(tag);
		}
		doc.put("tags", tagList);

		return doc;
	}

	private void assertTagCount(String tag, int n, TagCount tagCount) {
		assertThat(tagCount.getTag(), is(tag));
		assertThat(tagCount.getN(), is(n));
	}

}
