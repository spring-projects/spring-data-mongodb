/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.mapreduce.GroupBy.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * Integration tests for group-by operations.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class GroupByTests {

	@Autowired MongoDbFactory factory;
	@Autowired ApplicationContext applicationContext;

	MongoTemplate mongoTemplate;

	@Autowired
	@SuppressWarnings("unchecked")
	public void setMongo(Mongo mongo) throws Exception {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(XObject.class)));
		mappingContext.initialize();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MappingMongoConverter mappingConverter = new MappingMongoConverter(dbRefResolver, mappingContext);
		mappingConverter.afterPropertiesSet();

		this.mongoTemplate = new MongoTemplate(factory, mappingConverter);
		mongoTemplate.setApplicationContext(applicationContext);
	}

	@Before
	public void setUp() {
		cleanDb();
	}

	@After
	public void cleanUp() {
		cleanDb();
	}

	protected void cleanDb() {
		mongoTemplate.dropCollection(mongoTemplate.getCollectionName(XObject.class));
		mongoTemplate.dropCollection("group_test_collection");
	}

	@Test
	public void singleKeyCreation() {

		DBObject gc = new GroupBy("a").getGroupByObject();

		assertThat(gc.toString(), is("{ \"key\" : { \"a\" : 1} , \"$reduce\" :  null  , \"initial\" :  null }"));
	}

	@Test
	public void multipleKeyCreation() {

		DBObject gc = GroupBy.key("a", "b").getGroupByObject();

		assertThat(gc.toString(), is("{ \"key\" : { \"a\" : 1 , \"b\" : 1} , \"$reduce\" :  null  , \"initial\" :  null }"));
	}

	@Test
	public void keyFunctionCreation() {

		DBObject gc = GroupBy.keyFunction("classpath:keyFunction.js").getGroupByObject();

		assertThat(gc.toString(),
				is("{ \"$keyf\" : \"classpath:keyFunction.js\" , \"$reduce\" :  null  , \"initial\" :  null }"));
	}

	@Test
	public void SimpleGroup() {

		createGroupByData();
		GroupByResults<XObject> results = mongoTemplate.group(
				"group_test_collection",
				GroupBy.key("x").initialDocument(new BasicDBObject("count", 0))
						.reduceFunction("function(doc, prev) { prev.count += 1 }"), XObject.class);

		assertMapReduceResults(results);
	}

	@Test
	public void SimpleGroupWithKeyFunction() {

		createGroupByData();
		GroupByResults<XObject> results = mongoTemplate.group(
				"group_test_collection",
				GroupBy.keyFunction("function(doc) { return { x : doc.x }; }").initialDocument("{ count: 0 }")
						.reduceFunction("function(doc, prev) { prev.count += 1 }"), XObject.class);

		assertMapReduceResults(results);
	}

	@Test
	public void SimpleGroupWithFunctionsAsResources() {

		createGroupByData();
		GroupByResults<XObject> results = mongoTemplate.group(
				"group_test_collection",
				GroupBy.keyFunction("classpath:keyFunction.js").initialDocument("{ count: 0 }")
						.reduceFunction("classpath:groupReduce.js"), XObject.class);

		assertMapReduceResults(results);
	}

	@Test
	public void SimpleGroupWithQueryAndFunctionsAsResources() {

		createGroupByData();
		GroupByResults<XObject> results = mongoTemplate.group(
				where("x").gt(0),
				"group_test_collection",
				keyFunction("classpath:keyFunction.js").initialDocument("{ count: 0 }").reduceFunction(
						"classpath:groupReduce.js"), XObject.class);

		assertMapReduceResults(results);
	}

	private void assertMapReduceResults(GroupByResults<XObject> results) {

		DBObject dboRawResults = results.getRawResults();

		assertThat(dboRawResults.containsField("serverUsed"), is(true));
		assertThat(dboRawResults.get("serverUsed").toString(), endsWith("127.0.0.1:27017"));

		int numResults = 0;
		for (XObject xObject : results) {
			if (xObject.getX() == 1) {
				Assert.assertEquals(2, xObject.getCount(), 0.001);
			}
			if (xObject.getX() == 2) {
				Assert.assertEquals(1, xObject.getCount(), 0.001);
			}
			if (xObject.getX() == 3) {
				Assert.assertEquals(3, xObject.getCount(), 0.001);
			}
			numResults++;
		}
		assertThat(numResults, is(3));
		assertThat(results.getKeys(), is(3));
		assertEquals(6, results.getCount(), 0.001);
	}

	private void createGroupByData() {

		DBCollection c = mongoTemplate.getDb().getCollection("group_test_collection");

		c.save(new BasicDBObject("x", 1));
		c.save(new BasicDBObject("x", 1));
		c.save(new BasicDBObject("x", 2));
		c.save(new BasicDBObject("x", 3));
		c.save(new BasicDBObject("x", 3));
		c.save(new BasicDBObject("x", 3));
	}
}
