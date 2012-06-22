/*
 * Copyright 2011 the original author or authors.
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
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class GroupByTests {

	@Autowired
	MongoDbFactory factory;

	@Autowired
	ApplicationContext applicationContext;

	MongoTemplate mongoTemplate;

	@Autowired
	@SuppressWarnings("unchecked")
	public void setMongo(Mongo mongo) throws Exception {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(XObject.class)));
		mappingContext.initialize();

		MappingMongoConverter mappingConverter = new MappingMongoConverter(factory, mappingContext);
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
		// String expected =
		// "{ \"group\" : { \"ns\" : \"test\" , \"key\" : { \"a\" : 1} , \"cond\" :  null  , \"$reduce\" :  null  , \"initial\" :  null }}";
		String expected = "{ \"key\" : { \"a\" : 1} , \"$reduce\" :  null  , \"initial\" :  null }";
		Assert.assertEquals(expected, gc.toString());
	}

	@Test
	public void multipleKeyCreation() {
		DBObject gc = GroupBy.key("a", "b").getGroupByObject();
		// String expected =
		// "{ \"group\" : { \"ns\" : \"test\" , \"key\" : { \"a\" : 1 , \"b\" : 1} , \"cond\" :  null  , \"$reduce\" :  null  , \"initial\" :  null }}";
		String expected = "{ \"key\" : { \"a\" : 1 , \"b\" : 1} , \"$reduce\" :  null  , \"initial\" :  null }";
		Assert.assertEquals(expected, gc.toString());
	}

	@Test
	public void keyFunctionCreation() {
		DBObject gc = GroupBy.keyFunction("classpath:keyFunction.js").getGroupByObject();
		String expected = "{ \"$keyf\" : \"classpath:keyFunction.js\" , \"$reduce\" :  null  , \"initial\" :  null }";
		Assert.assertEquals(expected, gc.toString());
	}

	@Test
	public void SimpleGroup() {
		createGroupByData();
		GroupByResults<XObject> results;

		results = mongoTemplate.group(
				"group_test_collection",
				GroupBy.key("x").initialDocument(new BasicDBObject("count", 0))
						.reduceFunction("function(doc, prev) { prev.count += 1 }"), XObject.class);

		assertMapReduceResults(results);

	}

	@Test
	public void SimpleGroupWithKeyFunction() {
		createGroupByData();
		GroupByResults<XObject> results;

		results = mongoTemplate.group(
				"group_test_collection",
				GroupBy.keyFunction("function(doc) { return { x : doc.x }; }").initialDocument("{ count: 0 }")
						.reduceFunction("function(doc, prev) { prev.count += 1 }"), XObject.class);

		assertMapReduceResults(results);
	}

	@Test
	public void SimpleGroupWithFunctionsAsResources() {
		createGroupByData();
		GroupByResults<XObject> results;

		results = mongoTemplate.group("group_test_collection", GroupBy.keyFunction("classpath:keyFunction.js")
				.initialDocument("{ count: 0 }").reduceFunction("classpath:groupReduce.js"), XObject.class);

		assertMapReduceResults(results);
	}

	@Test
	public void SimpleGroupWithQueryAndFunctionsAsResources() {
		createGroupByData();
		GroupByResults<XObject> results;

		results = mongoTemplate.group(where("x").gt(0), "group_test_collection", keyFunction("classpath:keyFunction.js")
				.initialDocument("{ count: 0 }").reduceFunction("classpath:groupReduce.js"), XObject.class);

		assertMapReduceResults(results);
	}

	private void assertMapReduceResults(GroupByResults<XObject> results) {
		DBObject dboRawResults = results.getRawResults();
		String expected = "{ \"serverUsed\" : \"127.0.0.1:27017\" , \"retval\" : [ { \"x\" : 1.0 , \"count\" : 2.0} , { \"x\" : 2.0 , \"count\" : 1.0} , { \"x\" : 3.0 , \"count\" : 3.0}] , \"count\" : 6.0 , \"keys\" : 3 , \"ok\" : 1.0}";
		Assert.assertEquals(expected, dboRawResults.toString());

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
		Assert.assertEquals(3, numResults);
		Assert.assertEquals(6, results.getCount(), 0.001);
		Assert.assertEquals(3, results.getKeys());
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
