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

import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.mapreduce.MapReduceOptions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

/**
 * Integration test for {@link MongoTemplate}'s Map-Reduce operations
 * 
 * @author Mark Pollack
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MapReduceTests {

	private String mapFunction = "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }";
	private String reduceFunction = "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}";

	@Autowired MongoTemplate template;
	@Autowired MongoDbFactory factory;

	MongoTemplate mongoTemplate;

	@Autowired
	@SuppressWarnings("unchecked")
	public void setMongo(Mongo mongo) throws Exception {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(ValueObject.class)));
		mappingContext.initialize();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MappingMongoConverter mappingConverter = new MappingMongoConverter(dbRefResolver, mappingContext);
		mappingConverter.afterPropertiesSet();
		this.mongoTemplate = new MongoTemplate(factory, mappingConverter);
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
		template.dropCollection(template.getCollectionName(ValueObject.class));
		template.dropCollection("jmr2");
		template.dropCollection("jmr2_out");
		template.dropCollection("jmr1_out");
		template.dropCollection("jmr1");
	}

	@Test
	@Ignore
	public void testForDocs() {
		createMapReduceData();
		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce("jmr1", mapFunction, reduceFunction,
				ValueObject.class);
		for (ValueObject valueObject : results) {
			System.out.println(valueObject);
		}
	}

	@Test
	public void testIssue260() {
		createContentAndVersionData();
		String map = "function () { emit(this.document_id, this.version); }";
		String reduce = "function (key, values) { return Math.max.apply(Math, values); }";
		MapReduceResults<ContentAndVersion> results = mongoTemplate.mapReduce("jmr2", map, reduce,
				new MapReduceOptions().outputCollection("jmr2_out"), ContentAndVersion.class);

		int size = 0;
		for (ContentAndVersion cv : results) {
			if (cv.getId().equals("Resume")) {
				assertEquals(6, cv.getValue().longValue());
			}
			if (cv.getId().equals("Schema")) {
				assertEquals(2, cv.getValue().longValue());
			}
			if (cv.getId().equals("mongoDB How-To")) {
				assertEquals(2, cv.getValue().longValue());
			}
			size++;
		}
		assertEquals(3, size);
	}

	@Test
	public void testIssue260Part2() {
		createNumberAndVersionData();
		String map = "function () { emit(this.number, this.version); }";
		String reduce = "function (key, values) { return Math.max.apply(Math, values); }";
		MapReduceResults<NumberAndVersion> results = mongoTemplate.mapReduce("jmr2", map, reduce,
				new MapReduceOptions().outputCollection("jmr2_out"), NumberAndVersion.class);
		int size = 0;
		for (NumberAndVersion nv : results) {
			if (nv.getId().equals("1")) {
				assertEquals(2, nv.getValue().longValue());
			}
			if (nv.getId().equals("2")) {
				assertEquals(6, nv.getValue().longValue());
			}
			if (nv.getId().equals("3")) {
				assertEquals(2, nv.getValue().longValue());
			}
			size++;
		}
		assertEquals(3, size);
	}

	private void createNumberAndVersionData() {
		NumberAndVersion nv1 = new NumberAndVersion();
		nv1.setNumber(1L);
		nv1.setVersion(1L);
		template.save(nv1, "jmr2");

		NumberAndVersion nv2 = new NumberAndVersion();
		nv2.setNumber(1L);
		nv2.setVersion(2L);
		template.save(nv2, "jmr2");

		NumberAndVersion nv3 = new NumberAndVersion();
		nv3.setNumber(2L);
		nv3.setVersion(6L);
		template.save(nv3, "jmr2");

		NumberAndVersion nv4 = new NumberAndVersion();
		nv4.setNumber(3L);
		nv4.setVersion(1L);
		template.save(nv4, "jmr2");

		NumberAndVersion nv5 = new NumberAndVersion();
		nv5.setNumber(3L);
		nv5.setVersion(2L);
		template.save(nv5, "jmr2");

	}

	private void createContentAndVersionData() {
		/*
		{ "_id" : 1, "document_id" : "mongoDB How-To", "author" : "Amos King", "content" : "...", "version" : 1 }
		{ "_id" : 2, "document_id" : "mongoDB How-To", "author" : "Amos King", "content" : "...", "version" : 1.1 }
		{ "_id" : 3, "document_id" : "Resume", "author" : "Author", "content" : "...", "version" : 6 }
		{ "_id" : 4, "document_id" : "Schema", "author" : "Someone Else", "content" : "...", "version" : 0.9 }
		{ "_id" : 5, "document_id" : "Schema", "author" : "Someone Else", "content" : "...", "version" : 1 }

		 */
		ContentAndVersion cv1 = new ContentAndVersion();
		cv1.setDocumentId("mongoDB How-To");
		cv1.setAuthor("Amos King");
		cv1.setContent("...");
		cv1.setVersion(1L);
		template.save(cv1, "jmr2");

		ContentAndVersion cv2 = new ContentAndVersion();
		cv2.setDocumentId("mongoDB How-To");
		cv2.setAuthor("Amos King");
		cv2.setContent("...");
		cv2.setVersion(2L);
		template.save(cv2, "jmr2");

		ContentAndVersion cv3 = new ContentAndVersion();
		cv3.setDocumentId("Resume");
		cv3.setAuthor("Author");
		cv3.setContent("...");
		cv3.setVersion(6L);
		template.save(cv3, "jmr2");

		ContentAndVersion cv4 = new ContentAndVersion();
		cv4.setDocumentId("Schema");
		cv4.setAuthor("Someone Else");
		cv4.setContent("...");
		cv4.setVersion(1L);
		template.save(cv4, "jmr2");

		ContentAndVersion cv5 = new ContentAndVersion();
		cv5.setDocumentId("Schema");
		cv5.setAuthor("Someone Else");
		cv5.setContent("...");
		cv5.setVersion(2L);
		template.save(cv5, "jmr2");

	}

	@Test
	public void testMapReduce() {
		performMapReduce(false, false);
	}

	@Test
	public void testMapReduceInline() {
		performMapReduce(true, false);
	}

	@Test
	public void testMapReduceWithQuery() {
		performMapReduce(false, true);
	}

	@Test
	public void testMapReduceInlineWithScope() {
		createMapReduceData();

		Map<String, Object> scopeVariables = new HashMap<String, Object>();
		scopeVariables.put("exclude", "a");

		String mapWithExcludeFunction = "function(){ for ( var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) emit( this.x[i] , 1 ); } }";

		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce("jmr1", mapWithExcludeFunction, reduceFunction,
				new MapReduceOptions().scopeVariables(scopeVariables).outputTypeInline(), ValueObject.class);
		Map<String, Float> m = copyToMap(results);
		assertEquals(3, m.size());
		assertEquals(2, m.get("b").intValue());
		assertEquals(2, m.get("c").intValue());
		assertEquals(1, m.get("d").intValue());
	}

	@Test
	public void testMapReduceExcludeQuery() {
		createMapReduceData();

		Query query = new Query(where("x").ne(new String[] { "a", "b" }));
		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce(query, "jmr1", mapFunction, reduceFunction,
				ValueObject.class);

		Map<String, Float> m = copyToMap(results);
		assertEquals(3, m.size());
		assertEquals(1, m.get("b").intValue());
		assertEquals(2, m.get("c").intValue());
		assertEquals(1, m.get("d").intValue());

	}

	private void performMapReduce(boolean inline, boolean withQuery) {
		createMapReduceData();
		MapReduceResults<ValueObject> results;
		if (inline) {
			if (withQuery) {
				results = mongoTemplate.mapReduce(new Query(), "jmr1", "classpath:map.js", "classpath:reduce.js",
						ValueObject.class);
			} else {
				results = mongoTemplate.mapReduce("jmr1", mapFunction, reduceFunction, ValueObject.class);
			}
		} else {
			if (withQuery) {
				results = mongoTemplate.mapReduce(new Query(), "jmr1", mapFunction, reduceFunction,
						options().outputCollection("jmr1_out"), ValueObject.class);
			} else {
				results = mongoTemplate.mapReduce("jmr1", mapFunction, reduceFunction,
						new MapReduceOptions().outputCollection("jmr1_out"), ValueObject.class);
			}
		}
		Map<String, Float> m = copyToMap(results);
		assertMapReduceResults(m);
	}

	private void createMapReduceData() {
		DBCollection c = mongoTemplate.getDb().getCollection("jmr1");
		c.save(new BasicDBObject("x", new String[] { "a", "b" }));
		c.save(new BasicDBObject("x", new String[] { "b", "c" }));
		c.save(new BasicDBObject("x", new String[] { "c", "d" }));
	}

	private Map<String, Float> copyToMap(MapReduceResults<ValueObject> results) {
		List<ValueObject> valueObjects = new ArrayList<ValueObject>();
		for (ValueObject valueObject : results) {
			valueObjects.add(valueObject);
		}

		Map<String, Float> m = new HashMap<String, Float>();
		for (ValueObject vo : valueObjects) {
			m.put(vo.getId(), vo.getValue());
		}
		return m;
	}

	private void assertMapReduceResults(Map<String, Float> m) {
		assertEquals(4, m.size());
		assertEquals(1, m.get("a").intValue());
		assertEquals(2, m.get("b").intValue());
		assertEquals(2, m.get("c").intValue());
		assertEquals(1, m.get("d").intValue());
	}

}
