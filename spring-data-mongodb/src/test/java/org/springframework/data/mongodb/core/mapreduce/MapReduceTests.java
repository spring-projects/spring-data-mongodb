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

import static org.junit.Assert.assertEquals;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.mapreduce.MapReduceOptions.options;

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

	@Autowired
	MongoTemplate template;
	@Autowired
	MongoDbFactory factory;
	
	MongoTemplate mongoTemplate;

	@Autowired
	@SuppressWarnings("unchecked")
	public void setMongo(Mongo mongo) throws Exception {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(ValueObject.class)));
		mappingContext.afterPropertiesSet();

		MappingMongoConverter mappingConverter = new MappingMongoConverter(factory, mappingContext);
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
		template.dropCollection("jmr1_out");
		template.dropCollection("jmr1");
	}

	@Test
	@Ignore
	public void testForDocs() {
		createMapReduceData();
		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce("jmr1", mapFunction, reduceFunction, ValueObject.class);
		for (ValueObject valueObject : results) {
			System.out.println(valueObject);
		}
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
		
		String mapWithExcludeFunction =  "function(){ for ( var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) emit( this.x[i] , 1 ); } }";
	
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
		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce(query, "jmr1", mapFunction, reduceFunction, ValueObject.class);
		
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
				results = mongoTemplate.mapReduce(new Query(), "jmr1", "classpath:map.js", "classpath:reduce.js", ValueObject.class);
			} else {
				results = mongoTemplate.mapReduce("jmr1", mapFunction, reduceFunction, ValueObject.class);
			}
		} else {
			if (withQuery) {
				results = mongoTemplate.mapReduce(new Query(), "jmr1", mapFunction, reduceFunction, options().outputCollection("jmr1_out"), ValueObject.class);
			} else {
				results = mongoTemplate.mapReduce("jmr1", mapFunction, reduceFunction, new MapReduceOptions().outputCollection("jmr1_out"), ValueObject.class);
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
