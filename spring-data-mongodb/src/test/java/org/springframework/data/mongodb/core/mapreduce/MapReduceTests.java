/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.mapreduce.MapReduceOptions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Box;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.client.MongoCollection;

/**
 * Integration test for {@link MongoTemplate}'s Map-Reduce operations
 *
 * @author Mark Pollack
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MapReduceTests {

	private static final String MAP_FUNCTION = "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }";
	private static final String REDUCE_FUNCTION = "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}";

	@Autowired MongoTemplate template;
	@Autowired MongoTemplate mongoTemplate;

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
		template.dropCollection("jmrWithGeo");
		template.getMongoDbFactory().getDb("jmr1-out-db").drop();
	}

	@Test // DATADOC-7
	@Ignore
	public void testForDocs() {

		createMapReduceData();
		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce("jmr1", MAP_FUNCTION, REDUCE_FUNCTION,
				ValueObject.class);

		for (ValueObject valueObject : results) {
			System.out.println(valueObject);
		}
	}

	@Test // DATAMONGO-260
	public void testIssue260() {

		createContentAndVersionData();
		String map = "function () { emit(this.document_id, this.version); }";
		String reduce = "function (key, values) { return Math.max.apply(Math, values); }";

		MapReduceResults<ContentAndVersion> results = mongoTemplate.mapReduce("jmr2", map, reduce,
				new MapReduceOptions().outputCollection("jmr2_out"), ContentAndVersion.class);

		assertThat(results).hasSize(3);
		for (ContentAndVersion cv : results) {

			if ("Resume".equals(cv.getId())) {
				assertThat(cv.getValue().longValue()).isEqualTo(6);
			}
			if ("Schema".equals(cv.getId())) {
				assertThat(cv.getValue().longValue()).isEqualTo(2);
			}
			if ("mongoDB How-To".equals(cv.getId())) {
				assertThat(cv.getValue().longValue()).isEqualTo(2);
			}
		}

	}

	@Test // DATAMONGO-260
	public void testIssue260Part2() {

		createNumberAndVersionData();
		String map = "function () { emit(this.number, this.version); }";
		String reduce = "function (key, values) { return Math.max.apply(Math, values); }";

		MapReduceResults<NumberAndVersion> results = mongoTemplate.mapReduce("jmr2", map, reduce,
				new MapReduceOptions().outputCollection("jmr2_out"), NumberAndVersion.class);

		for (NumberAndVersion nv : results) {
			if ("1".equals(nv.getId())) {
				assertThat(nv.getValue().longValue()).isEqualTo(2);
			}
			if ("2".equals(nv.getId())) {
				assertThat(nv.getValue().longValue()).isEqualTo(6);
			}
			if ("3".equals(nv.getId())) {
				assertThat(nv.getValue().longValue()).isEqualTo(2);
			}
		}

		assertThat(results).hasSize(3);
	}

	@Test // DATADOC-7, DATAMONGO-2027
	public void testMapReduce() {

		performMapReduce(false, false);

		List<ValueObject> results = mongoTemplate.find(new Query(), ValueObject.class, "jmr1_out");
		assertMapReduceResults(copyToMap(results));
	}

	@Test // DATADOC-7, DATAMONGO-2027
	public void testMapReduceInline() {

		performMapReduce(true, false);
		assertThat(template.collectionExists("jmr1_out")).isFalse();
	}

	@Test // DATAMONGO-2027
	public void mapReduceWithOutputDatabaseShouldWorkCorrectly() {

		createMapReduceData();

		mongoTemplate.mapReduce("jmr1", MAP_FUNCTION, REDUCE_FUNCTION,
				options().outputDatabase("jmr1-out-db").outputCollection("jmr1-out"), ValueObject.class);

		assertThat(template.getMongoDbFactory().getDb("jmr1-out-db").listCollectionNames().into(new ArrayList<>()))
				.contains("jmr1-out");
	}

	@Test // DATADOC-7
	public void testMapReduceWithQuery() {
		performMapReduce(false, true);
	}

	@Test // DATADOC-7
	public void testMapReduceInlineWithScope() {

		createMapReduceData();

		Map<String, Object> scopeVariables = new HashMap<String, Object>();
		scopeVariables.put("exclude", "a");

		String mapWithExcludeFunction = "function(){ for ( var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) emit( this.x[i] , 1 ); } }";

		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce("jmr1", mapWithExcludeFunction, REDUCE_FUNCTION,
				new MapReduceOptions().scopeVariables(scopeVariables).outputTypeInline(), ValueObject.class);

		assertThat(copyToMap(results)) //
				.hasSize(3) //
				.containsEntry("b", 2F) //
				.containsEntry("c", 2F) //
				.containsEntry("d", 1F);
	}

	@Test // DATADOC-7
	public void testMapReduceExcludeQuery() {

		createMapReduceData();

		Query query = new Query(where("x").ne(new String[] { "a", "b" }));
		MapReduceResults<ValueObject> results = mongoTemplate.mapReduce(query, "jmr1", MAP_FUNCTION, REDUCE_FUNCTION,
				ValueObject.class);

		assertThat(copyToMap(results)) //
				.hasSize(3) //
				.containsEntry("b", 1F) //
				.containsEntry("c", 2F) //
				.containsEntry("d", 1F);
	}

	@Test // DATAMONGO-938
	public void mapReduceShouldUseQueryMapper() {

		MongoCollection<Document> c = mongoTemplate.getDb().getCollection("jmrWithGeo", Document.class);

		c.insertOne(new Document("x", Arrays.asList("a", "b")).append("loc", Arrays.asList(0D, 0D)));
		c.insertOne(new Document("x", Arrays.asList("b", "c")).append("loc", Arrays.asList(0D, 0D)));
		c.insertOne(new Document("x", Arrays.asList("c", "d")).append("loc", Arrays.asList(0D, 0D)));

		Query query = new Query(where("x").ne(new String[] { "a", "b" }).and("loc")
				.within(new Box(new double[] { 0, 0 }, new double[] { 1, 1 })));

		MapReduceResults<ValueObject> results = template.mapReduce(query, "jmrWithGeo", MAP_FUNCTION, REDUCE_FUNCTION,
				ValueObject.class);

		assertThat(copyToMap(results)) //
				.hasSize(3) //
				.containsEntry("b", 1F) //
				.containsEntry("c", 2F) //
				.containsEntry("d", 1F);
	}

	private void performMapReduce(boolean inline, boolean withQuery) {

		createMapReduceData();
		MapReduceResults<ValueObject> results;
		if (inline) {
			if (withQuery) {
				results = mongoTemplate.mapReduce(new Query(), "jmr1", "classpath:map.js", "classpath:reduce.js",
						ValueObject.class);
			} else {
				results = mongoTemplate.mapReduce("jmr1", MAP_FUNCTION, REDUCE_FUNCTION, ValueObject.class);
			}
		} else {
			if (withQuery) {
				results = mongoTemplate.mapReduce(new Query(), "jmr1", MAP_FUNCTION, REDUCE_FUNCTION,
						options().outputCollection("jmr1_out"), ValueObject.class);
			} else {
				results = mongoTemplate.mapReduce("jmr1", MAP_FUNCTION, REDUCE_FUNCTION,
						new MapReduceOptions().outputCollection("jmr1_out"), ValueObject.class);
			}
		}

		assertMapReduceResults(copyToMap(results));
	}

	private void createMapReduceData() {

		MongoCollection<Document> c = mongoTemplate.getDb().getCollection("jmr1", Document.class);
		c.insertOne(new Document("x", Arrays.asList("a", "b")));
		c.insertOne(new Document("x", Arrays.asList("b", "c")));
		c.insertOne(new Document("x", Arrays.asList("c", "d")));
	}

	private Map<String, Float> copyToMap(Iterable<ValueObject> results) {

		List<ValueObject> valueObjects = new ArrayList<>();
		for (ValueObject valueObject : results) {
			valueObjects.add(valueObject);
		}

		Map<String, Float> m = new HashMap<>();
		for (ValueObject vo : valueObjects) {
			m.put(vo.getId(), vo.getValue());
		}
		return m;
	}

	private void assertMapReduceResults(Map<String, Float> map) {

		assertThat(map) //
				.hasSize(4) //
				.containsEntry("a", 1F) //
				.containsEntry("b", 2F) //
				.containsEntry("c", 2F) //
				.containsEntry("d", 1F);
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
}
