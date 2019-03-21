/*
 * Copyright 2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.Success;

/**
 * Integration tests for group-by operations.
 *
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveGroupByTests {

	public static final String COLLECTION_NAME = "group_test_collection";

	@Autowired ReactiveMongoTemplate mongoTemplate;

	@Before
	public void setUp() {

		StepVerifier.create(mongoTemplate.dropCollection(mongoTemplate.getCollectionName(XObject.class))
				.mergeWith(mongoTemplate.dropCollection(COLLECTION_NAME))).verifyComplete();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1889
	public void throwsExceptionWhenUsingBlockingCalls() {

		createGroupByData();

		mongoTemplate.group("group_test_collection",
				GroupBy.key("x").initialDocument(new Document("count", 0)).reduceFunction("classpath:reduce.js"),
				XObject.class);
	}

	@Test // DATAMONGO-1889
	public void simpleGroupFunction() {

		createGroupByData();

		StepVerifier
				.create(mongoTemplate.group("group_test_collection",
						GroupBy.key("x").initialDocument(new Document("count", 0))
								.reduceFunction("function(doc, prev) { prev.count += 1 }"),
						XObject.class).buffer(3))
				.consumeNextWith(this::assertMapReduceResults).verifyComplete();
	}

	@Test // DATAMONGO-1889
	public void simpleGroupWithKeyFunction() {

		createGroupByData();

		StepVerifier
				.create(mongoTemplate.group("group_test_collection",
						GroupBy.keyFunction("function(doc) { return { x : doc.x }; }").initialDocument("{ count: 0 }")
								.reduceFunction("function(doc, prev) { prev.count += 1 }"),
						XObject.class).buffer(3))
				.consumeNextWith(this::assertMapReduceResults).verifyComplete();
	}

	private void assertMapReduceResults(List<XObject> results) {

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
	}

	private void createGroupByData() {

		MongoCollection<Document> collection = mongoTemplate.getCollection(COLLECTION_NAME);

		StepVerifier
				.create(collection.insertMany(Arrays.asList(new Document("x", 1), new Document("x", 1), new Document("x", 2),
						new Document("x", 3), new Document("x", 3), new Document("x", 3))))
				.expectNext(Success.SUCCESS).verifyComplete();
	}
}
