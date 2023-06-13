/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.ReplSetClient;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Tests for {@link ReactiveChangeStreamOperation}.
 *
 * @author Christoph Strobl
 * @currentRead Dawn Cook - The Decoy Princess
 */
@ExtendWith(MongoClientExtension.class)
@EnableIfReplicaSetAvailable
public class ReactiveChangeStreamOperationSupportTests {

	static final String DATABASE_NAME = "rx-change-stream";
	static @ReplSetClient MongoClient mongoClient;

	ReactiveMongoTemplate template;

	@BeforeEach
	public void setUp() {

		template = new ReactiveMongoTemplate(mongoClient, DATABASE_NAME);

		MongoTestUtils.createOrReplaceCollectionNow(DATABASE_NAME, "person", mongoClient);
	}

	@AfterEach
	public void tearDown() {
		MongoTestUtils.dropCollectionNow(DATABASE_NAME, "person", mongoClient);
	}

	@Test // DATAMONGO-2089
	public void changeStreamEventsShouldBeEmittedCorrectly() throws InterruptedException {

		BlockingQueue<ChangeStreamEvent<Document>> documents = new LinkedBlockingQueue<>(100);

		Disposable disposable = template.changeStream(Document.class) //
				.watchCollection("person") //
				.listen() //
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 39);
		Person person3 = new Person("MongoDB", 37);

		Flux.merge(template.insert(person1).delayElement(Duration.ofMillis(2)),
				template.insert(person2).delayElement(Duration.ofMillis(2)),
				template.insert(person3).delayElement(Duration.ofMillis(2))) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList())).hasSize(3)
					.allMatch(Document.class::isInstance);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-1803
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	public void changeStreamEventsShouldBeConvertedCorrectly() throws InterruptedException {

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);

		Disposable disposable = template.changeStream(Person.class).listen() //
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 39);
		Person person3 = new Person("MongoDB", 37);

		Flux.merge(template.insert(person1).delayElement(Duration.ofMillis(2)),
				template.insert(person2).delayElement(Duration.ofMillis(2)),
				template.insert(person3).delayElement(Duration.ofMillis(2))) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList())).containsOnly(person1,
					person2, person3);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-1803
	public void changeStreamEventsShouldBeFilteredCorrectly() throws InterruptedException {

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);

		Disposable disposable = template.changeStream(Person.class) //
				.watchCollection(Person.class) //
				.filter(where("age").gte(38)) //
				.listen() //
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);
		Person person3 = new Person("MongoDB", 39);

		Flux.merge(template.save(person1), template.save(person2).delayElement(Duration.ofMillis(50)),
				template.save(person3).delayElement(Duration.ofMillis(100))) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList())).containsOnly(person1,
					person3);
		} finally {
			disposable.dispose();
		}
	}
}
