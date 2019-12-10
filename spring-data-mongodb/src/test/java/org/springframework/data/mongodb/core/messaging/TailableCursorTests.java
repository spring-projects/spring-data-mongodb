/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import static org.springframework.data.mongodb.core.messaging.SubscriptionUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import lombok.Data;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.messaging.Message.MessageProperties;
import org.springframework.data.mongodb.core.messaging.TailableCursorRequest.TailableCursorRequestOptions;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

/**
 * Integration test for subscribing to a capped {@link com.mongodb.client.MongoCollection} inside the
 * {@link DefaultMessageListenerContainer} using {@link TailableCursorRequest}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class TailableCursorTests {

	static final String COLLECTION_NAME = "user";

	static ThreadPoolExecutor executor;
	MongoTemplate template;
	MessageListenerContainer container;

	User jellyBelly;
	User huffyFluffy;
	User sugarSplashy;

	@BeforeClass
	public static void beforeClass() {
		executor = new ThreadPoolExecutor(2, 2, 1, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
	}

	@Before
	public void setUp() {

		template = new MongoTemplate(MongoTestUtils.client(), "tailable-cursor-tests");

		template.dropCollection(User.class);
		template.createCollection(User.class, CollectionOptions.empty().capped().maxDocuments(10000).size(10000));

		container = new DefaultMessageListenerContainer(template, executor);
		container.start();

		jellyBelly = new User();
		jellyBelly.id = "id-1";
		jellyBelly.userName = "jellyBelly";
		jellyBelly.age = 7;

		huffyFluffy = new User();
		huffyFluffy.id = "id-2";
		huffyFluffy.userName = "huffyFluffy";
		huffyFluffy.age = 7;

		sugarSplashy = new User();
		sugarSplashy.id = "id-3";
		sugarSplashy.userName = "sugarSplashy";
		sugarSplashy.age = 5;
	}

	@After
	public void tearDown() {
		container.stop();
	}

	@AfterClass
	public static void afterClass() {
		executor.shutdown();
	}

	@Test // DATAMONGO-1803
	public void readsDocumentMessageCorrectly() throws InterruptedException {

		CollectingMessageListener<Document, Document> messageListener = new CollectingMessageListener<>();

		awaitSubscription(
				container.register(new TailableCursorRequest<>(messageListener, () -> COLLECTION_NAME), Document.class));

		template.save(jellyBelly);

		awaitMessages(messageListener, 1);

		Document expected = new Document("_id", "id-1").append("user_name", "jellyBelly").append("age", 7).append("_class",
				TailableCursorTests.User.class.getName());

		assertThat(messageListener.getFirstMessage().getProperties())
				.isEqualTo(MessageProperties.builder().collectionName("user").databaseName("tailable-cursor-tests").build());
		assertThat(messageListener.getFirstMessage().getRaw()).isEqualTo(expected);
		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(expected);
	}

	@Test // DATAMONGO-1803
	public void convertsMessageCorrectly() throws InterruptedException {

		CollectingMessageListener<Document, User> messageListener = new CollectingMessageListener<>();

		awaitSubscription(
				container.register(new TailableCursorRequest<>(messageListener, () -> COLLECTION_NAME), User.class));

		template.save(jellyBelly);

		awaitMessages(messageListener, 1);

		Document expected = new Document("_id", "id-1").append("user_name", "jellyBelly").append("age", 7).append("_class",
				TailableCursorTests.User.class.getName());

		assertThat(messageListener.getFirstMessage().getProperties())
				.isEqualTo(MessageProperties.builder().collectionName("user").databaseName("tailable-cursor-tests").build());
		assertThat(messageListener.getFirstMessage().getRaw()).isEqualTo(expected);
		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(jellyBelly);
	}

	@Test // DATAMONGO-1803
	public void filtersMessagesCorrectly() throws InterruptedException {

		CollectingMessageListener<Document, User> messageListener = new CollectingMessageListener<>();

		awaitSubscription(container.register(new TailableCursorRequest<>(messageListener,
				TailableCursorRequestOptions.builder().collection(COLLECTION_NAME).filter(query(where("age").is(7))).build()),
				User.class));

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener);

		assertThat(messageListener.getMessages().stream().map(Message::getBody)).hasSize(2).doesNotContain(sugarSplashy);
	}

	@Test // DATAMONGO-1803
	public void mapsFilterToDomainType() throws InterruptedException {

		CollectingMessageListener<Document, User> messageListener = new CollectingMessageListener<>();

		awaitSubscription(
				container
						.register(
								new TailableCursorRequest<>(messageListener, TailableCursorRequestOptions.builder()
										.collection(COLLECTION_NAME).filter(query(where("userName").is("sugarSplashy"))).build()),
								User.class));

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener);

		assertThat(messageListener.getMessages().stream().map(Message::getBody)).hasSize(1).containsExactly(sugarSplashy);
	}

	@Test // DATAMONGO-1803
	public void emitsFromStart() throws InterruptedException {

		template.save(jellyBelly);
		template.save(huffyFluffy);

		CollectingMessageListener<Document, User> messageListener = new CollectingMessageListener<>();

		awaitSubscription(
				container.register(new TailableCursorRequest<>(messageListener, () -> COLLECTION_NAME), User.class));

		template.save(sugarSplashy);

		awaitMessages(messageListener);

		assertThat(messageListener.getMessages().stream().map(Message::getBody)).hasSize(3).containsExactly(jellyBelly,
				huffyFluffy, sugarSplashy);
	}

	@Data
	static class User {

		@Id String id;
		@Field("user_name") String userName;
		int age;
	}
}
