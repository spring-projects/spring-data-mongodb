/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.messaging.SubscriptionUtils.*;

import lombok.Data;

import java.time.Duration;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.util.ErrorHandler;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Integration tests for {@link DefaultMessageListenerContainer}.
 *
 * @author Christoph Strobl
 */
public class DefaultMessageListenerContainerTests {

	public static final String DATABASE_NAME = "change-stream-events";
	public static final String COLLECTION_NAME = "collection-1";
	public static final String COLLECTION_2_NAME = "collection-2";

	public @Rule TestRule replSet = ReplicaSet.none();

	MongoDbFactory dbFactory;
	MongoCollection<Document> collection;
	MongoCollection<Document> collection2;

	private CollectingMessageListener<Object, Object> messageListener;
	private MongoTemplate template;

	@Before
	public void setUp() {

		dbFactory = new SimpleMongoDbFactory(new MongoClient(), DATABASE_NAME);
		template = new MongoTemplate(dbFactory);

		template.dropCollection(COLLECTION_NAME);
		template.dropCollection(COLLECTION_2_NAME);

		collection = template.getCollection(COLLECTION_NAME);
		collection2 = template.getCollection(COLLECTION_2_NAME);

		messageListener = new CollectingMessageListener<>();
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldCollectMappedChangeStreamMessagesCorrectly() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Person.class);
		container.start();

		awaitSubscription(subscription, Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("firstname", "bar"));

		awaitMessages(messageListener, 2, Duration.ofMillis(500));

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(new Person("id-1", "foo"), new Person("id-2", "bar"));

	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldNoLongerReceiveMessagesWhenConainerStopped() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Document.class);
		container.start();

		awaitSubscription(subscription, Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(messageListener, 2, Duration.ofMillis(500));

		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));

		Thread.sleep(200);

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldReceiveMessagesWhenAddingRequestToAlreadyStartedContainer() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		Document unexpected = new Document("_id", "id-1").append("value", "foo");
		collection.insertOne(unexpected);

		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Document.class);

		awaitSubscription(subscription, Duration.ofMillis(500));

		Document expected = new Document("_id", "id-2").append("value", "bar");
		collection.insertOne(expected);

		awaitMessages(messageListener, 1, Duration.ofMillis(500));
		container.stop();

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(expected);
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldStartReceivingMessagesWhenContainerStarts() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Document.class);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Thread.sleep(200);

		container.start();

		awaitSubscription(subscription);

		Document expected = new Document("_id", "id-2").append("value", "bar");
		collection.insertOne(expected);

		awaitMessages(messageListener);

		container.stop();

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(expected);
	}

	@Test // DATAMONGO-1803
	public void tailableCursor() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		awaitSubscription(
				container.register(new TailableCursorRequest(messageListener, () -> COLLECTION_NAME), Document.class),
				Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(messageListener, 2, Duration.ofSeconds(2));
		container.stop();

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void tailableCursorOnEmptyCollection() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		awaitSubscription(
				container.register(new TailableCursorRequest(messageListener, () -> COLLECTION_NAME), Document.class),
				Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(messageListener, 2, Duration.ofSeconds(2));
		container.stop();

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void abortsSubscriptionOnError() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Subscription subscription = container.register(new TailableCursorRequest(messageListener, () -> COLLECTION_NAME),
				Document.class);

		awaitSubscription(subscription);

		assertThat(subscription.isActive()).isTrue();

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));
		collection.drop();

		awaitMessages(messageListener);

		assertThat(subscription.isActive()).isFalse();

		container.stop();
	}

	@Test // DATAMONGO-1803
	public void callsDefaultErrorHandlerOnError() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		ErrorHandler errorHandler = mock(ErrorHandler.class);

		DefaultMessageListenerContainer container = new DefaultMessageListenerContainer(template,
				new SimpleAsyncTaskExecutor(), errorHandler);

		try {
			container.start();

			Subscription subscription = container.register(new TailableCursorRequest(messageListener, () -> COLLECTION_NAME),
					Document.class);

			SubscriptionUtils.awaitSubscription(subscription);

			template.dropCollection(COLLECTION_NAME);

			Thread.sleep(20);

			verify(errorHandler, atLeast(1)).handleError(any(DataAccessException.class));
		} finally {
			container.stop();
		}
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void runsMoreThanOneTaskAtOnce() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		CollectingMessageListener<Document, Document> tailableListener = new CollectingMessageListener<>();
		Subscription tailableSubscription = container
				.register(new TailableCursorRequest(tailableListener, () -> COLLECTION_NAME), Document.class);

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> changeStreamListener = new CollectingMessageListener<>();
		Subscription changeStreamSubscription = container
				.register(new ChangeStreamRequest(changeStreamListener, () -> COLLECTION_NAME), Document.class);

		awaitSubscriptions(tailableSubscription, changeStreamSubscription);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		awaitMessages(tailableListener);
		awaitMessages(changeStreamListener);

		assertThat(tailableListener.getTotalNumberMessagesReceived()).isEqualTo(1);
		assertThat(tailableListener.getFirstMessage().getRaw()).isInstanceOf(Document.class);

		assertThat(changeStreamListener.getTotalNumberMessagesReceived()).isEqualTo(1);
		assertThat(changeStreamListener.getFirstMessage().getRaw()).isInstanceOf(ChangeStreamDocument.class);
	}

	@Test // DATAMONGO-2012
	@IfProfileValue(name = "replSet", value = "true")
	public void databaseLevelWatch() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, RequestOptions.none()),
				Person.class);

		container.start();

		awaitSubscription(subscription, Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "col-1-id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "col-1-id-2").append("firstname", "bar"));

		collection2.insertOne(new Document("_id", "col-2-id-1").append("firstname", "bar"));
		collection2.insertOne(new Document("_id", "col-2-id-2").append("firstname", "foo"));

		awaitMessages(messageListener, 4, Duration.ofMillis(500));

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(new Person("col-1-id-1", "foo"), new Person("col-1-id-2", "bar"),
						new Person("col-2-id-1", "bar"), new Person("col-2-id-2", "foo"));
	}

	@Data
	static class Person {
		@Id String id;
		private String firstname;
		private String lastname;

		public Person() {}

		public Person(String id, String firstname) {
			this.id = id;
			this.firstname = firstname;
		}
	}
}
