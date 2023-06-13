/*
 * Copyright 2018-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.messaging.SubscriptionUtils.*;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoServerCondition;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.util.ErrorHandler;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Integration tests for {@link DefaultMessageListenerContainer}.
 *
 * @author Christoph Strobl
 */
@ExtendWith({ MongoTemplateExtension.class, MongoServerCondition.class })
public class DefaultMessageListenerContainerTests {

	static final String DATABASE_NAME = "change-stream-events";
	static final String COLLECTION_NAME = "collection-1";
	static final String COLLECTION_2_NAME = "collection-2";
	static final String COLLECTION_3_NAME = "collection-3";

	static final Duration TIMEOUT = Duration.ofSeconds(2);

	@Client static MongoClient client;

	@Template(database = DATABASE_NAME, initialEntitySet = Person.class) //
	static MongoTemplate template;

	MongoDatabaseFactory dbFactory = template.getMongoDatabaseFactory();

	MongoCollection<Document> collection = template.getCollection(COLLECTION_NAME);
	MongoCollection<Document> collection2 = template.getCollection(COLLECTION_2_NAME);

	private CollectingMessageListener<Object, Object> messageListener;

	@BeforeEach
	void beforeEach() throws InterruptedException {

		MongoTestUtils.dropCollectionNow(DATABASE_NAME, COLLECTION_NAME, client);
		MongoTestUtils.dropCollectionNow(DATABASE_NAME, COLLECTION_2_NAME, client);
		MongoTestUtils.dropCollectionNow(DATABASE_NAME, COLLECTION_3_NAME, client);

		Thread.sleep(100);

		messageListener = new CollectingMessageListener<>();
	}

	@Test // DATAMONGO-1803
	@EnableIfReplicaSetAvailable
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	public void shouldCollectMappedChangeStreamMessagesCorrectly() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, options()), Person.class);
		container.start();

		awaitSubscription(subscription, TIMEOUT);

		collection.insertOne(new Document("_id", "id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("firstname", "bar"));

		awaitMessages(messageListener, 2, TIMEOUT);

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(new Person("id-1", "foo"), new Person("id-2", "bar"));
	}

	@Test // DATAMONGO-2322
	@EnableIfReplicaSetAvailable
	public void shouldNotifyErrorHandlerOnErrorInListener() throws InterruptedException {

		ErrorHandler errorHandler = mock(ErrorHandler.class);
		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		AtomicBoolean thrownException = new AtomicBoolean();
		Subscription subscription = container.register(new ChangeStreamRequest(message -> {

			try {
				if (thrownException.compareAndSet(false, true)) {
					throw new IllegalStateException("Boom");
				}
			} finally {
				messageListener.onMessage(message);
			}

		}, options()), Person.class, errorHandler);
		container.start();

		awaitSubscription(subscription, TIMEOUT);

		collection.insertOne(new Document("_id", "id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("firstname", "bar"));

		awaitMessages(messageListener, 2, TIMEOUT);

		verify(errorHandler, atLeast(1)).handleError(any(IllegalStateException.class));
		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	@EnableIfReplicaSetAvailable
	public void shouldNoLongerReceiveMessagesWhenContainerStopped() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, options()), Document.class);
		container.start();

		awaitSubscription(subscription, TIMEOUT);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(messageListener, 2, TIMEOUT);

		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));

		Thread.sleep(200);

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	@EnableIfReplicaSetAvailable
	public void shouldReceiveMessagesWhenAddingRequestToAlreadyStartedContainer() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		Document unexpected = new Document("_id", "id-1").append("value", "foo");
		collection.insertOne(unexpected);

		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, options()), Document.class);

		awaitSubscription(subscription, TIMEOUT);

		Document expected = new Document("_id", "id-2").append("value", "bar");
		collection.insertOne(expected);

		awaitMessages(messageListener, 1, TIMEOUT);
		container.stop();

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(expected);
	}

	@Test // DATAMONGO-1803
	@EnableIfReplicaSetAvailable
	public void shouldStartReceivingMessagesWhenContainerStarts() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, options()), Document.class);

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

		dbFactory.getMongoDatabase().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		awaitSubscription(container.register(new TailableCursorRequest(messageListener, options()), Document.class),
				TIMEOUT);

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(messageListener, 2, TIMEOUT);
		container.stop();

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void tailableCursorOnEmptyCollection() throws InterruptedException {

		dbFactory.getMongoDatabase().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		awaitSubscription(container.register(new TailableCursorRequest(messageListener, options()), Document.class),
				TIMEOUT);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(messageListener, 2, TIMEOUT);
		container.stop();

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void abortsSubscriptionOnError() throws InterruptedException {

		dbFactory.getMongoDatabase().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Subscription subscription = container.register(new TailableCursorRequest(messageListener, options()),
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

		dbFactory.getMongoDatabase().createCollection(COLLECTION_3_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		ErrorHandler errorHandler = mock(ErrorHandler.class);

		DefaultMessageListenerContainer container = new DefaultMessageListenerContainer(template,
				new SimpleAsyncTaskExecutor(), errorHandler);

		try {
			container.start();

			Subscription subscription = container.register(new TailableCursorRequest(messageListener, options()),
					Document.class);

			SubscriptionUtils.awaitSubscription(subscription);
			dbFactory.getMongoDatabase().drop();

			verify(errorHandler, atLeast(1)).handleError(any(DataAccessException.class));
		} finally {
			container.stop();
		}
	}

	@Test // DATAMONGO-1803
	@EnableIfReplicaSetAvailable
	public void runsMoreThanOneTaskAtOnce() throws InterruptedException {

		dbFactory.getMongoDatabase().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		CollectingMessageListener<Document, Document> tailableListener = new CollectingMessageListener<>();
		Subscription tailableSubscription = container.register(new TailableCursorRequest(tailableListener, options()),
				Document.class);

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> changeStreamListener = new CollectingMessageListener<>();
		Subscription changeStreamSubscription = container.register(new ChangeStreamRequest(changeStreamListener, options()),
				Document.class);

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
	@EnableIfReplicaSetAvailable
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	public void databaseLevelWatch() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, RequestOptions.none()),
				Person.class);

		container.start();

		awaitSubscription(subscription, TIMEOUT);

		collection.insertOne(new Document("_id", "col-1-id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "col-1-id-2").append("firstname", "bar"));

		collection2.insertOne(new Document("_id", "col-2-id-1").append("firstname", "bar"));
		collection2.insertOne(new Document("_id", "col-2-id-2").append("firstname", "foo"));

		awaitMessages(messageListener, 4, TIMEOUT);

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(new Person("col-1-id-1", "foo"), new Person("col-1-id-2", "bar"),
						new Person("col-2-id-1", "bar"), new Person("col-2-id-2", "foo"));
	}

	static class Person {

		@Id String id;
		private String firstname;
		private String lastname;

		public Person() {}

		public Person(String id, String firstname) {
			this.id = id;
			this.firstname = firstname;
		}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(firstname, person.firstname)
					&& Objects.equals(lastname, person.lastname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname, lastname);
		}

		public String toString() {
			return "DefaultMessageListenerContainerTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname()
					+ ", lastname=" + this.getLastname() + ")";
		}
	}

	static ChangeStreamRequestOptions options() {
		return new ChangeStreamRequestOptions(DATABASE_NAME, COLLECTION_NAME, Duration.ofMillis(10),
				ChangeStreamOptions.builder().build());
	}
}
