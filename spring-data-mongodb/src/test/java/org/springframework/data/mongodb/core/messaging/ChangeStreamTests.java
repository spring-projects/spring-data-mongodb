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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.messaging.SubscriptionUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.RepeatFailedTest;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.ChangeStreamTask.ChangeStreamEventMessage;
import org.springframework.data.mongodb.core.messaging.Message.MessageProperties;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.MongoVersion;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

/**
 * Integration test for subscribing to a {@link com.mongodb.operation.ChangeStreamBatchCursor} inside the
 * {@link DefaultMessageListenerContainer} using {@link ChangeStreamRequest}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Myroslav Kosinskyi
 */
@ExtendWith({ MongoTemplateExtension.class })
@EnableIfReplicaSetAvailable
class ChangeStreamTests {

	private static ThreadPoolExecutor executor;

	@Template(initialEntitySet = User.class, replicaSet = true) //
	private static MongoTestTemplate template;

	private MessageListenerContainer container;

	private User jellyBelly;
	private User huffyFluffy;
	private User sugarSplashy;

	@BeforeAll
	static void beforeClass() {
		executor = new ThreadPoolExecutor(2, 2, 1, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
	}

	@BeforeEach
	void setUp() {

		template.dropCollection(User.class);

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

	@AfterEach
	void tearDown() {
		container.stop();
	}

	@AfterAll
	static void afterClass() {
		executor.shutdown();
	}

	@Test // DATAMONGO-1803
	void readsPlainDocumentMessageCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(messageListener,
				new ChangeStreamRequestOptions(null, "user", Duration.ofMillis(10), ChangeStreamOptions.builder().build()));

		Subscription subscription = container.register(request, Document.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		awaitMessages(messageListener, 1);

		Message<ChangeStreamDocument<Document>, Document> message1 = messageListener.getFirstMessage();

		assertThat(message1.getRaw()).isNotNull();
		assertThat(message1.getProperties())
				.isEqualTo(MessageProperties.builder().collectionName("user").databaseName("change-stream-tests").build());
		assertThat(message1.getBody()).isEqualTo(new Document("_id", "id-1").append("user_name", "jellyBelly")
				.append("age", 7).append("_class", User.class.getName()));
	}

	@Test // DATAMONGO-1803
	void useSimpleAggregationToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(match(where("age").is(7)))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(sugarSplashy);
	}

	@Test // DATAMONGO-1803
	@MongoVersion(asOf = "4.0")
	void useAggregationToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(match(
						new Criteria().orOperator(where("user_name").is("huffyFluffy"), where("user_name").is("jellyBelly"))))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(sugarSplashy);
	}

	@RepeatFailedTest(3) // DATAMONGO-1803
	void mapsTypedAggregationToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.publishTo(messageListener) //
				.filter(newAggregation(User.class,
						match(new Criteria().orOperator(where("userName").is("huffyFluffy"), where("userName").is("jellyBelly"))))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener, 2);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(sugarSplashy);
	}

	@Test // DATAMONGO-1803
	void mapsReservedWordsCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.publishTo(messageListener) //
				.filter(newAggregation(User.class, match(where("operationType").is("replace")))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);
		template.save(sugarSplashy);

		User replacement = new User();
		replacement.id = jellyBelly.id;
		replacement.userName = new StringBuilder(jellyBelly.userName).reverse().toString();
		replacement.age = jellyBelly.age;

		template.save(replacement);

		awaitMessages(messageListener, 1);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(1).containsExactly(replacement);
	}

	@Test // DATAMONGO-1803
	void plainAggregationPipelineToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.publishTo(messageListener) //
				.filter(new Document("$match", new Document("fullDocument.user_name", "sugarSplashy"))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener, 1);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(1).containsExactly(sugarSplashy);
	}

	@Test // DATAMONGO-1803
	void resumesCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener1 = new CollectingMessageListener<>();
		Subscription subscription1 = container.register(
				new ChangeStreamRequest<>(messageListener1,
						new ChangeStreamRequestOptions(null, "user", Duration.ofMillis(10), ChangeStreamOptions.builder().build())),
				User.class);

		awaitSubscription(subscription1);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener1, 3);

		BsonDocument resumeToken = messageListener1.getFirstMessage().getRaw().getResumeToken();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener2 = new CollectingMessageListener<>();
		ChangeStreamRequest<User> subSequentRequest = ChangeStreamRequest.builder().collection("user")
				.publishTo(messageListener2).resumeToken(resumeToken).maxAwaitTime(Duration.ofMillis(10)).build();

		Subscription subscription2 = container.register(subSequentRequest, User.class);
		awaitSubscription(subscription2);

		awaitMessages(messageListener2, 2);

		List<User> messageBodies = messageListener2.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(jellyBelly);
	}

	@Test // DATAMONGO-1803
	void readsAndConvertsMessageBodyCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = new ChangeStreamRequest<>(messageListener,
				new ChangeStreamRequestOptions(null, "user", Duration.ofMillis(10), ChangeStreamOptions.builder().build()));

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		awaitMessages(messageListener, 1);

		Message<ChangeStreamDocument<Document>, User> message1 = messageListener.getFirstMessage();

		assertThat(message1.getRaw()).isNotNull();
		assertThat(message1.getProperties())
				.isEqualTo(MessageProperties.builder().collectionName("user").databaseName("change-stream-tests").build());
		assertThat(message1.getBody()).isEqualTo(jellyBelly);
	}

	@Test // DATAMONGO-1803
	void readsAndConvertsUpdateMessageBodyCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = new ChangeStreamRequest<>(messageListener,
				new ChangeStreamRequestOptions(null, "user", Duration.ofMillis(10), ChangeStreamOptions.builder().build()));

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(jellyBelly);
		assertThat(messageListener.getLastMessage().getBody()).isNotNull().hasFieldOrPropertyWithValue("age", 8);
	}

	@Test // DATAMONGO-1803
	void readsOnlyDiffForUpdateWhenNotMappedToDomainType() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(messageListener,
				new ChangeStreamRequestOptions(null, "user", Duration.ofMillis(10), ChangeStreamOptions.builder().build()));

		Subscription subscription = container.register(request, Document.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(new Document("_id", "id-1")
				.append("user_name", "jellyBelly").append("age", 7).append("_class", User.class.getName()));
		assertThat(messageListener.getLastMessage().getBody()).isNull();
	}

	@Test // DATAMONGO-1803
	void readsOnlyDiffForUpdateWhenOptionsDeclareDefaultExplicitly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentLookup(FullDocument.DEFAULT) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(jellyBelly);
		assertThat(messageListener.getLastMessage().getBody()).isNull();
	}

	@Test // DATAMONGO-1803
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	void readsFullDocumentForUpdateWhenNotMappedToDomainTypeButLookupSpecified() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<Document> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentLookup(FullDocument.UPDATE_LOOKUP) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, Document.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(new Document("_id", "id-1")
				.append("user_name", "jellyBelly").append("age", 7).append("_class", User.class.getName()));
		assertThat(messageListener.getLastMessage().getBody()).isEqualTo(new Document("_id", "id-1")
				.append("user_name", "jellyBelly").append("age", 8).append("_class", User.class.getName()));
	}

	@Test // DATAMONGO-2012, DATAMONGO-2113
	@MongoVersion(asOf = "4.0")
	void resumeAtTimestampCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener1 = new CollectingMessageListener<>();
		Subscription subscription1 = container.register(
				new ChangeStreamRequest<>(messageListener1,
						new ChangeStreamRequestOptions(null, "user", Duration.ofMillis(10), ChangeStreamOptions.builder().build())),
				User.class);

		awaitSubscription(subscription1);

		template.save(jellyBelly);

		Thread.sleep(1000); // cluster timestamp is in seconds, so we need to wait at least one.

		template.save(sugarSplashy);

		awaitMessages(messageListener1, 12);

		Instant resumeAt = ((ChangeStreamEventMessage) messageListener1.getLastMessage()).getTimestamp();

		template.save(huffyFluffy);

		awaitMessages(messageListener1, 3);

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener2 = new CollectingMessageListener<>();
		ChangeStreamRequest<User> subSequentRequest = ChangeStreamRequest.builder() //
				.collection("user") //
				.resumeAt(resumeAt) //
				.publishTo(messageListener2) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription2 = container.register(subSequentRequest, User.class);
		awaitSubscription(subscription2);

		awaitMessages(messageListener2, 2);

		List<User> messageBodies = messageListener2.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(jellyBelly);
	}

	@Test // DATAMONGO-1996
	void filterOnNestedElementWorksCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(User.class, match(where("address.street").is("flower street")))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		jellyBelly.address = new Address();
		jellyBelly.address.street = "candy ave";

		huffyFluffy.address = new Address();
		huffyFluffy.address.street = "flower street";

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(1).contains(huffyFluffy);
	}

	@Test // DATAMONGO-1996
	void filterOnUpdateDescriptionElement() throws InterruptedException {

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(User.class, match(where("updateDescription.updatedFields.address").exists(true)))) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.fullDocumentLookup(FullDocument.UPDATE_LOOKUP).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id)))
				.apply(Update.update("address", new Address("candy ave"))).first();

		template.update(User.class).matching(query(where("id").is(sugarSplashy.id))).apply(new Update().inc("age", 1))
				.first();

		template.update(User.class).matching(query(where("id").is(huffyFluffy.id)))
				.apply(Update.update("address", new Address("flower street"))).first();

		awaitMessages(messageListener);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2);
	}

	@Test // GH-4187
	@EnableIfMongoServerVersion(isGreaterThanEqual = "6.0")
	void readsFullDocumentBeforeChangeWhenOptionDeclaredWhenAvailable() throws InterruptedException {

		createUserCollectionWithChangeStreamPreAndPostImagesEnabled();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentLookup(FullDocument.WHEN_AVAILABLE) //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.WHEN_AVAILABLE) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(jellyBelly);

		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isEqualTo(jellyBelly);
		assertThat(messageListener.getLastMessage().getBody()).isEqualTo(jellyBelly.withAge(8));
	}

	@Test // GH-4187
	@EnableIfMongoServerVersion(isGreaterThanEqual = "6.0")
	void readsFullDocumentBeforeChangeWhenOptionDeclaredRequired() throws InterruptedException {

		createUserCollectionWithChangeStreamPreAndPostImagesEnabled();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentLookup(FullDocument.WHEN_AVAILABLE) //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.REQUIRED) //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(jellyBelly);

		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isEqualTo(jellyBelly);
		assertThat(messageListener.getLastMessage().getBody()).isEqualTo(jellyBelly.withAge(8));
	}

	@Test // GH-4187
	@EnableIfMongoServerVersion(isGreaterThanEqual = "6.0")
	void readsFullDocumentBeforeChangeWhenOptionIsNotDeclared() throws InterruptedException {

		createUserCollectionWithChangeStreamPreAndPostImagesEnabled();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isNull();
	}

	@Test // GH-4187
	@EnableIfMongoServerVersion(isGreaterThanEqual = "6.0")
	void readsFullDocumentBeforeChangeWhenOptionDeclaredDefault() throws InterruptedException {

		createUserCollectionWithChangeStreamPreAndPostImagesEnabled();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.DEFAULT).maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isNull();
	}

	@Test // GH-4187
	@EnableIfMongoServerVersion(isGreaterThanEqual = "6.0")
	void readsFullDocumentBeforeChangeWhenOptionDeclaredOff() throws InterruptedException {

		createUserCollectionWithChangeStreamPreAndPostImagesEnabled();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.OFF).maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isNull();
	}

	@Test // GH-4187
	@EnableIfMongoServerVersion(isGreaterThanEqual = "6.0")
	void readsFullDocumentBeforeChangeWhenOptionDeclaredWhenAvailableAndChangeStreamPreAndPostImagesDisabled()
			throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.WHEN_AVAILABLE).maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isNull();
	}

	@Test // GH-4187
	@Disabled("Flakey test failing occasionally due to timing issues")
	void readsFullDocumentBeforeChangeWhenOptionDeclaredRequiredAndMongoVersionIsLessThan6() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.REQUIRED).maxAwaitTime(Duration.ofMillis(10)) //
				.publishTo(messageListener).build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBodyBeforeChange()).isNull();
		assertThat(messageListener.getLastMessage().getBodyBeforeChange()).isNull();
	}

	private void createUserCollectionWithChangeStreamPreAndPostImagesEnabled() {
		template.createCollection(User.class, CollectionOptions.emitChangedRevisions());
	}

	static class User {

		@Id String id;
		@Field("user_name") String userName;
		int age;

		Address address;

		User withAge(int age) {

			User user = new User();
			user.id = id;
			user.userName = userName;
			user.age = age;

			return user;
		}

		public String getId() {
			return this.id;
		}

		public String getUserName() {
			return this.userName;
		}

		public int getAge() {
			return this.age;
		}

		public Address getAddress() {
			return this.address;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public void setAddress(Address address) {
			this.address = address;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			User user = (User) o;
			return age == user.age && Objects.equals(id, user.id) && Objects.equals(userName, user.userName)
					&& Objects.equals(address, user.address);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, userName, age, address);
		}

		public String toString() {
			return "ChangeStreamTests.User(id=" + this.getId() + ", userName=" + this.getUserName() + ", age=" + this.getAge()
					+ ", address=" + this.getAddress() + ")";
		}
	}

	static class Address {

		@Field("s") String street;

		public Address(String street) {
			this.street = street;
		}

		public Address() {}

		public String getStreet() {
			return this.street;
		}

		public void setStreet(String street) {
			this.street = street;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Address address = (Address) o;
			return Objects.equals(street, address.street);
		}

		@Override
		public int hashCode() {
			return Objects.hash(street);
		}

		public String toString() {
			return "ChangeStreamTests.Address(street=" + this.getStreet() + ")";
		}
	}

}
