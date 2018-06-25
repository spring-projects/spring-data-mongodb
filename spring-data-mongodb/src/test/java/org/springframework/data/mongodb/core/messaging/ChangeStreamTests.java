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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.messaging.SubscriptionUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.messaging.ChangeStreamTask.ChangeStreamEventMessage;
import org.springframework.data.mongodb.core.messaging.Message.MessageProperties;
import org.springframework.data.mongodb.core.messaging.SubscriptionUtils.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.ReplicaSet;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * Integration test for subscribing to a {@link com.mongodb.operation.ChangeStreamBatchCursor} inside the
 * {@link DefaultMessageListenerContainer} using {@link ChangeStreamRequest}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ChangeStreamTests {

	public static @ClassRule TestRule replSet = ReplicaSet.required();

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

		template = new MongoTemplate(MongoTestUtils.replSetClient(), "change-stream-tests");
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

	@After
	public void tearDown() {
		container.stop();
	}

	@AfterClass
	public static void afterClass() {
		executor.shutdown();
	}

	@Test // DATAMONGO-1803
	public void readsPlainDocumentMessageCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(messageListener, () -> "user");

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
	public void useSimpleAggregationToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(match(where("age").is(7)))) //
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
	public void useAggregationToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(match(
						new Criteria().orOperator(where("user_name").is("huffyFluffy"), where("user_name").is("jellyBelly"))))) //
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
	public void mapsTypedAggregationToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.publishTo(messageListener) //
				.filter(newAggregation(User.class,
						match(new Criteria().orOperator(where("userName").is("huffyFluffy"), where("userName").is("jellyBelly"))))) //
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
	public void mapsReservedWordsCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.publishTo(messageListener) //
				.filter(newAggregation(User.class, match(where("operationType").is("replace")))) //
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

		awaitMessages(messageListener);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(1).containsExactly(replacement);
	}

	@Test // DATAMONGO-1803
	public void plainAggregationPipelineToFilterMessages() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.publishTo(messageListener) //
				.filter(new Document("$match", new Document("fullDocument.user_name", "sugarSplashy"))) //
				.build();

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener);

		List<User> messageBodies = messageListener.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(1).containsExactly(sugarSplashy);
	}

	@Test // DATAMONGO-1803
	public void resumesCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener1 = new CollectingMessageListener<>();
		Subscription subscription1 = container.register(new ChangeStreamRequest<>(messageListener1, () -> "user"),
				User.class);

		awaitSubscription(subscription1);

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		awaitMessages(messageListener1, 3);

		BsonDocument resumeToken = messageListener1.getFirstMessage().getRaw().getResumeToken();

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener2 = new CollectingMessageListener<>();
		ChangeStreamRequest<User> subSequentRequest = ChangeStreamRequest.builder().collection("user")
				.publishTo(messageListener2).resumeToken(resumeToken).build();

		Subscription subscription2 = container.register(subSequentRequest, User.class);
		awaitSubscription(subscription2);

		awaitMessages(messageListener2);

		List<User> messageBodies = messageListener2.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(jellyBelly);
	}

	@Test // DATAMONGO-1803
	public void readsAndConvertsMessageBodyCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = new ChangeStreamRequest<>(messageListener, () -> "user");

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
	public void readsAndConvertsUpdateMessageBodyCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = new ChangeStreamRequest<>(messageListener, () -> "user");

		Subscription subscription = container.register(request, User.class);
		awaitSubscription(subscription);

		template.save(jellyBelly);

		template.update(User.class).matching(query(where("id").is(jellyBelly.id))).apply(Update.update("age", 8)).first();

		awaitMessages(messageListener, 2);

		assertThat(messageListener.getFirstMessage().getBody()).isEqualTo(jellyBelly);
		assertThat(messageListener.getLastMessage().getBody()).isNotNull().hasFieldOrPropertyWithValue("age", 8);
	}

	@Test // DATAMONGO-1803
	public void readsOnlyDiffForUpdateWhenNotMappedToDomainType() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(messageListener, () -> "user");

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
	public void readsOnlyDiffForUpdateWhenOptionsDeclareDefaultExplicitly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentLookup(FullDocument.DEFAULT) //
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
	public void readsFullDocumentForUpdateWhenNotMappedToDomainTypeButLookupSpecified() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, Document> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<Document> request = ChangeStreamRequest.builder() //
				.collection("user") //
				.fullDocumentLookup(FullDocument.UPDATE_LOOKUP) //
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

	@Test // DATAMONGO-2012
	public void resumeAtTimestampCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener1 = new CollectingMessageListener<>();
		Subscription subscription1 = container.register(new ChangeStreamRequest<>(messageListener1, () -> "user"),
				User.class);

		awaitSubscription(subscription1);

		template.save(jellyBelly);
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
				.build();

		Subscription subscription2 = container.register(subSequentRequest, User.class);
		awaitSubscription(subscription2);

		awaitMessages(messageListener2);

		List<User> messageBodies = messageListener2.getMessages().stream().map(Message::getBody)
				.collect(Collectors.toList());

		assertThat(messageBodies).hasSize(2).doesNotContain(jellyBelly);
	}

	@Test // DATAMONGO-1996
	public void filterOnNestedElementWorksCorrectly() throws InterruptedException {

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(User.class, match(where("address.street").is("flower street")))) //
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
	public void filterOnUpdateDescriptionElement() throws InterruptedException {

		template.save(jellyBelly);
		template.save(sugarSplashy);
		template.save(huffyFluffy);

		CollectingMessageListener<ChangeStreamDocument<Document>, User> messageListener = new CollectingMessageListener<>();
		ChangeStreamRequest<User> request = ChangeStreamRequest.builder(messageListener) //
				.collection("user") //
				.filter(newAggregation(User.class, match(where("updateDescription.updatedFields.address").exists(true)))) //
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

	@Data
	static class User {

		@Id String id;
		@Field("user_name") String userName;
		int age;

		Address address;
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Address {

		@Field("s") String street;
	}

}
