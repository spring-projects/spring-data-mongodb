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
package org.springframework.data.mongodb.core.messaging;

import static org.mockito.Mockito.*;

import java.util.UUID;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

/**
 * Unit tests for {@link ChangeStreamTask}.
 *
 * @author Christoph Strobl
 * @author Myroslav Kosinskyi
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
class ChangeStreamTaskUnitTests {

	@Mock MongoTemplate template;
	@Mock MongoDatabase mongoDatabase;
	@Mock MongoCollection<Document> mongoCollection;
	@Mock ChangeStreamIterable<Document> changeStreamIterable;

	MongoConverter converter;

	@BeforeEach
	void setUp() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);

		when(template.getConverter()).thenReturn(converter);
		when(template.getDb()).thenReturn(mongoDatabase);

		when(mongoDatabase.getCollection(any())).thenReturn(mongoCollection);
		when(mongoCollection.watch(eq(Document.class))).thenReturn(changeStreamIterable);
		when(changeStreamIterable.fullDocument(any())).thenReturn(changeStreamIterable);
	}

	@Test // DATAMONGO-2258
	void shouldNotBreakLovelaceBehavior() {

		BsonDocument resumeToken = new BsonDocument("token", new BsonString(UUID.randomUUID().toString()));
		when(changeStreamIterable.resumeAfter(any())).thenReturn(changeStreamIterable);

		ChangeStreamRequest request = ChangeStreamRequest.builder() //
				.collection("start-wars") //
				.resumeToken(resumeToken) //
				.publishTo(message -> {}) //
				.build();

		initTask(request, Document.class);

		verify(changeStreamIterable).resumeAfter(resumeToken);
	}

	@Test // DATAMONGO-2258
	void shouldApplyResumeAfterToChangeStream() {

		when(changeStreamIterable.resumeAfter(any())).thenReturn(changeStreamIterable);

		BsonDocument resumeToken = new BsonDocument("token", new BsonString(UUID.randomUUID().toString()));

		ChangeStreamRequest request = ChangeStreamRequest.builder() //
				.collection("start-wars") //
				.resumeAfter(resumeToken) //
				.publishTo(message -> {}) //
				.build();

		initTask(request, Document.class);

		verify(changeStreamIterable).resumeAfter(resumeToken);
	}

	@Test // DATAMONGO-2258
	void shouldApplyStartAfterToChangeStream() {

		when(changeStreamIterable.startAfter(any())).thenReturn(changeStreamIterable);

		BsonDocument resumeToken = new BsonDocument("token", new BsonString(UUID.randomUUID().toString()));

		ChangeStreamRequest request = ChangeStreamRequest.builder() //
				.collection("start-wars") //
				.startAfter(resumeToken) //
				.publishTo(message -> {}) //
				.build();

		initTask(request, Document.class);

		verify(changeStreamIterable).startAfter(resumeToken);
	}

	@Test // GH-4495
	void shouldApplyFullDocumentBeforeChangeToChangeStream() {

		when(changeStreamIterable.fullDocumentBeforeChange(any())).thenReturn(changeStreamIterable);

		ChangeStreamRequest request = ChangeStreamRequest.builder() //
				.collection("start-wars") //
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.REQUIRED) //
				.publishTo(message -> {}) //
				.build();

		initTask(request, Document.class);

		verify(changeStreamIterable).fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED);
	}

	private MongoCursor<ChangeStreamDocument<Document>> initTask(ChangeStreamRequest request, Class<?> targetType) {

		ChangeStreamTask task = new ChangeStreamTask(template, request, targetType, er -> {});
		return task.initCursor(template, request.getRequestOptions(), targetType);
	}
}
