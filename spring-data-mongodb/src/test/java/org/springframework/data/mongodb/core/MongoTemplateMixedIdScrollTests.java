/*
 * Copyright 2026 the original author or authors.
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertCallback;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Regression tests for keyset scrolling across BSON ID types.
 *
 * @author Priyanshu Gagiya
 */
class MongoTemplateMixedIdScrollTests {

	private static final String COLLECTION = "mixedIds";
	private static final String HEX_ONE = "000000000000000000000001";
	private static final String HEX_TWO = "000000000000000000000002";
	private static final ObjectId OBJECT_ONE = new ObjectId(HEX_ONE);
	private static final ObjectId OBJECT_TWO = new ObjectId(HEX_TWO);

	@Client
	static MongoClient client;

	MongoTestTemplate template = new MongoTestTemplate(client, "mixed-bson-keyset-scrolling-tests");

	@BeforeEach
	void setUp() {
		template.dropCollection(COLLECTION);
	}

	@AfterEach
	void tearDown() {
		template.dropCollection(COLLECTION);
	}

	@Test // GH-5260
	void keysetTraversesMixedIdsInBothDirections() {

		insertIds("A", "B", OBJECT_ONE, OBJECT_TWO);

		assertThat(scrollIds(ScrollPosition.keyset())).containsExactly("A", "B", OBJECT_ONE, OBJECT_TWO);
		assertThat(scrollIds(ScrollPosition.keyset().backward())).containsExactly(OBJECT_ONE, OBJECT_TWO, "A", "B");
	}

	@Test // GH-5260
	void keysetRetainsStoredObjectIdForStringIdProperty() {

		insertIds(OBJECT_ONE, OBJECT_TWO, new ObjectId("000000000000000000000003"));
		Query query = new Query().with(Sort.by("id")).limit(2).with(ScrollPosition.keyset());
		Window<StringId> first = template.scroll(query, StringId.class, COLLECTION);

		assertThat(first.hasNext()).isTrue();
		Window<StringId> second = template.scroll(query.with(first.positionAt(1)), StringId.class, COLLECTION);

		assertThat(second).extracting(value -> {
			return value.id;
		}).containsExactly("000000000000000000000003");
	}

	@Test // GH-5260
	void keysetPreservesDistinctBsonTypesWithTheSameStringId() {

		template.getCollection(COLLECTION).insertMany(List.of(new Document("_id", HEX_ONE).append("kind", "string"),
				new Document("_id", OBJECT_ONE).append("kind", "objectId")));
		Query query = new Query().with(Sort.by("id")).limit(1).with(ScrollPosition.keyset());
		Window<StringId> first = template.scroll(query, StringId.class, COLLECTION);
		Window<StringId> second = template.scroll(query.with(first.positionAt(0)), StringId.class, COLLECTION);

		assertThat(first).extracting("kind").containsExactly("string");
		assertThat(second).extracting("kind").containsExactly("objectId");

		first = template.scroll(query.with(ScrollPosition.keyset().backward()), StringId.class, COLLECTION);
		second = template.scroll(query.with(first.positionAt(0)), StringId.class, COLLECTION);

		assertThat(first).extracting("kind").containsExactly("objectId");
		assertThat(second).extracting("kind").containsExactly("string");
	}

	@Test // GH-5260
	void keysetIgnoresSortKeyChangesFromCallbacks() {

		template.getCollection(COLLECTION).insertMany(List.of(new Document("_id", "A").append("date", new Date(1)),
				new Document("_id", "B").append("date", new Date(2))));
		template
				.setEntityCallbacks(EntityCallbacks.create((AfterConvertCallback<MixedId>) (entity, document, collection) -> {
					entity.id = "changed";
					entity.date.setTime(0);
					document.put("_id", "changed");
					return entity;
				}));
		Query query = new Query().with(Sort.by("date")).limit(1).with(ScrollPosition.keyset());
		Window<MixedId> first = template.scroll(query, MixedId.class, COLLECTION);
		Window<MixedId> second = template.scroll(query.with(first.positionAt(0)), MixedId.class, COLLECTION);

		assertThat(((KeysetScrollPosition) first.positionAt(0)).getKeys()).containsEntry("id", "A").containsEntry("date",
				new Date(1));
		assertThat(second).hasSize(1);
		assertThat(((KeysetScrollPosition) second.positionAt(0)).getKeys()).containsEntry("id", "B").containsEntry("date",
				new Date(2));
	}

	@Test // GH-5260
	void keysetUsesDatabaseCodecRegistryWithSuppliedConverter() {

		SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(client, template.getDb().getName()) {
			@Override
			protected MongoDatabase doGetMongoDatabase(String dbName) {
				MongoDatabase database = super.doGetMongoDatabase(dbName);
				return database.withCodecRegistry(
						CodecRegistries.withUuidRepresentation(database.getCodecRegistry(), UuidRepresentation.STANDARD));
			}
		};
		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				template.getConverter().getMappingContext());
		converter.afterPropertiesSet();
		MongoTemplate uuidTemplate = new MongoTemplate(factory, converter);
		UUID firstId = new UUID(0, 1);
		UUID secondId = new UUID(0, 2);
		uuidTemplate.getCollection(COLLECTION).insertMany(List.of(new Document("_id", firstId), new Document("_id", secondId)));
		Query query = new Query().with(Sort.by("id")).limit(1).with(ScrollPosition.keyset());
		Window<MixedId> first = uuidTemplate.scroll(query, MixedId.class, COLLECTION);
		Window<MixedId> second = uuidTemplate.scroll(query.with(first.positionAt(0)), MixedId.class, COLLECTION);

		assertThat(((KeysetScrollPosition) first.positionAt(0)).getKeys()).containsEntry("id", firstId);
		assertThat(second).extracting("id").containsExactly(secondId);
	}

	@Test // GH-5260
	void keysetTreatsDollarPrefixedIdsAsLiterals() {

		insertIds("$A", "$B", "$C");

		assertThat(scrollIds(ScrollPosition.keyset())).containsExactly("$A", "$B", "$C");
	}

	@Test // GH-5260
	void keysetMapsSortFieldsAndRetainsStoredValues() {

		template.getCollection(COLLECTION).insertMany(
				List.of(new Document("_id", "A").append("rank", "10"), new Document("_id", "B").append("rank", "2")));
		Query query = new Query().with(Sort.by(Sort.Direction.DESC, "priority")).limit(1).with(ScrollPosition.keyset());
		Window<MappedSort> first = template.scroll(query, MappedSort.class, COLLECTION);
		Window<MappedSort> second = template.scroll(query.with(first.positionAt(0)), MappedSort.class, COLLECTION);

		assertThat(first).extracting("id").containsExactly("B");
		assertThat(second).extracting("id").containsExactly("A");
	}

	private void insertIds(Object... ids) {

		List<Document> documents = new ArrayList<>();
		for (Object id : ids) {
			documents.add(new Document("_id", id));
		}
		template.getCollection(COLLECTION).insertMany(documents);
	}

	private List<Object> scrollIds(KeysetScrollPosition position) {

		List<Object> result = new ArrayList<>();
		Query query = new Query().with(Sort.by("id")).limit(2).with(position);
		for (int page = 0; page < 10; page++) {
			Window<MixedId> window = template.scroll(query, MixedId.class, COLLECTION);
			for (MixedId value : window) {
				result.add(value.id);
			}
			if (!window.hasNext()) {
				return result;
			}
			query.with(window.positionAt(position.scrollsBackward() ? 0 : window.size() - 1));
		}
		throw new AssertionError("Scrolling did not terminate within ten pages");
	}

	static class MixedId {
		@MongoId
		Object id;
		Date date;
	}

	static class MappedSort {
		@MongoId
		Object id;
		@Field(value = "rank", targetType = FieldType.STRING)
		Integer priority;
	}

	static class StringId {
		@Id
		String id;
		String kind;
	}
}
