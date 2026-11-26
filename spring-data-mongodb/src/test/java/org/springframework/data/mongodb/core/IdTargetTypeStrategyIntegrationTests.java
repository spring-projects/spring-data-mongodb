/*
 * Copyright 2026-present the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Query.*;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.BigDecimalRepresentation;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.IdTargetTypeStrategy;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for {@link IdTargetTypeStrategy}.
 *
 * @author Christoph Strobl
 */
class IdTargetTypeStrategyIntegrationTests {

	static final String DB_NAME = "id-target-type-strategy-tests";
	static final String COLLECTION = "testcol";

	static @Client MongoClient client;

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {
			it.client(client);
			it.defaultDb(DB_NAME);
		});
		cfg.configureConversion(it -> it.customConverters(adapter -> {
			adapter.bigDecimal(BigDecimalRepresentation.DECIMAL128).idTargetTypeStrategy(IdTargetTypeStrategy.IMPLICIT);
		}));
	});

	MongoTestTemplate defaultTemplate = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {
			it.client(client);
			it.defaultDb(DB_NAME);
		});
		cfg.configureConversion(it -> it.customConverters(adapter -> {
			adapter.bigDecimal(BigDecimalRepresentation.DECIMAL128);
		}));
	});

	@AfterEach
	void cleanUp() {
		template.flush(COLLECTION);
		template.flush(PlainId.class, ExplicitObjectId.class, GeneratedId.class, BigIntegerId.class,
				MongoIdBigIntegerId.class, MongoIdTarget.class, PointerReferrer.class, GeneratedBigIntegerId.class,
				LocalDateTimeId.class, MongoIdLocalDateTimeId.class);
	}

	@Test // GH-4466
	void retainsHexStringIdOfPlainDocumentInQueries() {

		ObjectId objectId = ObjectId.get();
		template.insert(new Document("_id", objectId.toHexString()), COLLECTION);

		assertThat(template.count(query(where("_id").is(objectId)), COLLECTION)).isZero();
		assertThat(template.count(query(where("_id").is(objectId.toHexString())), COLLECTION)).isOne();
		assertThat(template.updateFirst(query(where("_id").is(objectId.toHexString())), new Update().set("foo", "bar"),
				COLLECTION).getModifiedCount()).isOne();
	}

	@Test // GH-4466
	void convertsHexStringIdOfPlainDocumentInQueriesByDefault() {

		ObjectId objectId = ObjectId.get();
		defaultTemplate.insert(new Document("_id", objectId.toHexString()), COLLECTION);

		assertThat(defaultTemplate.count(query(where("_id").is(objectId.toHexString())), COLLECTION)).isZero();
	}

	@Test // GH-4466
	void storesHexStringIdOfUnannotatedIdPropertyAsString() {

		PlainId source = new PlainId(ObjectId.get().toHexString());
		template.save(source);

		assertThat(rawId(PlainId.class)).isEqualTo(source.id);
		assertThat(template.findById(source.id, PlainId.class)).isEqualTo(source);
	}

	@Test // GH-4466
	void storesHexStringIdOfExplicitObjectIdPropertyAsObjectId() {

		ExplicitObjectId source = new ExplicitObjectId(ObjectId.get().toHexString());
		template.save(source);

		assertThat(rawId(ExplicitObjectId.class)).isEqualTo(new ObjectId(source.id));
		assertThat(template.findById(source.id, ExplicitObjectId.class)).isEqualTo(source);
	}

	@Test // GH-4466
	void storesGeneratedIdOfUnannotatedIdPropertyAsString() {

		GeneratedId inserted = template.insert(new GeneratedId());
		GeneratedId saved = template.save(new GeneratedId());

		assertThat(inserted.id).isNotNull();
		assertThat(template.findById(inserted.id, GeneratedId.class)).isNotNull();
		assertThat(template.findById(saved.id, GeneratedId.class)).isNotNull();
		assertThat(template.getCollection(template.getCollectionName(GeneratedId.class)).find()
				.map(it -> it.get("_id")).into(new java.util.ArrayList<>())).containsExactlyInAnyOrder(inserted.id, saved.id);
	}

	@Test // GH-4466
	void storesGeneratedIdsOfUnannotatedIdPropertyAsStringInBatch() {

		java.util.Collection<GeneratedId> inserted = template.insert(java.util.List.of(new GeneratedId(), new GeneratedId()),
				GeneratedId.class);

		assertThat(inserted).allSatisfy(it -> {
			assertThat(it.id).isNotNull();
			assertThat(template.findById(it.id, GeneratedId.class)).isNotNull();
		});
	}

	@Test // GH-4466
	void storesGeneratedIdOfUnannotatedIdPropertyAsObjectIdByDefault() {

		GeneratedId inserted = defaultTemplate.insert(new GeneratedId());

		assertThat(rawId(GeneratedId.class)).isEqualTo(new ObjectId(inserted.id));
		assertThat(defaultTemplate.findById(inserted.id, GeneratedId.class)).isNotNull();
	}

	@Test // GH-4466
	void storesImplicitIdWithoutCodecUsingStoreConversion() {

		BigIntegerId source = new BigIntegerId(BigInteger.TEN);
		template.save(source);

		assertThat(rawId(BigIntegerId.class)).isEqualTo(new Decimal128(10));
		assertThat(template.findById(BigInteger.TEN, BigIntegerId.class)).isEqualTo(source);
	}

	@Test // GH-4466
	void storesMongoIdWithoutCodecUsingStoreConversion() {

		MongoIdBigIntegerId source = new MongoIdBigIntegerId(BigInteger.TEN);
		defaultTemplate.save(source);

		assertThat(rawId(MongoIdBigIntegerId.class)).isEqualTo(new Decimal128(10));
		assertThat(defaultTemplate.findById(BigInteger.TEN, MongoIdBigIntegerId.class)).isEqualTo(source);
	}

	@Test // GH-4466
	void storesGeneratedIdsOfUnannotatedIdPropertyAsStringWithBulkOperations() {

		template.bulkOps(BulkMode.ORDERED, GeneratedId.class).insert(List.of(new GeneratedId(), new GeneratedId()))
				.execute();

		assertThat(rawIds(GeneratedId.class)).hasSize(2).allSatisfy(it -> assertThat(it).isInstanceOf(String.class));
	}

	@Test // GH-4466
	@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
	void storesGeneratedIdOfUnannotatedIdPropertyAsStringWithBulkWrite() {

		template.bulkWrite(Bulk.builder().inCollection(GeneratedId.class, ops -> ops.insert(new GeneratedId())).build(),
				BulkWriteOptions.ordered());

		assertThat(rawIds(GeneratedId.class)).singleElement().isInstanceOf(String.class);
	}

	@Test // GH-4466
	void writesDocumentReferenceToMongoIdAsStoredId() {

		String hex = ObjectId.get().toHexString();
		defaultTemplate.save(new MongoIdTarget(hex, "target"));
		defaultTemplate.save(new PointerReferrer("referrer", new MongoIdTarget(hex, "target")));

		assertThat(defaultTemplate.getCollection(defaultTemplate.getCollectionName(PointerReferrer.class))
				.find(new Document("_id", "referrer")).first().get("target")).isEqualTo(hex);
		assertThat(defaultTemplate.findById("referrer", PointerReferrer.class).target())
				.isEqualTo(new MongoIdTarget(hex, "target"));
	}

	@Test // GH-4466
	void storesGeneratedBigIntegerIdWithoutLossOfPrecision() {

		GeneratedBigIntegerId inserted = template.insert(new GeneratedBigIntegerId());

		assertThat(rawId(GeneratedBigIntegerId.class)).isEqualTo(new Decimal128(new java.math.BigDecimal(inserted.id)));
		assertThat(template.findById(inserted.id, GeneratedBigIntegerId.class)).isNotNull();
	}

	@Test // GH-4466
	void roundTripsLocalDateTimeIdInNonUtcZone() {

		LocalDateTime id = LocalDateTime.of(2026, 1, 10, 12, 0);

		inTimeZone("Europe/Vienna", () -> {

			template.save(new LocalDateTimeId(id));
			assertThat(template.findById(id, LocalDateTimeId.class)).isEqualTo(new LocalDateTimeId(id));
		});
	}

	@Test // GH-4466
	void roundTripsLocalDateTimeIdUsingNativeDriverCodecs() {

		LocalDateTime id = LocalDateTime.of(2026, 1, 10, 12, 0);
		MongoTestTemplate nativeCodecs = new MongoTestTemplate(cfg -> {

			cfg.configureDatabaseFactory(it -> {
				it.client(client);
				it.defaultDb(DB_NAME);
			});
			cfg.configureConversion(it -> it.customConverters(adapter -> {
				adapter.useNativeDriverJavaTimeCodecs().idTargetTypeStrategy(IdTargetTypeStrategy.IMPLICIT);
			}));
		});

		inTimeZone("Europe/Vienna", () -> {

			nativeCodecs.save(new LocalDateTimeId(id));
			assertThat(nativeCodecs.findById(id, LocalDateTimeId.class)).isEqualTo(new LocalDateTimeId(id));

			// as stored by @MongoId prior to 5.3
			nativeCodecs.getCollection(nativeCodecs.getCollectionName(MongoIdLocalDateTimeId.class))
					.insertOne(new Document("_id", id));
			assertThat(nativeCodecs.findById(id, MongoIdLocalDateTimeId.class)).isEqualTo(new MongoIdLocalDateTimeId(id));
		});
	}

	private static void inTimeZone(String zone, Runnable runnable) {

		TimeZone current = TimeZone.getDefault();
		try {
			TimeZone.setDefault(TimeZone.getTimeZone(zone));
			runnable.run();
		} finally {
			TimeZone.setDefault(current);
		}
	}

	private List<Object> rawIds(Class<?> type) {
		return template.getCollection(template.getCollectionName(type)).find().map(it -> it.get("_id"))
				.into(new java.util.ArrayList<>());
	}

	private Object rawId(Class<?> type) {
		return template.getCollection(template.getCollectionName(type)).find().first().get("_id");
	}

	record PlainId(@Id String id) {
	}

	record ExplicitObjectId(@MongoId(FieldType.OBJECT_ID) String id) {
	}

	record BigIntegerId(@Id BigInteger id) {
	}

	record MongoIdBigIntegerId(@MongoId BigInteger id) {
	}

	record MongoIdTarget(@MongoId String id, String name) {
	}

	record PointerReferrer(@Id String id, @DocumentReference MongoIdTarget target) {
	}

	record LocalDateTimeId(@Id LocalDateTime id) {
	}

	record MongoIdLocalDateTimeId(@MongoId LocalDateTime id) {
	}

	static class GeneratedBigIntegerId {
		@Id BigInteger id;
	}

	static class GeneratedId {
		@Id String id;
	}
}
