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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MappingMongoConverterUnitTests.ComplexId;
import org.springframework.data.mongodb.core.convert.MappingMongoConverterUnitTests.ComplexIdAndIdAnnotation;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.BigDecimalRepresentation;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.IdTargetTypeStrategy;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Unit tests for {@link IdTargetTypeStrategy} usage.
 *
 * @author Christoph Strobl
 */
class IdTargetTypeStrategyUnitTests {

	static final String HEX = new ObjectId().toHexString();

	@ParameterizedTest(name = "[{index}] {1} -> {2}") // GH-4466
	@MethodSource("idFieldTypes")
	void resolvesIdFieldType(@Nullable IdTargetTypeStrategy strategy, Class<?> entityType, Class<?> expected) {
		assertThat(new Fixture(strategy).idFieldType(entityType)).isEqualTo(expected);
	}

	static Stream<Arguments> idFieldTypes() {

		return Stream.of( //
				// default strategy
				Arguments.of(null, AnnotatedId.class, ObjectId.class), //
				Arguments.of(null, ConventionalId.class, ObjectId.class), //
				// IMPLICIT uses the property type
				Arguments.of(IdTargetTypeStrategy.IMPLICIT, AnnotatedId.class, String.class), //
				Arguments.of(IdTargetTypeStrategy.IMPLICIT, ConventionalId.class, String.class), //
				Arguments.of(IdTargetTypeStrategy.IMPLICIT, ComplexIdAndIdAnnotation.class, Document.class), //
				// annotated properties are not subject to the strategy
				Arguments.of(IdTargetTypeStrategy.IMPLICIT, ExplicitObjectId.class, ObjectId.class), //
				Arguments.of((IdTargetTypeStrategy) property -> Long.class, ExplicitImplicitId.class, String.class));
	}

	@Test // GH-4466
	void customStrategyReceivesIdProperty() {

		IdTargetTypeStrategy strategy = property -> property != null && property.getOwner().getType() == AnnotatedId.class
				? String.class
				: ObjectId.class;
		Fixture fixture = new Fixture(strategy);

		assertThat(fixture.idFieldType(AnnotatedId.class)).isEqualTo(String.class);
		assertThat(fixture.idFieldType(ConventionalId.class)).isEqualTo(ObjectId.class);
	}

	@Test // GH-4466
	void convertsUntypedHexStringIdToObjectIdByDefault() {

		Document mapped = new Fixture(null).mapper.getMappedObject(query(where("_id").is(HEX)).getQueryObject(),
				(MongoPersistentEntity<?>) null);

		assertThat(mapped.get("_id")).isEqualTo(new ObjectId(HEX));
	}

	@Test // GH-4466
	void retainsUntypedHexStringIdWithImplicitStrategy() {

		QueryMapper mapper = new Fixture(IdTargetTypeStrategy.IMPLICIT).mapper;

		assertThat(mapper.getMappedObject(query(where("_id").is(HEX)).getQueryObject(), (MongoPersistentEntity<?>) null)
				.get("_id")).isEqualTo(HEX);
		assertThat(mapper.getMappedObject(query(where("_id").in(HEX)).getQueryObject(), (MongoPersistentEntity<?>) null))
				.isEqualTo(new Document("_id", new Document("$in", List.of(HEX))));
		assertThat(mapper.convertId(HEX)).isEqualTo(HEX);
	}

	@Test // GH-4466
	void retainsHexStringIdOfUnannotatedPropertyWithImplicitStrategy() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);

		assertThat(fixture.mapper.getMappedObject(query(where("id").is(HEX)).getQueryObject(),
				fixture.context.getPersistentEntity(AnnotatedId.class)).get("_id")).isEqualTo(HEX);
	}

	@Test // GH-4466
	void convertsHexStringIdOfExplicitObjectIdPropertyWithImplicitStrategy() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);

		assertThat(fixture.mapper.getMappedObject(query(where("id").is(HEX)).getQueryObject(),
				fixture.context.getPersistentEntity(ExplicitObjectId.class)).get("_id")).isEqualTo(new ObjectId(HEX));
	}

	@Test // GH-4466
	void retainsDbRefHexStringIdWithImplicitStrategy() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);
		com.mongodb.DBRef dbRef = new com.mongodb.DBRef("collection", HEX);

		Document mapped = fixture.mapper.getMappedObject(query(where("ref").is(dbRef)).getQueryObject(),
				fixture.context.getPersistentEntity(WithDbRef.class));

		assertThat(((com.mongodb.DBRef) mapped.get("ref")).getId()).isEqualTo(HEX);
	}

	@Test // GH-4466
	void writesHexStringIdAsObjectIdByDefault() {
		assertThat(new Fixture(null).write(new AnnotatedId(HEX)).get("_id")).isEqualTo(new ObjectId(HEX));
	}

	@Test // GH-4466
	void writesHexStringIdAsStringWithImplicitStrategy() {
		assertThat(new Fixture(IdTargetTypeStrategy.IMPLICIT).write(new AnnotatedId(HEX)).get("_id")).isEqualTo(HEX);
	}

	@Test // GH-4466
	void writesDocumentReferenceToMongoIdUsingTargetIdType() {

		Document document = new Fixture(IdTargetTypeStrategy.IMPLICIT)
				.write(new Referrer(null, new ExplicitImplicitId(HEX)));

		assertThat(document.get("annotated")).isEqualTo(HEX);
	}

	@Test // GH-4466
	void appliesStoreConversionToImplicitIdWithoutCodec() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);

		assertThat(fixture.write(new BigIntegerId(BigInteger.TEN)).get("_id")).isEqualTo(new Decimal128(10));
		assertThat(fixture.mapper.getMappedObject(query(where("id").is(BigInteger.TEN)).getQueryObject(),
				fixture.context.getPersistentEntity(BigIntegerId.class)).get("_id")).isEqualTo(new Decimal128(10));
	}

	@Test // GH-4466
	void appliesStoreConversionToMongoIdWithoutCodec() {

		Fixture fixture = new Fixture(null);

		assertThat(fixture.write(new MongoIdBigIntegerId(BigInteger.TEN)).get("_id")).isEqualTo(new Decimal128(10));
		assertThat(fixture.mapper.getMappedObject(query(where("id").is(BigInteger.TEN)).getQueryObject(),
				fixture.context.getPersistentEntity(MongoIdBigIntegerId.class)).get("_id")).isEqualTo(new Decimal128(10));
	}

	@Test // GH-4466
	void writesImplicitIdUsingRegularWriteConversion() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);
		Instant instant = Instant.ofEpochMilli(1000);

		assertThat(fixture.write(new InstantId(instant)).get("_id")).isEqualTo(Date.from(instant));
		assertThat(fixture.write(new EnumId(Color.RED)).get("_id")).isEqualTo("RED");
	}

	@Test // GH-4466
	void preservesExplicitBsonTargetTypeDespiteCustomWriteConverter() {

		MongoCustomConversions conversions = new MongoCustomConversions(new MongoConverterConfigurationAdapter()
				.registerConverter(ObjectIdToStringConverter.INSTANCE).idTargetTypeStrategy(IdTargetTypeStrategy.IMPLICIT));
		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.setIdTargetTypeStrategy(conversions.getIdTargetTypeStrategy());
		context.afterPropertiesSet();
		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		Document written = new Document();
		converter.write(new ExplicitObjectId(HEX), written);
		Document mapped = new QueryMapper(converter).getMappedObject(query(where("id").is(HEX)).getQueryObject(),
				context.getPersistentEntity(ExplicitObjectId.class));

		assertThat(written.get("_id")).isEqualTo(new ObjectId(HEX));
		assertThat(mapped.get("_id")).isEqualTo(new ObjectId(HEX));
	}

	@WritingConverter
	enum ObjectIdToStringConverter implements Converter<ObjectId, String> {

		INSTANCE;

		@Override
		public String convert(ObjectId source) {
			return source.toHexString();
		}
	}

	@Test // GH-4466
	void writesImplicitObjectTypedIdUsingRegularWriteConversion() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);

		assertThat(fixture.write(new ObjectTypedId(ComplexId.of(1L))).get("_id")).isEqualTo(new Document("innerId", 1L));
		assertThat(fixture.mapper.getMappedObject(query(where("id").is(ComplexId.of(1L))).getQueryObject(),
				fixture.context.getPersistentEntity(ObjectTypedId.class)).get("_id")).isEqualTo(new Document("innerId", 1L));
	}

	@Test // GH-4466
	void appliesWriteConversionToOperandsOfNonIdOperators() {

		Fixture fixture = new Fixture(null);

		assertThat(fixture.mapper.getMappedObject(query(where("id").mod(BigInteger.TEN, BigInteger.ONE)).getQueryObject(),
				fixture.context.getPersistentEntity(LongId.class)))
				.isEqualTo(new Document("_id", new Document("$mod", List.of(new Decimal128(10), new Decimal128(1)))));
	}

	@Test // GH-4466
	void appliesStoreConversionToDocumentReferenceIdWithoutCodec() {

		assertThat(new Fixture(IdTargetTypeStrategy.IMPLICIT)
				.write(new BigIntegerReferrer(new BigIntegerId(BigInteger.TEN), null)).get("target"))
				.isEqualTo(new Decimal128(10));
		assertThat(
				new Fixture(null).write(new BigIntegerReferrer(null, new MongoIdBigIntegerId(BigInteger.TEN))).get("annotated"))
				.isEqualTo(new Decimal128(10));
	}

	@Test // GH-4466
	void convertsDbRefIdUsingReferencedIdPropertyWithImplicitStrategy() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);
		ExplicitObjectId target = new ExplicitObjectId(HEX);

		Document written = fixture.write(new WithExplicitObjectIdDbRef("1", target));
		Document mapped = fixture.mapper.getMappedObject(
				query(where("ref").is(new com.mongodb.DBRef("explicitObjectId", HEX))).getQueryObject(),
				fixture.context.getPersistentEntity(WithExplicitObjectIdDbRef.class));

		assertThat(((com.mongodb.DBRef) written.get("ref")).getId()).isEqualTo(new ObjectId(HEX));
		assertThat(((com.mongodb.DBRef) mapped.get("ref")).getId()).isEqualTo(new ObjectId(HEX));
	}

	@Test // GH-4466
	void convertsDbRefIdUsingReferencedMongoIdProperty() {

		Fixture fixture = new Fixture(null);

		Document written = fixture.write(new WithMongoIdDbRef("1", new ExplicitImplicitId(HEX)));
		Document mapped = fixture.mapper.getMappedObject(
				query(where("ref").is(new com.mongodb.DBRef("explicitImplicitId", HEX))).getQueryObject(),
				fixture.context.getPersistentEntity(WithMongoIdDbRef.class));

		assertThat(((com.mongodb.DBRef) written.get("ref")).getId()).isEqualTo(HEX);
		assertThat(((com.mongodb.DBRef) mapped.get("ref")).getId()).isEqualTo(HEX);
	}

	@Test // GH-4466
	void convertsDbRefIdsInIterableOperandsLikeScalarOperands() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);
		com.mongodb.DBRef dbRef = new com.mongodb.DBRef("explicitObjectId", HEX);
		MongoPersistentEntity<?> entity = fixture.context.getPersistentEntity(WithExplicitObjectIdDbRef.class);

		Document is = fixture.mapper.getMappedObject(query(where("ref").is(dbRef)).getQueryObject(), entity);
		Document in = fixture.mapper.getMappedObject(query(where("ref").in(dbRef)).getQueryObject(), entity);

		assertThat(((com.mongodb.DBRef) is.get("ref")).getId()).isEqualTo(new ObjectId(HEX));
		assertThat((List<Object>) in.get("ref", Document.class).get("$in"))
				.containsExactly(new com.mongodb.DBRef("explicitObjectId", new ObjectId(HEX)));
	}

	@Test // GH-4466
	void retainsControlArgumentsOfIdOperators() {

		Fixture fixture = new Fixture(IdTargetTypeStrategy.IMPLICIT);
		MongoPersistentEntity<?> entity = fixture.context.getPersistentEntity(AnnotatedId.class);

		assertThat(fixture.mapper.getMappedObject(query(where("id").exists(false)).getQueryObject(), entity))
				.isEqualTo(new Document("_id", new Document("$exists", false)));
		assertThat(fixture.mapper.getMappedObject(query(where("id").type(2)).getQueryObject(), entity))
				.isEqualTo(new Document("_id", new Document("$type", 2)));
	}

	@Test // GH-4466
	void convertsOperandsOfIdComparisonOperators() {

		Fixture fixture = new Fixture(null);
		MongoPersistentEntity<?> entity = fixture.context.getPersistentEntity(AnnotatedId.class);

		assertThat(fixture.mapper.getMappedObject(query(where("id").ne(HEX).gt(HEX)).getQueryObject(), entity))
				.isEqualTo(new Document("_id", new Document("$ne", new ObjectId(HEX)).append("$gt", new ObjectId(HEX))));
		assertThat(fixture.mapper.getMappedObject(query(where("id").not().in(HEX)).getQueryObject(), entity))
				.isEqualTo(new Document("_id", new Document("$not", new Document("$in", List.of(new ObjectId(HEX))))));
	}

	@ParameterizedTest(name = "{0} {1}") // GH-4466
	@MethodSource("documentReferenceTargets")
	void writesDocumentReferencePointerEqualToStoredId(String strategy, String field, Object target) {

		Fixture fixture = new Fixture("IMPLICIT".equals(strategy) ? IdTargetTypeStrategy.IMPLICIT : null);

		Object storedId = fixture.write(target).get("_id");
		Object pointer = fixture.write(DocumentReferences.of(field, target)).get(field);

		assertThat(pointer).isEqualTo(storedId);
	}

	static Stream<Arguments> documentReferenceTargets() {

		List<Arguments> targets = List.of( //
				Arguments.of("string", new AnnotatedId(HEX)), //
				Arguments.of("string", new AnnotatedId("not-an-object-id")), //
				Arguments.of("bigInteger", new BigIntegerId(BigInteger.TEN)), //
				Arguments.of("entity", complexId(1L)), //
				Arguments.of("instant", new InstantId(Instant.ofEpochMilli(1000))), //
				Arguments.of("uuid", new UuidId(UUID.fromString("11111111-2222-3333-4444-555555555555"))), //
				Arguments.of("longId", new LongId(42L)), //
				Arguments.of("mongoIdString", new ExplicitImplicitId(HEX)), //
				Arguments.of("mongoIdObjectId", new ExplicitObjectId(HEX)), //
				Arguments.of("mongoIdObjectId", new ExplicitObjectId("not-an-object-id")), //
				Arguments.of("mongoIdBigInteger", new MongoIdBigIntegerId(BigInteger.TEN)));

		return Stream.of("DEFAULT", "IMPLICIT")
				.flatMap(strategy -> targets.stream().map(it -> Arguments.of(strategy, it.get()[0], it.get()[1])));
	}

	private static ComplexIdAndIdAnnotation complexId(long value) {

		ComplexIdAndIdAnnotation entity = new ComplexIdAndIdAnnotation();
		entity.id = ComplexId.of(value);
		return entity;
	}

	static class Fixture {

		final MongoMappingContext context;
		final MappingMongoConverter converter;
		final QueryMapper mapper;

		Fixture(IdTargetTypeStrategy strategy) {

			MongoConverterConfigurationAdapter adapter = new MongoConverterConfigurationAdapter()
					.bigDecimal(BigDecimalRepresentation.DECIMAL128);
			if (strategy != null) {
				adapter.idTargetTypeStrategy(strategy);
			}
			MongoCustomConversions conversions = new MongoCustomConversions(adapter);

			context = new MongoMappingContext();
			context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
			context.setIdTargetTypeStrategy(conversions.getIdTargetTypeStrategy());
			context.afterPropertiesSet();

			converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
			converter.setCustomConversions(conversions);
			converter.afterPropertiesSet();

			mapper = new QueryMapper(converter);
		}

		Class<?> idFieldType(Class<?> type) {

			MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(type);
			MongoPersistentProperty idProperty = entity.getRequiredIdProperty();
			return idProperty.getFieldType();
		}

		Document write(Object source) {

			Document document = new Document();
			converter.write(source, document);
			return document;
		}
	}

	record AnnotatedId(@Id String id) {
	}

	record ConventionalId(String id) {
	}

	record ExplicitObjectId(@MongoId(FieldType.OBJECT_ID) String id) {
	}

	record ExplicitImplicitId(@MongoId String id) {
	}

	record WithDbRef(@Id String id, @DBRef AnnotatedId ref) {
	}

	record BigIntegerId(@Id BigInteger id) {
	}

	record MongoIdBigIntegerId(@MongoId BigInteger id) {
	}

	record InstantId(@Id Instant id) {
	}

	record BigIntegerReferrer(@DocumentReference BigIntegerId target, @DocumentReference MongoIdBigIntegerId annotated) {
	}

	record UuidId(@Id UUID id) {
	}

	enum Color {
		RED
	}

	record EnumId(@Id Color id) {
	}

	record ObjectTypedId(@Id Object id) {
	}

	record LongId(@Id Long id) {
	}

	record WithExplicitObjectIdDbRef(@Id String id, @DBRef ExplicitObjectId ref) {
	}

	record WithMongoIdDbRef(@Id String id, @DBRef ExplicitImplicitId ref) {
	}

	record DocumentReferences(@DocumentReference AnnotatedId string, @DocumentReference BigIntegerId bigInteger,
			@DocumentReference ComplexIdAndIdAnnotation entity, @DocumentReference InstantId instant,
			@DocumentReference UuidId uuid, @DocumentReference LongId longId,
			@DocumentReference ExplicitImplicitId mongoIdString, @DocumentReference ExplicitObjectId mongoIdObjectId,
			@DocumentReference MongoIdBigIntegerId mongoIdBigInteger) {

		static DocumentReferences of(String field, Object target) {
			return new DocumentReferences(field.equals("string") ? (AnnotatedId) target : null,
					field.equals("bigInteger") ? (BigIntegerId) target : null,
					field.equals("entity") ? (ComplexIdAndIdAnnotation) target : null,
					field.equals("instant") ? (InstantId) target : null, field.equals("uuid") ? (UuidId) target : null,
					field.equals("longId") ? (LongId) target : null,
					field.equals("mongoIdString") ? (ExplicitImplicitId) target : null,
					field.equals("mongoIdObjectId") ? (ExplicitObjectId) target : null,
					field.equals("mongoIdBigInteger") ? (MongoIdBigIntegerId) target : null);
		}
	}

	record Referrer(@DocumentReference AnnotatedId target, @DocumentReference ExplicitImplicitId annotated) {
	}
}
