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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ConfigurableTypeInformationMapper;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * Tests for enum values held in properties whose declared type does not identify the enum.
 *
 * @author Priyanshu Gagiya
 */
class PolymorphicEnumMappingUnitTests {

	MongoMappingContext mappingContext;
	MappingMongoConverter converter;

	@BeforeEach
	void setUp() {

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		converter.afterPropertiesSet();
	}

	@Test // GH-2786
	void readsEnumThroughInterfaceUsingStoredType() {

		Document source = new Document("eventType",
				new Document("_class", OrderEventType.class.getName()).append("value", "CREATED"));

		assertThat(converter.read(Event.class, source).eventType).isSameAs(OrderEventType.CREATED);
	}

	@Test // GH-2786
	void distinguishesEnumsWithTheSameConstantName() {

		Document source = new Document("eventType",
				new Document("_class", CustomerEventType.class.getName()).append("value", "CREATED"));

		assertThat(converter.read(Event.class, source).eventType).isSameAs(CustomerEventType.CREATED);
	}

	@Test // GH-2786
	void retainsStringRepresentationByDefault() {

		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		assertThat(write(event)).containsEntry("eventType", "CREATED");
	}

	@Test // GH-2786
	void roundTripsInterfaceEnumWhenEnabled() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		Document document = write(event);

		assertThat(document).containsEntry("eventType", orderEventDocument());
		assertThat(converter.read(Event.class, document).eventType).isSameAs(OrderEventType.CREATED);
	}

	@Test // GH-2786
	void roundTripsInterfaceEnumsInContainers() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.events = Arrays.asList(OrderEventType.CREATED, CustomerEventType.CREATED, null);
		event.eventArray = new EventType[] { CustomerEventType.CREATED, OrderEventType.CREATED, null };
		event.eventMap = Map.of("order", OrderEventType.CREATED, "customer", CustomerEventType.CREATED);

		Document document = write(event);
		Event result = converter.read(Event.class, document);

		assertThat(result.events).containsExactlyElementsOf(event.events);
		assertThat(result.eventArray).containsExactly(event.eventArray);
		assertThat(result.eventMap).isEqualTo(event.eventMap);
	}

	@Test // GH-2786
	void roundTripsInterfaceEnumsInNestedContainers() {

		converter.setPreserveEnumTypeInformation(true);
		NestedEvents event = nestedEvents();
		Document document = write(event);
		NestedEvents result = converter.read(NestedEvents.class, document);

		assertThat(result.lists).isEqualTo(event.lists);
		assertThat(result.mapOfLists).isEqualTo(event.mapOfLists);
		assertThat(result.maps).isEqualTo(event.maps);
		assertThat(result.listOfMaps).isEqualTo(event.listOfMaps);
	}

	@Test // GH-2786
	void mapsNestedContainerQueriesAndUpdatesToStoredRepresentation() {

		converter.setPreserveEnumTypeInformation(true);
		NestedEvents event = nestedEvents();
		Document stored = write(event);
		Document source = new Document("lists", event.lists).append("mapOfLists", event.mapOfLists)
				.append("maps", event.maps).append("listOfMaps", event.listOfMaps);
		var entity = mappingContext.getRequiredPersistentEntity(NestedEvents.class);
		Document query = new QueryMapper(converter).getMappedObject(source, entity);
		Document update = new UpdateMapper(converter).getMappedObject(new Document("$set", source), entity);

		for (String field : source.keySet()) {
			assertThat(query).containsEntry(field, stored.get(field));
			assertThat(update.get("$set", Document.class)).containsEntry(field, stored.get(field));
		}
	}

	@Test // GH-2786
	void mapsNestedContainerOperatorsAndDottedPaths() {

		converter.setPreserveEnumTypeInformation(true);
		var entity = mappingContext.getRequiredPersistentEntity(NestedEvents.class);
		Document source = new Document("mapOfLists.order", List.of(OrderEventType.CREATED));
		Document query = new QueryMapper(converter).getMappedObject(source, entity);
		Document update = new UpdateMapper(converter).getMappedObject(new Document("$set", source), entity);
		Document inQuery = new QueryMapper(converter)
				.getMappedObject(new Document("lists", new Document("$in", List.of(List.of(OrderEventType.CREATED)))), entity);
		Document eachUpdate = new UpdateMapper(converter).getMappedObject(
				new Update().push("lists").each(List.of(List.of(OrderEventType.CREATED))).getUpdateObject(), entity);

		assertThat(query).containsEntry("mapOfLists.order", List.of(orderEventDocument()));
		assertThat(update.get("$set", Document.class)).containsEntry("mapOfLists.order", List.of(orderEventDocument()));
		assertThat(inQuery.get("lists", Document.class)).containsEntry("$in", List.of(List.of(orderEventDocument())));
		assertThat(eachUpdate.get("$push", Document.class).get("lists", Document.class)).containsEntry("$each",
				List.of(List.of(orderEventDocument())));
	}

	private NestedEvents nestedEvents() {

		NestedEvents event = new NestedEvents();
		event.lists = List.of(List.of(OrderEventType.CREATED));
		event.mapOfLists = Map.of("order", List.of(OrderEventType.CREATED));
		event.maps = Map.of("outer", Map.of("order", OrderEventType.CREATED));
		event.listOfMaps = List.of(Map.of("order", OrderEventType.CREATED));
		return event;
	}

	@Test // GH-2786
	void retainsConcreteEnumRepresentationWhenEnabled() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.concrete = OrderEventType.CREATED;
		event.concreteList = List.of(OrderEventType.CREATED);
		event.concreteMap = Map.of("order", OrderEventType.CREATED);

		Document document = write(event);

		assertThat(document).containsEntry("concrete", "CREATED").containsEntry("concreteList", List.of("CREATED"))
				.containsEntry("concreteMap", new Document("order", "CREATED"));
		assertThat(converter.read(Event.class, document).concrete).isSameAs(OrderEventType.CREATED);
	}

	@Test // GH-2786
	void usesDeclaringEnumTypeForConstantSpecificClass() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.eventType = SpecializedEventType.CREATED;

		Document document = write(event);

		assertThat(document.get("eventType", Document.class)).containsEntry("_class", SpecializedEventType.class.getName())
				.containsEntry("value", "CREATED");
		assertThat(converter.read(Event.class, document).eventType).isSameAs(SpecializedEventType.CREATED);
	}

	@Test // GH-2786
	void stillMapsNonEnumImplementations() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.eventType = new NamedEventType();

		assertThat(converter.read(Event.class, write(event)).eventType).isInstanceOf(NamedEventType.class);
	}

	@Test // GH-2786
	void honorsCustomTypeKeyAndAlias() {

		converter.setPreserveEnumTypeInformation(true);
		converter.setTypeMapper(new DefaultMongoTypeMapper("kind",
				List.of(new ConfigurableTypeInformationMapper(Map.of(OrderEventType.class, "order")))));
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		Document document = write(event);

		assertThat(document.get("eventType")).isEqualTo(new Document("value", "CREATED").append("kind", "order"));
		assertThat(converter.read(Event.class, document).eventType).isSameAs(OrderEventType.CREATED);
	}

	@Test // GH-2786
	void rejectsMissingTypeInformationOnWrite() {

		converter.setPreserveEnumTypeInformation(true);
		converter.setTypeMapper(new DefaultMongoTypeMapper(null));
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			write(event);
		});
	}

	@Test // GH-2786
	void rejectsTypeKeyCollidingWithEnumValue() {

		converter.setPreserveEnumTypeInformation(true);
		converter.setTypeMapper(new DefaultMongoTypeMapper("value"));
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			write(event);
		});
	}

	@Test // GH-2786
	void rejectsTypeKeyCollisionEvenWhenAliasMatchesConstantName() {

		converter.setPreserveEnumTypeInformation(true);
		converter.setTypeMapper(new DefaultMongoTypeMapper("value",
				List.of(new ConfigurableTypeInformationMapper(Map.of(OrderEventType.class, "CREATED")))));
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			write(event);
		});
	}

	@Test // GH-2786
	void rejectsEnumTypeIncompatibleWithDeclaredProperty() {

		Document source = new Document("concrete",
				new Document("_class", CustomerEventType.class.getName()).append("value", "CREATED"));

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			converter.read(Event.class, source);
		});
	}

	@Test // GH-2786
	void rejectsUnknownEnumConstant() {

		Document source = new Document("eventType",
				new Document("_class", OrderEventType.class.getName()).append("value", "UNKNOWN"));

		assertThatIllegalArgumentException().isThrownBy(() -> {
			converter.read(Event.class, source);
		});
	}

	@Test // GH-2786
	void rejectsMissingOrNonStringEnumNames() {

		Document missing = new Document("eventType", new Document("_class", OrderEventType.class.getName()));
		Document nonString = new Document("eventType",
				new Document("_class", OrderEventType.class.getName()).append("value", 1));

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			converter.read(Event.class, missing);
		});
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			converter.read(Event.class, nonString);
		});
	}

	@Test // GH-2786
	void honorsExplicitStringTarget() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.stringValue = OrderEventType.CREATED;
		event.stringList = List.of(OrderEventType.CREATED);
		event.stringMap = Map.of("order", OrderEventType.CREATED);

		assertThat(write(event)).containsEntry("stringValue", "CREATED").containsEntry("stringList", List.of("CREATED"))
				.containsEntry("stringMap", new Document("order", "CREATED"));
	}

	@Test // GH-2786
	void honorsCustomEnumWriter() {

		converter.setPreserveEnumTypeInformation(true);
		converter.setCustomConversions(new MongoCustomConversions(List.of(new OrderEventWriter())));
		converter.afterPropertiesSet();
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;
		event.events = List.of(OrderEventType.CREATED);
		event.eventMap = Map.of("order", OrderEventType.CREATED);

		assertThat(write(event)).containsEntry("eventType", "custom").containsEntry("events", List.of("custom"))
				.containsEntry("eventMap", new Document("order", "custom"));
	}

	@Test // GH-2786
	void honorsCustomInterfaceReader() {

		converter.setCustomConversions(new MongoCustomConversions(List.of(new EventReader())));
		converter.afterPropertiesSet();

		assertThat(converter.read(Event.class, new Document("eventType", orderEventDocument())).eventType)
				.isSameAs(CustomerEventType.CREATED);
	}

	@Test // GH-2786
	void mapsEqualityAndInQueriesToStoredRepresentation() {

		converter.setPreserveEnumTypeInformation(true);
		QueryMapper mapper = new QueryMapper(converter);
		Query query = Query.query(Criteria.where("eventType").is(OrderEventType.CREATED).and("events")
				.in(OrderEventType.CREATED, CustomerEventType.CREATED));

		Document mapped = mapper.getMappedObject(query.getQueryObject(),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(mapped).containsEntry("eventType", orderEventDocument());
		assertThat(mapped.get("events", Document.class).getList("$in", Document.class)).containsExactly(
				orderEventDocument(), new Document("value", "CREATED").append("_class", CustomerEventType.class.getName()));
	}

	@Test // GH-2786
	void mapsUpdatesToStoredRepresentation() {

		converter.setPreserveEnumTypeInformation(true);
		Update update = new Update().set("eventType", OrderEventType.CREATED).addToSet("eventArray",
				CustomerEventType.CREATED);
		Document mapped = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(mapped.get("$set", Document.class)).containsEntry("eventType", orderEventDocument());
		assertThat(mapped.get("$addToSet", Document.class)).containsEntry("eventArray",
				new Document("value", "CREATED").append("_class", CustomerEventType.class.getName()));
	}

	@Test // GH-2786
	void mapsEachModifierToStoredRepresentation() {

		converter.setPreserveEnumTypeInformation(true);
		Update update = new Update().push("events").each(OrderEventType.CREATED);
		Document mapped = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(mapped.get("$push", Document.class).get("events", Document.class)).containsEntry("$each",
				List.of(orderEventDocument()));
	}

	@Test // GH-2786
	void honorsExplicitStringTargetInQueriesAndEachUpdates() {

		converter.setPreserveEnumTypeInformation(true);
		Query query = Query.query(Criteria.where("stringList").in(OrderEventType.CREATED));
		Document mappedQuery = new QueryMapper(converter).getMappedObject(query.getQueryObject(),
				mappingContext.getRequiredPersistentEntity(Event.class));
		Update update = new Update().push("stringList").each(OrderEventType.CREATED);
		Document mappedUpdate = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(mappedQuery.get("stringList", Document.class)).containsEntry("$in", List.of("CREATED"));
		assertThat(mappedUpdate.get("$push", Document.class).get("stringList", Document.class)).containsEntry("$each",
				List.of("CREATED"));
	}

	@ParameterizedTest // GH-2786
	@ValueSource(booleans = { false, true })
	void keepsNumericUpdateControlsWithExplicitStringTarget(boolean preserveEnumTypeInformation) {

		converter.setPreserveEnumTypeInformation(preserveEnumTypeInformation);
		Update update = new Update().push("stringList").atPosition(0).slice(2).sort(Direction.ASC)
				.each(OrderEventType.CREATED);
		Document mapped = new UpdateMapper(converter).getMappedObject(update.getUpdateObject(),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(mapped.get("$push", Document.class).get("stringList", Document.class)).containsEntry("$position", 0)
				.containsEntry("$slice", 2).containsEntry("$sort", 1).containsEntry("$each", List.of("CREATED"));
	}

	@Test // GH-2786
	void mapsDottedMapValueQueriesAndUpdates() {

		converter.setPreserveEnumTypeInformation(true);
		Document source = new Document("eventMap.order", OrderEventType.CREATED);
		Document query = new QueryMapper(converter).getMappedObject(source,
				mappingContext.getRequiredPersistentEntity(Event.class));
		Document update = new UpdateMapper(converter).getMappedObject(new Document("$set", source),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(query).containsEntry("eventMap.order", orderEventDocument());
		assertThat(update.get("$set", Document.class)).containsEntry("eventMap.order", orderEventDocument());
	}

	@ParameterizedTest // GH-2786
	@ValueSource(booleans = { false, true })
	void retainsNumericDottedMapValuesWithExplicitTarget(boolean preserveEnumTypeInformation) {

		converter.setPreserveEnumTypeInformation(preserveEnumTypeInformation);
		Event event = new Event();
		event.numericMap = Map.of("one", 1);
		Document query = new QueryMapper(converter).getMappedObject(new Document("numericMap.one", 1),
				mappingContext.getRequiredPersistentEntity(Event.class));

		assertThat(query).containsEntry("numericMap.one", write(event).get("numericMap", Document.class).get("one"));
	}

	@Test // GH-2786
	void mapsIndexedListsAndNestedMapKeysWithoutTruncatingPaths() {

		converter.setPreserveEnumTypeInformation(true);
		var entity = mappingContext.getRequiredPersistentEntity(NestedEvents.class);
		Document source = new Document("lists.0.0", OrderEventType.CREATED).append("maps.outer.order",
				OrderEventType.CREATED);
		Document query = new QueryMapper(converter).getMappedObject(source, entity);
		Document update = new UpdateMapper(converter).getMappedObject(new Document("$set", source), entity);

		assertThat(query).containsEntry("lists.0.0", orderEventDocument()).containsEntry("maps.outer.order",
				orderEventDocument());
		assertThat(update.get("$set", Document.class)).isEqualTo(query);
	}

	@Test // GH-2786
	void retainsConcreteEnumQueryAndUpdateRepresentation() {

		converter.setPreserveEnumTypeInformation(true);
		Document source = new Document("concrete", OrderEventType.CREATED);

		assertThat(
				new QueryMapper(converter).getMappedObject(source, mappingContext.getRequiredPersistentEntity(Event.class)))
				.containsEntry("concrete", "CREATED");
		assertThat(new UpdateMapper(converter)
				.getMappedObject(new Document("$set", source), mappingContext.getRequiredPersistentEntity(Event.class))
				.get("$set", Document.class)).containsEntry("concrete", "CREATED");
	}

	@Test // GH-2786
	void retainsNestedEnumTypeInformationInWholeObjectQueries() {

		converter.setPreserveEnumTypeInformation(true);
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;
		Document query = new QueryMapper(converter).getMappedObject(new Document("event", event),
				mappingContext.getRequiredPersistentEntity(EventHolder.class));

		assertThat(query.get("event", Document.class)).containsEntry("eventType", orderEventDocument());
	}

	@Test // GH-2786
	void retainsSettingWhenChangingDatabaseFactory() {

		converter.setPreserveEnumTypeInformation(true);
		MongoDatabaseFactory factory = mock(MongoDatabaseFactory.class);
		doReturn(new MongoExceptionTranslator()).when(factory).getExceptionTranslator();
		converter = converter.with(factory);
		Event event = new Event();
		event.eventType = OrderEventType.CREATED;

		assertThat(write(event)).containsEntry("eventType", orderEventDocument());
	}

	private Document write(Object source) {

		Document document = new Document();
		converter.write(source, document);
		return document;
	}

	private Document orderEventDocument() {
		return new Document("value", "CREATED").append("_class", OrderEventType.class.getName());
	}

	interface EventType {}

	enum OrderEventType implements EventType {
		CREATED
	}

	enum CustomerEventType implements EventType {
		CREATED
	}

	enum SpecializedEventType implements EventType {
		CREATED {
			@Override
			public String toString() {
				return "created";
			}
		}
	}

	static class NamedEventType implements EventType {
		String name = "created";
	}

	@WritingConverter
	static class OrderEventWriter implements Converter<OrderEventType, String> {
		@Override
		public String convert(OrderEventType source) {
			return "custom";
		}
	}

	@ReadingConverter
	static class EventReader implements Converter<Document, EventType> {
		@Override
		public EventType convert(Document source) {
			return CustomerEventType.CREATED;
		}
	}

	static class Event {
		EventType eventType;
		List<EventType> events;
		EventType[] eventArray;
		Map<String, EventType> eventMap;
		OrderEventType concrete;
		List<OrderEventType> concreteList;
		Map<String, OrderEventType> concreteMap;
		@Field(targetType = FieldType.STRING)
		EventType stringValue;
		@Field(targetType = FieldType.STRING)
		List<EventType> stringList;
		@Field(targetType = FieldType.STRING)
		Map<String, EventType> stringMap;
		@Field(targetType = FieldType.STRING)
		Map<String, Integer> numericMap;
	}

	static class NestedEvents {
		List<List<EventType>> lists;
		Map<String, List<EventType>> mapOfLists;
		Map<String, Map<String, EventType>> maps;
		List<Map<String, EventType>> listOfMaps;
	}

	static class EventHolder {
		Event event;
	}
}
