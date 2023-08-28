/*
 * Copyright 2021-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.annotation.Reference;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.LazyLoadingTestUtils;
import org.springframework.data.mongodb.core.mapping.DocumentPointer;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;

/**
 * {@link DocumentReference} related integration tests for {@link MongoTemplate}.
 *
 * @author Christoph Strobl
 * @author Julia Lee
 */
@ExtendWith(MongoClientExtension.class)
public class MongoTemplateDocumentReferenceTests {

	public static final String DB_NAME = "document-reference-tests";

	static @Client MongoClient client;

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureConversion(it -> {
			it.customConverters(new ReferencableConverter(), new SimpleObjectRefWithReadingConverterToDocumentConverter(),
					new DocumentToSimpleObjectRefWithReadingConverter());
		});

		cfg.configureMappingContext(it -> {
			it.autocreateIndex(false);
		});
	});

	@BeforeEach
	public void setUp() {
		template.flushDatabase();
	}

	@Test // GH-3602
	void writeSimpleTypeReference() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);

		SingleRefRoot source = new SingleRefRoot();
		source.id = "root-1";
		source.simpleValueRef = new SimpleObjectRef("ref-1", "me-the-referenced-object");

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("simpleValueRef")).isEqualTo("ref-1");
	}

	@Test // GH-3782
	void writeTypeReferenceHavingCustomizedIdTargetType() {

		ObjectId expectedIdValue = new ObjectId();
		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);

		SingleRefRoot source = new SingleRefRoot();
		source.id = "root-1";
		source.customIdTargetRef = new ObjectRefHavingCustomizedIdTargetType(expectedIdValue.toString(),
				"me-the-referenced-object");

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("customIdTargetRef")).isEqualTo(expectedIdValue);
	}

	@Test // GH-3602
	void writeMapTypeReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot source = new CollectionRefRoot();
		source.id = "root-1";
		source.mapValueRef = new LinkedHashMap<>();
		source.mapValueRef.put("frodo", new SimpleObjectRef("ref-1", "me-the-1-referenced-object"));
		source.mapValueRef.put("bilbo", new SimpleObjectRef("ref-2", "me-the-2-referenced-object"));

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("mapValueRef", Map.class)).containsEntry("frodo", "ref-1").containsEntry("bilbo", "ref-2");
	}

	@Test // GH-3782
	void writeMapOfTypeReferenceHavingCustomizedIdTargetType() {

		ObjectId expectedIdValue = new ObjectId();
		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot source = new CollectionRefRoot();
		source.id = "root-1";
		source.customIdTargetRefMap = Collections.singletonMap("frodo",
				new ObjectRefHavingCustomizedIdTargetType(expectedIdValue.toString(), "me-the-referenced-object"));

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("customIdTargetRefMap", Map.class)).containsEntry("frodo", expectedIdValue);
	}

	@Test // GH-3602
	void writeCollectionOfSimpleTypeReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot source = new CollectionRefRoot();
		source.id = "root-1";
		source.simpleValueRef = Arrays.asList(new SimpleObjectRef("ref-1", "me-the-1-referenced-object"),
				new SimpleObjectRef("ref-2", "me-the-2-referenced-object"));

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("simpleValueRef", List.class)).containsExactly("ref-1", "ref-2");
	}

	@Test // GH-3782
	void writeListOfTypeReferenceHavingCustomizedIdTargetType() {

		ObjectId expectedIdValue = new ObjectId();
		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot source = new CollectionRefRoot();
		source.id = "root-1";
		source.customIdTargetRefList = Collections.singletonList(
				new ObjectRefHavingCustomizedIdTargetType(expectedIdValue.toString(), "me-the-referenced-object"));

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("customIdTargetRefList", List.class)).containsExactly(expectedIdValue);
	}

	@Test // GH-3602
	void writeObjectTypeReference() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);

		SingleRefRoot source = new SingleRefRoot();
		source.id = "root-1";
		source.objectValueRef = new ObjectRefOfDocument("ref-1", "me-the-referenced-object");

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("objectValueRef")).isEqualTo(source.getObjectValueRef().toReference());
	}

	@Test // GH-3602
	void writeCollectionOfObjectTypeReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot source = new CollectionRefRoot();
		source.id = "root-1";
		source.objectValueRef = Arrays.asList(new ObjectRefOfDocument("ref-1", "me-the-1-referenced-object"),
				new ObjectRefOfDocument("ref-2", "me-the-2-referenced-object"));

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("objectValueRef", List.class)).containsExactly(
				source.getObjectValueRef().get(0).toReference(), source.getObjectValueRef().get(1).toReference());
	}

	@Test // GH-3602
	void readSimpleTypeObjectReference() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simpleValueRef", "ref-1");

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);
		assertThat(result.getSimpleValueRef()).isEqualTo(new SimpleObjectRef("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readCollectionOfSimpleTypeObjectReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simpleValueRef",
				Collections.singletonList("ref-1"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getSimpleValueRef()).containsExactly(new SimpleObjectRef("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readLazySimpleTypeObjectReference() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simpleLazyValueRef", "ref-1");

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);

		LazyLoadingTestUtils.assertProxy(result.simpleLazyValueRef, (proxy) -> {

			assertThat(proxy.isResolved()).isFalse();
			assertThat(proxy.currentValue()).isNull();
		});
		assertThat(result.getSimpleLazyValueRef()).isEqualTo(new SimpleObjectRef("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readSimpleTypeObjectReferenceFromFieldWithCustomName() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simple-value-ref-annotated-field-name",
				"ref-1");

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);
		assertThat(result.getSimpleValueRefWithAnnotatedFieldName())
				.isEqualTo(new SimpleObjectRef("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readCollectionTypeObjectReferenceFromFieldWithCustomName() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simple-value-ref-annotated-field-name",
				Collections.singletonList("ref-1"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getSimpleValueRefWithAnnotatedFieldName())
				.containsExactly(new SimpleObjectRef("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readObjectReferenceFromDocumentType() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(ObjectRefOfDocument.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("objectValueRef",
				new Document("id", "ref-1").append("property", "without-any-meaning"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);
		assertThat(result.getObjectValueRef()).isEqualTo(new ObjectRefOfDocument("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readCollectionObjectReferenceFromDocumentType() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(ObjectRefOfDocument.class);
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("objectValueRef",
				Collections.singletonList(new Document("id", "ref-1").append("property", "without-any-meaning")));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getObjectValueRef())
				.containsExactly(new ObjectRefOfDocument("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readObjectReferenceFromDocumentDeclaringCollectionName() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = "object-ref-of-document-with-embedded-collection-name";
		Document refSource = new Document("_id", "ref-1").append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append(
				"objectValueRefWithEmbeddedCollectionName",
				new Document("id", "ref-1").append("collection", "object-ref-of-document-with-embedded-collection-name")
						.append("property", "without-any-meaning"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);
		assertThat(result.getObjectValueRefWithEmbeddedCollectionName())
				.isEqualTo(new ObjectRefOfDocumentWithEmbeddedCollectionName("ref-1", "me-the-referenced-object"));
	}

	@Test // GH-3602
	void readCollectionObjectReferenceFromDocumentDeclaringCollectionName() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = "object-ref-of-document-with-embedded-collection-name";
		Document refSource1 = new Document("_id", "ref-1").append("value", "me-the-1-referenced-object");
		Document refSource2 = new Document("_id", "ref-2").append("value", "me-the-2-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append(
				"objectValueRefWithEmbeddedCollectionName",
				Arrays.asList(
						new Document("id", "ref-2").append("collection", "object-ref-of-document-with-embedded-collection-name"),
						new Document("id", "ref-1").append("collection", "object-ref-of-document-with-embedded-collection-name")
								.append("property", "without-any-meaning")));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource1);
			db.getCollection(refCollectionName).insertOne(refSource2);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getObjectValueRefWithEmbeddedCollectionName()).containsExactly(
				new ObjectRefOfDocumentWithEmbeddedCollectionName("ref-2", "me-the-2-referenced-object"),
				new ObjectRefOfDocumentWithEmbeddedCollectionName("ref-1", "me-the-1-referenced-object"));
	}

	@Test // GH-3602
	void useOrderFromAnnotatedSort() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource1 = new Document("_id", "ref-1").append("value", "me-the-1-referenced-object");
		Document refSource2 = new Document("_id", "ref-2").append("value", "me-the-2-referenced-object");
		Document refSource3 = new Document("_id", "ref-3").append("value", "me-the-3-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simpleSortedValueRef",
				Arrays.asList("ref-1", "ref-3", "ref-2"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource1);
			db.getCollection(refCollectionName).insertOne(refSource2);
			db.getCollection(refCollectionName).insertOne(refSource3);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getSimpleSortedValueRef()).containsExactly(
				new SimpleObjectRef("ref-3", "me-the-3-referenced-object"),
				new SimpleObjectRef("ref-2", "me-the-2-referenced-object"),
				new SimpleObjectRef("ref-1", "me-the-1-referenced-object"));
	}

	@Test // GH-3602
	void readObjectReferenceFromDocumentNotRelatingToTheIdProperty() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(ObjectRefOnNonIdField.class);
		Document refSource = new Document("_id", "ref-1").append("refKey1", "ref-key-1").append("refKey2", "ref-key-2")
				.append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("objectValueRefOnNonIdFields",
				new Document("refKey1", "ref-key-1").append("refKey2", "ref-key-2").append("property", "without-any-meaning"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);
		assertThat(result.getObjectValueRefOnNonIdFields())
				.isEqualTo(new ObjectRefOnNonIdField("ref-1", "me-the-referenced-object", "ref-key-1", "ref-key-2"));
	}

	@Test // GH-3602
	void readLazyObjectReferenceFromDocumentNotRelatingToTheIdProperty() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(ObjectRefOnNonIdField.class);
		Document refSource = new Document("_id", "ref-1").append("refKey1", "ref-key-1").append("refKey2", "ref-key-2")
				.append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("lazyObjectValueRefOnNonIdFields",
				new Document("refKey1", "ref-key-1").append("refKey2", "ref-key-2").append("property", "without-any-meaning"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);

		LazyLoadingTestUtils.assertProxy(result.lazyObjectValueRefOnNonIdFields, (proxy) -> {

			assertThat(proxy.isResolved()).isFalse();
			assertThat(proxy.currentValue()).isNull();
		});
		assertThat(result.getLazyObjectValueRefOnNonIdFields())
				.isEqualTo(new ObjectRefOnNonIdField("ref-1", "me-the-referenced-object", "ref-key-1", "ref-key-2"));
	}

	@Test // GH-3602
	void readCollectionObjectReferenceFromDocumentNotRelatingToTheIdProperty() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(ObjectRefOnNonIdField.class);
		Document refSource = new Document("_id", "ref-1").append("refKey1", "ref-key-1").append("refKey2", "ref-key-2")
				.append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("objectValueRefOnNonIdFields",
				Collections.singletonList(new Document("refKey1", "ref-key-1").append("refKey2", "ref-key-2").append("property",
						"without-any-meaning")));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getObjectValueRefOnNonIdFields())
				.containsExactly(new ObjectRefOnNonIdField("ref-1", "me-the-referenced-object", "ref-key-1", "ref-key-2"));
	}

	@Test // GH-3602
	void readMapOfReferences() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);

		Document refSource1 = new Document("_id", "ref-1").append("refKey1", "ref-key-1").append("refKey2", "ref-key-2")
				.append("value", "me-the-1-referenced-object");

		Document refSource2 = new Document("_id", "ref-2").append("refKey1", "ref-key-1").append("refKey2", "ref-key-2")
				.append("value", "me-the-2-referenced-object");

		Map<String, String> refmap = new LinkedHashMap<>();
		refmap.put("frodo", "ref-1");
		refmap.put("bilbo", "ref-2");

		Document source = new Document("_id", "id-1").append("value", "v1").append("mapValueRef", refmap);

		template.execute(db -> {

			db.getCollection(rootCollectionName).insertOne(source);
			db.getCollection(refCollectionName).insertOne(refSource1);
			db.getCollection(refCollectionName).insertOne(refSource2);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);

		assertThat(result.getMapValueRef())
				.containsEntry("frodo", new SimpleObjectRef("ref-1", "me-the-1-referenced-object"))
				.containsEntry("bilbo", new SimpleObjectRef("ref-2", "me-the-2-referenced-object"));
	}

	@Test // GH-3602
	void loadLazyCyclicReference() {

		WithRefA a = new WithRefA();
		a.id = "a";

		WithRefB b = new WithRefB();
		b.id = "b";

		a.toB = b;
		b.lazyToA = a;

		template.save(a);
		template.save(b);

		WithRefA loadedA = template.query(WithRefA.class).matching(where("id").is(a.id)).firstValue();
		assertThat(loadedA).isNotNull();
		assertThat(loadedA.getToB()).isNotNull();
		LazyLoadingTestUtils.assertProxy(loadedA.getToB().lazyToA, (proxy) -> {

			assertThat(proxy.isResolved()).isFalse();
			assertThat(proxy.currentValue()).isNull();
		});
	}

	@Test // GH-3602
	void loadEagerCyclicReference() {

		WithRefA a = new WithRefA();
		a.id = "a";

		WithRefB b = new WithRefB();
		b.id = "b";

		a.toB = b;
		b.eagerToA = a;

		template.save(a);
		template.save(b);

		WithRefA loadedA = template.query(WithRefA.class).matching(where("id").is(a.id)).firstValue();

		assertThat(loadedA).isNotNull();
		assertThat(loadedA.getToB()).isNotNull();
		assertThat(loadedA.getToB().eagerToA).isSameAs(loadedA);
	}

	@Test // GH-3602
	void loadAndStoreUnresolvedLazyDoesNotResolveTheProxy() {

		String collectionB = template.getCollectionName(WithRefB.class);

		WithRefA a = new WithRefA();
		a.id = "a";

		WithRefB b = new WithRefB();
		b.id = "b";

		a.toB = b;
		b.lazyToA = a;

		template.save(a);
		template.save(b);

		WithRefA loadedA = template.query(WithRefA.class).matching(where("id").is(a.id)).firstValue();
		template.save(loadedA.getToB());

		LazyLoadingTestUtils.assertProxy(loadedA.getToB().lazyToA, (proxy) -> {

			assertThat(proxy.isResolved()).isFalse();
			assertThat(proxy.currentValue()).isNull();
		});

		Document target = template.execute(db -> {
			return db.getCollection(collectionB).find(Filters.eq("_id", "b")).first();
		});
		assertThat(target.get("lazyToA", Object.class)).isEqualTo("a");
	}

	@Test // GH-3602
	void loadCollectionReferenceWithMissingRefs() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);

		// ref-1 is missing.
		Document refSource = new Document("_id", "ref-2").append("value", "me-the-2-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simpleValueRef",
				Arrays.asList("ref-1", "ref-2"));

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.getSimpleValueRef()).containsExactly(new SimpleObjectRef("ref-2", "me-the-2-referenced-object"));
	}

	@Test // GH-3805
	void loadEmptyCollectionReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		// an empty reference array.
		Document source = new Document("_id", "id-1").append("value", "v1").append("simplePreinitializedValueRef",
				Collections.emptyList());

		template.execute(db -> {
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.simplePreinitializedValueRef).isEmpty();
	}

	@Test // GH-3805
	void loadEmptyMapReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		// an empty reference array.
		Document source = new Document("_id", "id-1").append("value", "v1").append("simplePreinitializedMapRef",
				new Document());

		template.execute(db -> {
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.simplePreinitializedMapRef).isEmpty();
	}

	@Test // GH-3805
	void loadNoExistingCollectionReference() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		// no reference array at all
		Document source = new Document("_id", "id-1").append("value", "v1");

		template.execute(db -> {
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		CollectionRefRoot result = template.findOne(query(where("id").is("id-1")), CollectionRefRoot.class);
		assertThat(result.simplePreinitializedValueRef).isEmpty();
	}

	@Test // GH-3806
	void resolveReferenceWhenUsedAsCtorArgument() {

		Publisher publisher = new Publisher();
		publisher.id = "p-111";
		publisher.name = "ppp";

		template.save(publisher);

		WithRequiredArgsCtor source = new WithRequiredArgsCtor("id-1", publisher);

		template.save(source);

		WithRequiredArgsCtor target = template.findOne(query(where("id").is(source.id)), WithRequiredArgsCtor.class);
		assertThat(target.publisher).isNotNull();
	}

	@Test // GH-3806
	void resolveLazyReferenceWhenUsedAsCtorArgument() {

		Publisher publisher = new Publisher();
		publisher.id = "p-111";
		publisher.name = "ppp";

		template.save(publisher);

		WithLazyRequiredArgsCtor source = new WithLazyRequiredArgsCtor("id-1", publisher);

		template.save(source);

		WithLazyRequiredArgsCtor target = template.findOne(query(where("id").is(source.id)), WithLazyRequiredArgsCtor.class);

		// proxy not yet resolved
		LazyLoadingTestUtils.assertProxy(target.publisher, (proxy) -> {

			assertThat(proxy.isResolved()).isFalse();
			assertThat(proxy.currentValue()).isNull();
		});

		// resolve the proxy by invoking a method on it
		assertThat(target.getPublisher().getName()).isEqualTo("ppp");
		LazyLoadingTestUtils.assertProxy(target.publisher, (proxy) -> {
			assertThat(proxy.isResolved()).isTrue();
		});
	}

	@Test // GH-3602
	void queryForReference() {

		WithRefB b = new WithRefB();
		b.id = "b";
		template.save(b);

		WithRefA a = new WithRefA();
		a.id = "a";
		a.toB = b;
		template.save(a);

		WithRefA a2 = new WithRefA();
		a2.id = "a2";
		template.save(a2);

		WithRefA loadedA = template.query(WithRefA.class).matching(where("toB").is(b)).firstValue();
		assertThat(loadedA.getId()).isEqualTo(a.getId());
	}

	@Test // GH-3602
	void queryForReferenceInCollection() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		Document shouldBeFound = new Document("_id", "id-1").append("value", "v1").append("simpleValueRef",
				Arrays.asList("ref-1", "ref-2"));
		Document shouldNotBeFound = new Document("_id", "id-2").append("value", "v2").append("simpleValueRef",
				Arrays.asList("ref-1"));

		template.execute(db -> {

			db.getCollection(rootCollectionName).insertOne(shouldBeFound);
			db.getCollection(rootCollectionName).insertOne(shouldNotBeFound);
			return null;
		});

		SimpleObjectRef objectRef = new SimpleObjectRef("ref-2", "some irrelevant value");

		List<CollectionRefRoot> loaded = template.query(CollectionRefRoot.class)
				.matching(where("simpleValueRef").in(objectRef)).all();
		assertThat(loaded).map(CollectionRefRoot::getId).containsExactly("id-1");
	}

	@Test // GH-3602
	void queryForReferenceOnIdField() {

		WithRefB b = new WithRefB();
		b.id = "b";
		template.save(b);

		WithRefA a = new WithRefA();
		a.id = "a";
		a.toB = b;
		template.save(a);

		WithRefA a2 = new WithRefA();
		a2.id = "a2";
		template.save(a2);

		WithRefA loadedA = template.query(WithRefA.class).matching(where("toB.id").is(b.id)).firstValue();
		assertThat(loadedA.getId()).isEqualTo(a.getId());
	}

	@Test // GH-3602
	void updateReferenceWithEntityHavingPointerConversion() {

		WithRefB b = new WithRefB();
		b.id = "b";
		template.save(b);

		WithRefA a = new WithRefA();
		a.id = "a";
		template.save(a);

		template.update(WithRefA.class).apply(new Update().set("toB", b)).first();

		String collectionA = template.getCollectionName(WithRefA.class);

		Document target = template.execute(db -> {
			return db.getCollection(collectionA).find(Filters.eq("_id", "a")).first();
		});

		assertThat(target).containsEntry("toB", "b");
	}

	@Test // GH-3602
	void updateReferenceWithEntityWithoutPointerConversion() {

		String collectionName = template.getCollectionName(SingleRefRoot.class);
		SingleRefRoot refRoot = new SingleRefRoot();
		refRoot.id = "root-1";

		SimpleObjectRef ref = new SimpleObjectRef("ref-1", "me the referenced object");

		template.save(refRoot);

		template.update(SingleRefRoot.class).apply(new Update().set("simpleValueRef", ref)).first();

		Document target = template.execute(db -> {
			return db.getCollection(collectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target).containsEntry("simpleValueRef", "ref-1");
	}

	@Test // GH-3602
	void updateReferenceWithValue() {

		WithRefA a = new WithRefA();
		a.id = "a";
		template.save(a);

		template.update(WithRefA.class).apply(new Update().set("toB", "b")).first();

		String collectionA = template.getCollectionName(WithRefA.class);

		Document target = template.execute(db -> {
			return db.getCollection(collectionA).find(Filters.eq("_id", "a")).first();
		});

		assertThat(target).containsEntry("toB", "b");
	}

	@Test // GH-4041
	void updateReferenceWithPushToCollection() {

		WithListOfRefs a = new WithListOfRefs();
		a.id = "a";
		template.save(a);

		WithListOfRefs b = new WithListOfRefs();
		b.id = "b";
		template.save(b);

		template.update(WithListOfRefs.class).matching(where("id").is(a.id))
				.apply(new Update().push("refs").each(new Object[] { b })).first();

		String collection = template.getCollectionName(WithListOfRefs.class);

		Document target = template.execute(db -> {
			return db.getCollection(collection).find(Filters.eq("_id", "a")).first();
		});

		assertThat(target).containsEntry("refs", Collections.singletonList("b"));
	}

	@Test // GH-3782
	void updateReferenceHavingCustomizedIdTargetType() {

		ObjectId expectedIdValue = new ObjectId();
		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);

		SingleRefRoot root = new SingleRefRoot();
		root.id = "root-1";
		template.save(root);

		template.update(SingleRefRoot.class).apply(new Update().set("customIdTargetRef",
				new ObjectRefHavingCustomizedIdTargetType(expectedIdValue.toString(), "b"))).first();

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target).containsEntry("customIdTargetRef", expectedIdValue);
	}

	@Test // GH-3602
	void updateReferenceCollectionWithEntity() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot root = new CollectionRefRoot();
		root.id = "root-1";
		root.simpleValueRef = Collections.singletonList(new SimpleObjectRef("ref-1", "beastie"));

		template.save(root);

		template.update(CollectionRefRoot.class)
				.apply(new Update().push("simpleValueRef").value(new SimpleObjectRef("ref-2", "boys"))).first();

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target).containsEntry("simpleValueRef", Arrays.asList("ref-1", "ref-2"));
	}

	@Test // GH-3602
	void updateReferenceCollectionWithValue() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot root = new CollectionRefRoot();
		root.id = "root-1";
		root.simpleValueRef = Collections.singletonList(new SimpleObjectRef("ref-1", "beastie"));

		template.save(root);

		template.update(CollectionRefRoot.class).apply(new Update().push("simpleValueRef").value("ref-2")).first();

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target).containsEntry("simpleValueRef", Arrays.asList("ref-1", "ref-2"));
	}

	@Test // GH-3602
	@Disabled("Property path resolution does not work inside maps, the key is considered :/")
	void updateReferenceMapWithEntity() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot root = new CollectionRefRoot();
		root.id = "root-1";
		root.mapValueRef = Collections.singletonMap("beastie", new SimpleObjectRef("ref-1", "boys"));

		template.save(root);

		template.update(CollectionRefRoot.class)
				.apply(new Update().set("mapValueRef.rise", new SimpleObjectRef("ref-2", "against"))).first();

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target).containsEntry("mapValueRef", new Document("beastie", "ref-1").append("rise", "ref-2"));
	}

	@Test // GH-3602
	void updateReferenceMapWithValue() {

		String rootCollectionName = template.getCollectionName(CollectionRefRoot.class);

		CollectionRefRoot root = new CollectionRefRoot();
		root.id = "root-1";
		root.mapValueRef = Collections.singletonMap("beastie", new SimpleObjectRef("ref-1", "boys"));

		template.save(root);

		template.update(CollectionRefRoot.class).apply(new Update().set("mapValueRef.rise", "ref-2")).first();

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target).containsEntry("mapValueRef", new Document("beastie", "ref-1").append("rise", "ref-2"));
	}

	@Test // GH-3602
	void useReadingWriterConverterPairForLoading() {

		SingleRefRoot root = new SingleRefRoot();
		root.id = "root-1";
		root.withReadingConverter = new SimpleObjectRefWithReadingConverter("ref-1", "value-1");

		template.save(root.withReadingConverter);

		template.save(root);

		Document target = template.execute(db -> {
			return db.getCollection(template.getCollectionName(SingleRefRoot.class)).find(Filters.eq("_id", root.id)).first();
		});

		assertThat(target).containsEntry("withReadingConverter",
				new Document("ref-key-from-custom-write-converter", root.withReadingConverter.id));

		SingleRefRoot loaded = template.findOne(query(where("id").is(root.id)), SingleRefRoot.class);
		assertThat(loaded.withReadingConverter).isInstanceOf(SimpleObjectRefWithReadingConverter.class);
	}

	@Test // GH-3602
	void deriveMappingFromLookup() {

		Publisher publisher = new Publisher();
		publisher.id = "p-1";
		publisher.acronym = "TOR";
		publisher.name = "Tom Doherty Associates";

		template.save(publisher);

		Book book = new Book();
		book.id = "book-1";
		book.publisher = publisher;

		template.save(book);

		Document target = template.execute(db -> {
			return db.getCollection(template.getCollectionName(Book.class)).find(Filters.eq("_id", book.id)).first();
		});

		assertThat(target).containsEntry("publisher", new Document("acc", publisher.acronym).append("n", publisher.name));

		Book result = template.findOne(query(where("id").is(book.id)), Book.class);
		assertThat(result.publisher).isNotNull();
	}

	@Test // GH-3602
	void updateDerivedMappingFromLookup() {

		Publisher publisher = new Publisher();
		publisher.id = "p-1";
		publisher.acronym = "TOR";
		publisher.name = "Tom Doherty Associates";

		template.save(publisher);

		Book book = new Book();
		book.id = "book-1";

		template.save(book);

		template.update(Book.class).matching(where("id").is(book.id)).apply(new Update().set("publisher", publisher))
				.first();

		Document target = template.execute(db -> {
			return db.getCollection(template.getCollectionName(Book.class)).find(Filters.eq("_id", book.id)).first();
		});

		assertThat(target).containsEntry("publisher", new Document("acc", publisher.acronym).append("n", publisher.name));

		Book result = template.findOne(query(where("id").is(book.id)), Book.class);
		assertThat(result.publisher).isNotNull();
	}

	@Test // GH-3602
	void queryDerivedMappingFromLookup() {

		Publisher publisher = new Publisher();
		publisher.id = "p-1";
		publisher.acronym = "TOR";
		publisher.name = "Tom Doherty Associates";

		template.save(publisher);

		Book book = new Book();
		book.id = "book-1";
		book.publisher = publisher;

		template.save(book);
		book.publisher = publisher;

		Book result = template.findOne(query(where("publisher").is(publisher)), Book.class);
		assertThat(result.publisher).isNotNull();
	}

	@Test // GH-3602
	void allowsDirectUsageOfAtReference() {

		Publisher publisher = new Publisher();
		publisher.id = "p-1";
		publisher.acronym = "TOR";
		publisher.name = "Tom Doherty Associates";

		template.save(publisher);

		UsingAtReference root = new UsingAtReference();
		root.id = "book-1";
		root.publisher = publisher;

		template.save(root);

		Document target = template.execute(db -> {
			return db.getCollection(template.getCollectionName(UsingAtReference.class)).find(Filters.eq("_id", root.id)).first();
		});

		assertThat(target).containsEntry("publisher", "p-1");

		UsingAtReference result = template.findOne(query(where("id").is(root.id)), UsingAtReference.class);
		assertThat(result.publisher).isNotNull();
	}

	@Test // GH-3602
	void updateWhenUsingAtReferenceDirectly() {

		Publisher publisher = new Publisher();
		publisher.id = "p-1";
		publisher.acronym = "TOR";
		publisher.name = "Tom Doherty Associates";

		template.save(publisher);

		UsingAtReference root = new UsingAtReference();
		root.id = "book-1";

		template.save(root);
		template.update(UsingAtReference.class).matching(where("id").is(root.id)).apply(new Update().set("publisher", publisher)).first();

		Document target = template.execute(db -> {
			return db.getCollection(template.getCollectionName(UsingAtReference.class)).find(Filters.eq("_id", root.id)).first();
		});

		assertThat(target).containsEntry("publisher", "p-1");
	}

	@Test // GH-3798
	void allowsOneToMayStyleLookupsUsingSelfVariable() {

		OneToManyStyleBook book1 = new OneToManyStyleBook();
		book1.id = "id-1";
		book1.publisherId = "p-100";

		OneToManyStyleBook book2 = new OneToManyStyleBook();
		book2.id = "id-2";
		book2.publisherId = "p-200";

		OneToManyStyleBook book3 = new OneToManyStyleBook();
		book3.id = "id-3";
		book3.publisherId = "p-100";

		template.save(book1);
		template.save(book2);
		template.save(book3);

		OneToManyStylePublisher publisher = new OneToManyStylePublisher();
		publisher.id = "p-100";

		template.save(publisher);

		OneToManyStylePublisher target = template.findOne(query(where("id").is(publisher.id)), OneToManyStylePublisher.class);
		assertThat(target.books).containsExactlyInAnyOrder(book1, book3);
	}

	@Test // GH-3847
	void writeReferenceWithIdStringThatIsAnObjectId() {

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);

		ObjectId id = new ObjectId();

		SingleRefRoot source = new SingleRefRoot();
		source.id = "root-1";
		source.simpleValueRef = new SimpleObjectRef(id.toHexString(), "me-the-referenced-object");

		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("simpleValueRef")).isEqualTo(id);
	}

	@Test // GH-3847
	void readWithIdStringThatIsAnObjectId() {

		ObjectId id = new ObjectId();

		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);
		String refCollectionName = template.getCollectionName(SimpleObjectRef.class);
		Document refSource = new Document("_id", id).append("value", "me-the-referenced-object");
		Document source = new Document("_id", "id-1").append("value", "v1").append("simpleValueRef", id);

		template.execute(db -> {

			db.getCollection(refCollectionName).insertOne(refSource);
			db.getCollection(rootCollectionName).insertOne(source);
			return null;
		});

		SingleRefRoot result = template.findOne(query(where("id").is("id-1")), SingleRefRoot.class);
		assertThat(result.getSimpleValueRef()).isEqualTo(new SimpleObjectRef(id.toHexString(), "me-the-referenced-object"));
	}

	@Test // GH-3847
	void readWriteTypeReferenceHavingFixedStringIdTargetType() {

		ObjectId id = new ObjectId();
		String rootCollectionName = template.getCollectionName(SingleRefRoot.class);

		ObjectRefHavingStringIdTargetType customStringIdTargetRef = new ObjectRefHavingStringIdTargetType(id.toHexString(),
				"me-the-referenced-object");
		template.save(customStringIdTargetRef);

		SingleRefRoot source = new SingleRefRoot();
		source.id = "root-1";
		source.customStringIdTargetRef = customStringIdTargetRef;
		template.save(source);

		Document target = template.execute(db -> {
			return db.getCollection(rootCollectionName).find(Filters.eq("_id", "root-1")).first();
		});

		assertThat(target.get("customStringIdTargetRef")).isEqualTo(id.toHexString());

		SingleRefRoot result = template.findOne(query(where("id").is("root-1")), SingleRefRoot.class);
		assertThat(result.getCustomStringIdTargetRef())
				.isEqualTo(new ObjectRefHavingStringIdTargetType(id.toHexString(), "me-the-referenced-object"));
	}

	@Test // GH-4484
	void resolveReferenceForOneToManyLookupWithSelfVariableWhenUsedInCtorArgument() {

		OneToManyStylePublisherWithRequiredArgsCtor publisher = new OneToManyStylePublisherWithRequiredArgsCtor("p-100", null);
		template.save(publisher);

		OneToManyStyleBook book1 = new OneToManyStyleBook();
		book1.id = "id-1";
		book1.publisherId = publisher.id;

		OneToManyStyleBook book2 = new OneToManyStyleBook();
		book2.id = "id-2";
		book2.publisherId = "p-200";

		OneToManyStyleBook book3 = new OneToManyStyleBook();
		book3.id = "id-3";
		book3.publisherId = publisher.id;

		template.save(book1);
		template.save(book2);
		template.save(book3);

		OneToManyStylePublisherWithRequiredArgsCtor target = template.findOne(query(where("id").is(publisher.id)), OneToManyStylePublisherWithRequiredArgsCtor.class);
		assertThat(target.books).containsExactlyInAnyOrder(book1, book3);
	}

	static class SingleRefRoot {

		String id;
		String value;

		@DocumentReference SimpleObjectRefWithReadingConverter withReadingConverter;

		@DocumentReference(lookup = "{ '_id' : ?#{#target} }") //
		SimpleObjectRef simpleValueRef;

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }", lazy = true) //
		SimpleObjectRef simpleLazyValueRef;

		@Field("simple-value-ref-annotated-field-name") //
		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		SimpleObjectRef simpleValueRefWithAnnotatedFieldName;

		@DocumentReference(lookup = "{ '_id' : '?#{id}' }") //
		ObjectRefOfDocument objectValueRef;

		@DocumentReference(lookup = "{ '_id' : '?#{id}' }", collection = "#collection") //
		ObjectRefOfDocumentWithEmbeddedCollectionName objectValueRefWithEmbeddedCollectionName;

		@DocumentReference(lookup = "{ 'refKey1' : '?#{refKey1}', 'refKey2' : '?#{refKey2}' }") //
		ObjectRefOnNonIdField objectValueRefOnNonIdFields;

		@DocumentReference(lookup = "{ 'refKey1' : '?#{refKey1}', 'refKey2' : '?#{refKey2}' }", lazy = true) //
		ObjectRefOnNonIdField lazyObjectValueRefOnNonIdFields;

		@DocumentReference ObjectRefHavingCustomizedIdTargetType customIdTargetRef;

		@DocumentReference ObjectRefHavingStringIdTargetType customStringIdTargetRef;

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public SimpleObjectRefWithReadingConverter getWithReadingConverter() {
			return this.withReadingConverter;
		}

		public SimpleObjectRef getSimpleValueRef() {
			return this.simpleValueRef;
		}

		public SimpleObjectRef getSimpleLazyValueRef() {
			return this.simpleLazyValueRef;
		}

		public SimpleObjectRef getSimpleValueRefWithAnnotatedFieldName() {
			return this.simpleValueRefWithAnnotatedFieldName;
		}

		public ObjectRefOfDocument getObjectValueRef() {
			return this.objectValueRef;
		}

		public ObjectRefOfDocumentWithEmbeddedCollectionName getObjectValueRefWithEmbeddedCollectionName() {
			return this.objectValueRefWithEmbeddedCollectionName;
		}

		public ObjectRefOnNonIdField getObjectValueRefOnNonIdFields() {
			return this.objectValueRefOnNonIdFields;
		}

		public ObjectRefOnNonIdField getLazyObjectValueRefOnNonIdFields() {
			return this.lazyObjectValueRefOnNonIdFields;
		}

		public ObjectRefHavingCustomizedIdTargetType getCustomIdTargetRef() {
			return this.customIdTargetRef;
		}

		public ObjectRefHavingStringIdTargetType getCustomStringIdTargetRef() {
			return this.customStringIdTargetRef;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setWithReadingConverter(SimpleObjectRefWithReadingConverter withReadingConverter) {
			this.withReadingConverter = withReadingConverter;
		}

		public void setSimpleValueRef(SimpleObjectRef simpleValueRef) {
			this.simpleValueRef = simpleValueRef;
		}

		public void setSimpleLazyValueRef(SimpleObjectRef simpleLazyValueRef) {
			this.simpleLazyValueRef = simpleLazyValueRef;
		}

		public void setSimpleValueRefWithAnnotatedFieldName(SimpleObjectRef simpleValueRefWithAnnotatedFieldName) {
			this.simpleValueRefWithAnnotatedFieldName = simpleValueRefWithAnnotatedFieldName;
		}

		public void setObjectValueRef(ObjectRefOfDocument objectValueRef) {
			this.objectValueRef = objectValueRef;
		}

		public void setObjectValueRefWithEmbeddedCollectionName(
				ObjectRefOfDocumentWithEmbeddedCollectionName objectValueRefWithEmbeddedCollectionName) {
			this.objectValueRefWithEmbeddedCollectionName = objectValueRefWithEmbeddedCollectionName;
		}

		public void setObjectValueRefOnNonIdFields(ObjectRefOnNonIdField objectValueRefOnNonIdFields) {
			this.objectValueRefOnNonIdFields = objectValueRefOnNonIdFields;
		}

		public void setLazyObjectValueRefOnNonIdFields(ObjectRefOnNonIdField lazyObjectValueRefOnNonIdFields) {
			this.lazyObjectValueRefOnNonIdFields = lazyObjectValueRefOnNonIdFields;
		}

		public void setCustomIdTargetRef(ObjectRefHavingCustomizedIdTargetType customIdTargetRef) {
			this.customIdTargetRef = customIdTargetRef;
		}

		public void setCustomStringIdTargetRef(ObjectRefHavingStringIdTargetType customStringIdTargetRef) {
			this.customStringIdTargetRef = customStringIdTargetRef;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.SingleRefRoot(id=" + this.getId() + ", value=" + this.getValue()
					+ ", withReadingConverter=" + this.getWithReadingConverter() + ", simpleValueRef=" + this.getSimpleValueRef()
					+ ", simpleLazyValueRef=" + this.getSimpleLazyValueRef() + ", simpleValueRefWithAnnotatedFieldName="
					+ this.getSimpleValueRefWithAnnotatedFieldName() + ", objectValueRef=" + this.getObjectValueRef()
					+ ", objectValueRefWithEmbeddedCollectionName=" + this.getObjectValueRefWithEmbeddedCollectionName()
					+ ", objectValueRefOnNonIdFields=" + this.getObjectValueRefOnNonIdFields()
					+ ", lazyObjectValueRefOnNonIdFields=" + this.getLazyObjectValueRefOnNonIdFields() + ", customIdTargetRef="
					+ this.getCustomIdTargetRef() + ", customStringIdTargetRef=" + this.getCustomStringIdTargetRef() + ")";
		}
	}

	static class CollectionRefRoot {

		String id;
		String value;

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		List<SimpleObjectRef> simpleValueRef;

		@DocumentReference
		List<SimpleObjectRef> simplePreinitializedValueRef = new ArrayList<>();

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }", sort = "{ '_id' : -1 } ") //
		List<SimpleObjectRef> simpleSortedValueRef;

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		Map<String, SimpleObjectRef> mapValueRef;

		@DocumentReference //
		Map<String, SimpleObjectRef> simplePreinitializedMapRef = new LinkedHashMap<>();

		@Field("simple-value-ref-annotated-field-name") //
		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		List<SimpleObjectRef> simpleValueRefWithAnnotatedFieldName;

		@DocumentReference(lookup = "{ '_id' : '?#{id}' }") //
		List<ObjectRefOfDocument> objectValueRef;

		@DocumentReference(lookup = "{ '_id' : '?#{id}' }", collection = "?#{collection}") //
		List<ObjectRefOfDocumentWithEmbeddedCollectionName> objectValueRefWithEmbeddedCollectionName;

		@DocumentReference(lookup = "{ 'refKey1' : '?#{refKey1}', 'refKey2' : '?#{refKey2}' }") //
		List<ObjectRefOnNonIdField> objectValueRefOnNonIdFields;

		@DocumentReference List<ObjectRefHavingCustomizedIdTargetType> customIdTargetRefList;

		@DocumentReference Map<String, ObjectRefHavingCustomizedIdTargetType> customIdTargetRefMap;

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public List<SimpleObjectRef> getSimpleValueRef() {
			return this.simpleValueRef;
		}

		public List<SimpleObjectRef> getSimplePreinitializedValueRef() {
			return this.simplePreinitializedValueRef;
		}

		public List<SimpleObjectRef> getSimpleSortedValueRef() {
			return this.simpleSortedValueRef;
		}

		public Map<String, SimpleObjectRef> getMapValueRef() {
			return this.mapValueRef;
		}

		public Map<String, SimpleObjectRef> getSimplePreinitializedMapRef() {
			return this.simplePreinitializedMapRef;
		}

		public List<SimpleObjectRef> getSimpleValueRefWithAnnotatedFieldName() {
			return this.simpleValueRefWithAnnotatedFieldName;
		}

		public List<ObjectRefOfDocument> getObjectValueRef() {
			return this.objectValueRef;
		}

		public List<ObjectRefOfDocumentWithEmbeddedCollectionName> getObjectValueRefWithEmbeddedCollectionName() {
			return this.objectValueRefWithEmbeddedCollectionName;
		}

		public List<ObjectRefOnNonIdField> getObjectValueRefOnNonIdFields() {
			return this.objectValueRefOnNonIdFields;
		}

		public List<ObjectRefHavingCustomizedIdTargetType> getCustomIdTargetRefList() {
			return this.customIdTargetRefList;
		}

		public Map<String, ObjectRefHavingCustomizedIdTargetType> getCustomIdTargetRefMap() {
			return this.customIdTargetRefMap;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setSimpleValueRef(List<SimpleObjectRef> simpleValueRef) {
			this.simpleValueRef = simpleValueRef;
		}

		public void setSimplePreinitializedValueRef(List<SimpleObjectRef> simplePreinitializedValueRef) {
			this.simplePreinitializedValueRef = simplePreinitializedValueRef;
		}

		public void setSimpleSortedValueRef(List<SimpleObjectRef> simpleSortedValueRef) {
			this.simpleSortedValueRef = simpleSortedValueRef;
		}

		public void setMapValueRef(Map<String, SimpleObjectRef> mapValueRef) {
			this.mapValueRef = mapValueRef;
		}

		public void setSimplePreinitializedMapRef(Map<String, SimpleObjectRef> simplePreinitializedMapRef) {
			this.simplePreinitializedMapRef = simplePreinitializedMapRef;
		}

		public void setSimpleValueRefWithAnnotatedFieldName(List<SimpleObjectRef> simpleValueRefWithAnnotatedFieldName) {
			this.simpleValueRefWithAnnotatedFieldName = simpleValueRefWithAnnotatedFieldName;
		}

		public void setObjectValueRef(List<ObjectRefOfDocument> objectValueRef) {
			this.objectValueRef = objectValueRef;
		}

		public void setObjectValueRefWithEmbeddedCollectionName(
				List<ObjectRefOfDocumentWithEmbeddedCollectionName> objectValueRefWithEmbeddedCollectionName) {
			this.objectValueRefWithEmbeddedCollectionName = objectValueRefWithEmbeddedCollectionName;
		}

		public void setObjectValueRefOnNonIdFields(List<ObjectRefOnNonIdField> objectValueRefOnNonIdFields) {
			this.objectValueRefOnNonIdFields = objectValueRefOnNonIdFields;
		}

		public void setCustomIdTargetRefList(List<ObjectRefHavingCustomizedIdTargetType> customIdTargetRefList) {
			this.customIdTargetRefList = customIdTargetRefList;
		}

		public void setCustomIdTargetRefMap(Map<String, ObjectRefHavingCustomizedIdTargetType> customIdTargetRefMap) {
			this.customIdTargetRefMap = customIdTargetRefMap;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.CollectionRefRoot(id=" + this.getId() + ", value=" + this.getValue()
					+ ", simpleValueRef=" + this.getSimpleValueRef() + ", simplePreinitializedValueRef="
					+ this.getSimplePreinitializedValueRef() + ", simpleSortedValueRef=" + this.getSimpleSortedValueRef()
					+ ", mapValueRef=" + this.getMapValueRef() + ", simplePreinitializedMapRef="
					+ this.getSimplePreinitializedMapRef() + ", simpleValueRefWithAnnotatedFieldName="
					+ this.getSimpleValueRefWithAnnotatedFieldName() + ", objectValueRef=" + this.getObjectValueRef()
					+ ", objectValueRefWithEmbeddedCollectionName=" + this.getObjectValueRefWithEmbeddedCollectionName()
					+ ", objectValueRefOnNonIdFields=" + this.getObjectValueRefOnNonIdFields() + ", customIdTargetRefList="
					+ this.getCustomIdTargetRefList() + ", customIdTargetRefMap=" + this.getCustomIdTargetRefMap() + ")";
		}
	}

	@FunctionalInterface
	interface ReferenceAble {
		Object toReference();
	}

	@org.springframework.data.mongodb.core.mapping.Document("simple-object-ref")
	static class SimpleObjectRef {

		@Id String id;
		String value;

		public SimpleObjectRef(String id, String value) {
			this.id = id;
			this.value = value;
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SimpleObjectRef that = (SimpleObjectRef) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.SimpleObjectRef(id=" + this.getId() + ", value=" + this.getValue()
					+ ")";
		}
	}

	static class SimpleObjectRefWithReadingConverter extends SimpleObjectRef {

		public SimpleObjectRefWithReadingConverter(String id, String value) {
			super(id, value);
		}

	}

	static class ObjectRefOfDocument implements ReferenceAble {

		@Id String id;
		String value;

		public ObjectRefOfDocument(String id, String value) {
			this.id = id;
			this.value = value;
		}

		@Override
		public Object toReference() {
			return new Document("id", id).append("property", "without-any-meaning");
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ObjectRefOfDocument that = (ObjectRefOfDocument) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.ObjectRefOfDocument(id=" + this.getId() + ", value=" + this.getValue()
					+ ")";
		}
	}

	static class ObjectRefOfDocumentWithEmbeddedCollectionName implements ReferenceAble {

		@Id String id;
		String value;

		public ObjectRefOfDocumentWithEmbeddedCollectionName(String id, String value) {
			this.id = id;
			this.value = value;
		}

		@Override
		public Object toReference() {
			return new Document("id", id).append("collection", "object-ref-of-document-with-embedded-collection-name");
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ObjectRefOfDocumentWithEmbeddedCollectionName that = (ObjectRefOfDocumentWithEmbeddedCollectionName) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.ObjectRefOfDocumentWithEmbeddedCollectionName(id=" + this.getId()
					+ ", value=" + this.getValue() + ")";
		}
	}

	static class ObjectRefOnNonIdField implements ReferenceAble {

		@Id String id;
		String value;
		String refKey1;
		String refKey2;

		public ObjectRefOnNonIdField(String id, String value, String refKey1, String refKey2) {
			this.id = id;
			this.value = value;
			this.refKey1 = refKey1;
			this.refKey2 = refKey2;
		}

		@Override
		public Object toReference() {
			return new Document("refKey1", refKey1).append("refKey2", refKey2);
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public String getRefKey1() {
			return this.refKey1;
		}

		public String getRefKey2() {
			return this.refKey2;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setRefKey1(String refKey1) {
			this.refKey1 = refKey1;
		}

		public void setRefKey2(String refKey2) {
			this.refKey2 = refKey2;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ObjectRefOnNonIdField that = (ObjectRefOnNonIdField) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value) && Objects.equals(refKey1, that.refKey1)
					&& Objects.equals(refKey2, that.refKey2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value, refKey1, refKey2);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.ObjectRefOnNonIdField(id=" + this.getId() + ", value="
					+ this.getValue() + ", refKey1=" + this.getRefKey1() + ", refKey2=" + this.getRefKey2() + ")";
		}
	}

	static class ObjectRefHavingCustomizedIdTargetType {

		@MongoId(targetType = FieldType.OBJECT_ID) String id;
		String name;

		public ObjectRefHavingCustomizedIdTargetType(String id, String name) {
			this.id = id;
			this.name = name;
		}

		public String getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ObjectRefHavingCustomizedIdTargetType that = (ObjectRefHavingCustomizedIdTargetType) o;
			return Objects.equals(id, that.id) && Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.ObjectRefHavingCustomizedIdTargetType(id=" + this.getId() + ", name="
					+ this.getName() + ")";
		}
	}

	static class ObjectRefHavingStringIdTargetType {

		@MongoId(targetType = FieldType.STRING) String id;
		String name;

		public ObjectRefHavingStringIdTargetType(String id, String name) {
			this.id = id;
			this.name = name;
		}

		public String getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ObjectRefHavingStringIdTargetType that = (ObjectRefHavingStringIdTargetType) o;
			return Objects.equals(id, that.id) && Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.ObjectRefHavingStringIdTargetType(id=" + this.getId() + ", name="
					+ this.getName() + ")";
		}
	}

	static class ReferencableConverter implements Converter<ReferenceAble, DocumentPointer<Object>> {

		@Nullable
		@Override
		public DocumentPointer<Object> convert(ReferenceAble source) {
			return source::toReference;
		}
	}

	@WritingConverter
	static class DocumentToSimpleObjectRefWithReadingConverter
			implements Converter<DocumentPointer<Document>, SimpleObjectRefWithReadingConverter> {

		@Nullable
		@Override
		public SimpleObjectRefWithReadingConverter convert(DocumentPointer<Document> source) {

			Document document = client.getDatabase(DB_NAME).getCollection("simple-object-ref")
					.find(Filters.eq("_id", source.getPointer().get("ref-key-from-custom-write-converter"))).first();
			return new SimpleObjectRefWithReadingConverter(document.getString("_id"), document.getString("value"));
		}
	}

	@WritingConverter
	static class SimpleObjectRefWithReadingConverterToDocumentConverter
			implements Converter<SimpleObjectRefWithReadingConverter, DocumentPointer<Document>> {

		@Nullable
		@Override
		public DocumentPointer<Document> convert(SimpleObjectRefWithReadingConverter source) {
			return () -> new Document("ref-key-from-custom-write-converter", source.getId());
		}
	}

	static class WithRefA/* to B */ implements ReferenceAble {

		@Id String id;
		@DocumentReference //
		WithRefB toB;

		@Override
		public Object toReference() {
			return id;
		}

		public String getId() {
			return this.id;
		}

		public WithRefB getToB() {
			return this.toB;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setToB(WithRefB toB) {
			this.toB = toB;
		}
	}

	static class WithRefB/* to A */ implements ReferenceAble {

		@Id String id;
		@DocumentReference(lazy = true) //
		WithRefA lazyToA;

		@DocumentReference //
		WithRefA eagerToA;

		@Override
		public Object toReference() {
			return id;
		}

		public String getId() {
			return this.id;
		}

		public WithRefA getLazyToA() {
			return this.lazyToA;
		}

		public WithRefA getEagerToA() {
			return this.eagerToA;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setLazyToA(WithRefA lazyToA) {
			this.lazyToA = lazyToA;
		}

		public void setEagerToA(WithRefA eagerToA) {
			this.eagerToA = eagerToA;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.WithRefB(id=" + this.getId() + ", lazyToA=" + this.getLazyToA()
					+ ", eagerToA=" + this.getEagerToA() + ")";
		}
	}

	static class ReferencedObject {}

	class ToDocumentPointerConverter implements Converter<ReferencedObject, DocumentPointer<Document>> {

		@Nullable
		@Override
		public DocumentPointer<Document> convert(ReferencedObject source) {
			return () -> new Document("", source);
		}
	}

	static class Book {

		String id;

		@DocumentReference(lookup = "{ 'acronym' : ?#{acc}, 'name' : ?#{n} }") //
		Publisher publisher;

		public String getId() {
			return this.id;
		}

		public Publisher getPublisher() {
			return this.publisher;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setPublisher(Publisher publisher) {
			this.publisher = publisher;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.Book(id=" + this.getId() + ", publisher=" + this.getPublisher() + ")";
		}
	}

	static class Publisher {

		String id;
		String acronym;
		String name;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getAcronym() {
			return acronym;
		}

		public void setAcronym(String acronym) {
			this.acronym = acronym;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	static class UsingAtReference {

		String id;

		@Reference //
		Publisher publisher;

		public String getId() {
			return this.id;
		}

		public Publisher getPublisher() {
			return this.publisher;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setPublisher(Publisher publisher) {
			this.publisher = publisher;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.UsingAtReference(id=" + this.getId() + ", publisher="
					+ this.getPublisher() + ")";
		}
	}

	static class OneToManyStyleBook {

		@Id
		String id;

		private String publisherId;

		public String getId() {
			return this.id;
		}

		public String getPublisherId() {
			return this.publisherId;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setPublisherId(String publisherId) {
			this.publisherId = publisherId;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			OneToManyStyleBook that = (OneToManyStyleBook) o;
			return Objects.equals(id, that.id) && Objects.equals(publisherId, that.publisherId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, publisherId);
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.OneToManyStyleBook(id=" + this.getId() + ", publisherId="
					+ this.getPublisherId() + ")";
		}
	}

	static class OneToManyStylePublisher {

		@Id
		String id;

		@ReadOnlyProperty
		@DocumentReference(lookup="{'publisherId':?#{#self._id} }")
		List<OneToManyStyleBook> books;

		public String getId() {
			return this.id;
		}

		public List<OneToManyStyleBook> getBooks() {
			return this.books;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setBooks(List<OneToManyStyleBook> books) {
			this.books = books;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.OneToManyStylePublisher(id=" + this.getId() + ", books="
					+ this.getBooks() + ")";
		}
	}

	static class WithRequiredArgsCtor {

		final String id;

		@DocumentReference final Publisher publisher;

		public WithRequiredArgsCtor(String id, Publisher publisher) {

			this.id = id;
			this.publisher = publisher;
		}
	}

	static class WithLazyRequiredArgsCtor {

		final String id;

		@DocumentReference(lazy = true) final Publisher publisher;

		public WithLazyRequiredArgsCtor(String id, Publisher publisher) {

			this.id = id;
			this.publisher = publisher;
		}

		public String getId() {
			return id;
		}

		public Publisher getPublisher() {
			return publisher;
		}
	}

	public static class WithListOfRefs {

		@Id private String id;

		@DocumentReference private List<WithListOfRefs> refs;

		public String getId() {
			return this.id;
		}

		public List<WithListOfRefs> getRefs() {
			return this.refs;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setRefs(List<WithListOfRefs> refs) {
			this.refs = refs;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.WithListOfRefs(id=" + this.getId() + ", refs=" + this.getRefs() + ")";
		}
	}

	static class OneToManyStylePublisherWithRequiredArgsCtor {

		@Id
		String id;

		@ReadOnlyProperty
		@DocumentReference(lookup="{'publisherId':?#{#self._id} }")
		List<OneToManyStyleBook> books;

		public OneToManyStylePublisherWithRequiredArgsCtor(String id, List<OneToManyStyleBook> books) {
			this.id = id;
			this.books = books;
		}

		public String getId() {
			return this.id;
		}

		public List<OneToManyStyleBook> getBooks() {
			return this.books;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setBooks(List<OneToManyStyleBook> books) {
			this.books = books;
		}

		public String toString() {
			return "MongoTemplateDocumentReferenceTests.OneToManyStylePublisherWithRequiredArgsCtor(id=" + this.getId() + ", book="
				+ this.getBooks() + ")";
		}
	}
}
