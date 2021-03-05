/*
 * Copyright 2021 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.LazyLoadingTestUtils;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.ObjectReference;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;

/**
 * {@link DBRef} related integration tests for {@link MongoTemplate}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
public class MongoTemplateDocumentReferenceTests {

	public static final String DB_NAME = "manual-reference-tests";

	static @Client MongoClient client;

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureConversion(it -> {
			it.customConverters(new ReferencableConverter());
		});

		cfg.configureMappingContext(it -> {
			it.autocreateIndex(false);
		});
	});

	@BeforeEach
	public void setUp() {
		template.flushDatabase();
	}

	@Test
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

	@Test
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

		System.out.println("target: " + target.toJson());
		assertThat(target.get("mapValueRef", Map.class)).containsEntry("frodo", "ref-1").containsEntry("bilbo", "ref-2");
	}

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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
		System.out.println("result: " + result);

		assertThat(result.getMapValueRef()).containsEntry("frodo",
				new SimpleObjectRef("ref-1", "me-the-1-referenced-object"))
				.containsEntry("bilbo",
						new SimpleObjectRef("ref-2", "me-the-2-referenced-object"));
	}

	@Data
	static class SingleRefRoot {

		String id;
		String value;

		@DocumentReference SimpleObjectRefWithReadingConverter withReadingConverter;

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
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
	}

	@Data
	static class CollectionRefRoot {

		String id;
		String value;

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		List<SimpleObjectRef> simpleValueRef;

		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		Map<String, SimpleObjectRef> mapValueRef;

		@Field("simple-value-ref-annotated-field-name") //
		@DocumentReference(lookup = "{ '_id' : '?#{#target}' }") //
		List<SimpleObjectRef> simpleValueRefWithAnnotatedFieldName;

		@DocumentReference(lookup = "{ '_id' : '?#{id}' }") //
		List<ObjectRefOfDocument> objectValueRef;

		@DocumentReference(lookup = "{ '_id' : '?#{id}' }", collection = "?#{collection}") //
		List<ObjectRefOfDocumentWithEmbeddedCollectionName> objectValueRefWithEmbeddedCollectionName;

		@DocumentReference(lookup = "{ 'refKey1' : '?#{refKey1}', 'refKey2' : '?#{refKey2}' }") //
		List<ObjectRefOnNonIdField> objectValueRefOnNonIdFields;
	}

	@FunctionalInterface
	interface ReferenceAble {
		Object toReference();
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document("simple-object-ref")
	static class SimpleObjectRef {

		@Id String id;
		String value;

	}

	@Getter
	@Setter
	static class SimpleObjectRefWithReadingConverter extends SimpleObjectRef {

		public SimpleObjectRefWithReadingConverter(String id, String value, String id1, String value1) {
			super(id, value);
		}
	}

	@Data
	@AllArgsConstructor
	static class ObjectRefOfDocument implements ReferenceAble {

		@Id String id;
		String value;

		@Override
		public Object toReference() {
			return new Document("id", id).append("property", "without-any-meaning");
		}
	}

	@Data
	@AllArgsConstructor
	static class ObjectRefOfDocumentWithEmbeddedCollectionName implements ReferenceAble {

		@Id String id;
		String value;

		@Override
		public Object toReference() {
			return new Document("id", id).append("collection", "object-ref-of-document-with-embedded-collection-name");
		}
	}

	@Data
	@AllArgsConstructor
	static class ObjectRefOnNonIdField implements ReferenceAble {

		@Id String id;
		String value;
		String refKey1;
		String refKey2;

		@Override
		public Object toReference() {
			return new Document("refKey1", refKey1).append("refKey2", refKey2);
		}
	}

	static class ReferencableConverter implements Converter<ReferenceAble, ObjectReference> {

		@Nullable
		@Override
		public ObjectReference convert(ReferenceAble source) {
			return source::toReference;
		}
	}

	@WritingConverter
	class DocumentToSimpleObjectRefWithReadingConverter
			implements Converter<ObjectReference<Document>, SimpleObjectRefWithReadingConverter> {

		private final MongoTemplate template;

		public DocumentToSimpleObjectRefWithReadingConverter(MongoTemplate template) {
			this.template = template;
		}

		@Nullable
		@Override
		public SimpleObjectRefWithReadingConverter convert(ObjectReference<Document> source) {
			return template.findOne(query(where("id").is(source.getPointer().get("the-ref-key-you-did-not-expect"))),
					SimpleObjectRefWithReadingConverter.class);
		}
	}

	@WritingConverter
	class SimpleObjectRefWithReadingConverterToDocumentConverter
			implements Converter<SimpleObjectRefWithReadingConverter, ObjectReference<Document>> {

		@Nullable
		@Override
		public ObjectReference<Document> convert(SimpleObjectRefWithReadingConverter source) {
			return () -> new Document("the-ref-key-you-did-not-expect", source.getId());
		}
	}
}
