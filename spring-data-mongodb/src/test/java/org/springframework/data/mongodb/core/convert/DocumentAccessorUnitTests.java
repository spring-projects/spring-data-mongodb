/*
 * Copyright 2013-2024 the original author or authors.
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

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.BasicDBObject;

/**
 * Unit tests for {@link DocumentAccessor}.
 *
 * @author Oliver Gierke
 */
public class DocumentAccessorUnitTests {

	MongoMappingContext context = new MongoMappingContext();
	MongoPersistentEntity<?> projectingTypeEntity = context.getRequiredPersistentEntity(ProjectingType.class);
	MongoPersistentProperty fooProperty = projectingTypeEntity.getRequiredPersistentProperty("foo");

	@Test // DATAMONGO-766
	public void putsNestedFieldCorrectly() {

		Document document = new Document();

		DocumentAccessor accessor = new DocumentAccessor(document);
		accessor.put(fooProperty, "FooBar");

		Document aDocument = DocumentTestUtils.getAsDocument(document, "a");
		assertThat(aDocument.get("b")).isEqualTo((Object) "FooBar");
	}

	@Test // DATAMONGO-766
	public void getsNestedFieldCorrectly() {

		Document source = new Document("a", new Document("b", "FooBar"));

		DocumentAccessor accessor = new DocumentAccessor(source);
		assertThat(accessor.get(fooProperty)).isEqualTo((Object) "FooBar");
	}

	@Test // DATAMONGO-766
	public void returnsNullForNonExistingFieldPath() {

		DocumentAccessor accessor = new DocumentAccessor(new Document());
		assertThat(accessor.get(fooProperty)).isNull();
	}

	@Test // DATAMONGO-766
	public void rejectsNonDocuments() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DocumentAccessor(new BsonDocument()));
	}

	@Test // DATAMONGO-766
	public void rejectsNullDocument() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DocumentAccessor(null));
	}

	@Test // DATAMONGO-1335
	public void writesAllNestingsCorrectly() {

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(TypeWithTwoNestings.class);

		Document target = new Document();

		DocumentAccessor accessor = new DocumentAccessor(target);
		accessor.put(entity.getRequiredPersistentProperty("id"), "id");
		accessor.put(entity.getRequiredPersistentProperty("b"), "b");
		accessor.put(entity.getRequiredPersistentProperty("c"), "c");

		Document nestedA = DocumentTestUtils.getAsDocument(target, "a");

		assertThat(nestedA).isNotNull();
		assertThat(nestedA.get("b")).isEqualTo((Object) "b");
		assertThat(nestedA.get("c")).isEqualTo((Object) "c");
	}

	@Test // DATAMONGO-1471
	public void exposesAvailabilityOfFields() {

		DocumentAccessor accessor = new DocumentAccessor(new Document("a", new BasicDBObject("c", "d")));
		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(ProjectingType.class);

		assertThat(accessor.hasValue(entity.getRequiredPersistentProperty("foo"))).isFalse();
		assertThat(accessor.hasValue(entity.getRequiredPersistentProperty("a"))).isTrue();
		assertThat(accessor.hasValue(entity.getRequiredPersistentProperty("name"))).isFalse();
	}

	static class ProjectingType {

		String name;
		@Field("a.b") String foo;
		NestedType a;
	}

	static class NestedType {
		String b;
		String c;
	}

	static class TypeWithTwoNestings {

		String id;
		@Field("a.b") String b;
		@Field("a.c") String c;
	}
}
