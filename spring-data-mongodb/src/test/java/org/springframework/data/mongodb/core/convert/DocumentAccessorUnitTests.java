/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.mongodb.BasicDBObject;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Unit tests for {@link DocumentAccessor}.
 * 
 * @see DATAMONGO-766
 * @author Oliver Gierke
 */
public class DocumentAccessorUnitTests {

	MongoMappingContext context = new MongoMappingContext();
	MongoPersistentEntity<?> projectingTypeEntity = context.getPersistentEntity(ProjectingType.class);
	MongoPersistentProperty fooProperty = projectingTypeEntity.getPersistentProperty("foo");

	@Test
	public void putsNestedFieldCorrectly() {

		Document dbObject = new Document();

		DocumentAccessor accessor = new DocumentAccessor(dbObject);
		accessor.put(fooProperty, "FooBar");

		Document aDbObject = DBObjectTestUtils.getAsDocument(dbObject, "a");
		assertThat(aDbObject.get("b"), is((Object) "FooBar"));
	}

	@Test
	public void getsNestedFieldCorrectly() {

		Document source = new Document("a", new Document("b", "FooBar"));

		DocumentAccessor accessor = new DocumentAccessor(source);
		assertThat(accessor.get(fooProperty), is((Object) "FooBar"));
	}

	@Test
	public void returnsNullForNonExistingFieldPath() {

		DocumentAccessor accessor = new DocumentAccessor(new Document());
		assertThat(accessor.get(fooProperty), is(nullValue()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNonBasicDocuments() {
		new DocumentAccessor(new BsonDocument());
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullDocument() {
		new DocumentAccessor(null);
	}

	/**
	 * @see DATAMONGO-1335
	 */
	@Test
	public void writesAllNestingsCorrectly() {

		MongoPersistentEntity<?> entity = context.getPersistentEntity(TypeWithTwoNestings.class);

		Document target = new Document();

		DocumentAccessor accessor = new DocumentAccessor(target);
		accessor.put(entity.getPersistentProperty("id"), "id");
		accessor.put(entity.getPersistentProperty("b"), "b");
		accessor.put(entity.getPersistentProperty("c"), "c");

		Document nestedA = DBObjectTestUtils.getAsDocument(target, "a");

		assertThat(nestedA, is(notNullValue()));
		assertThat(nestedA.get("b"), is((Object) "b"));
		assertThat(nestedA.get("c"), is((Object) "c"));
	}

	/**
	 * @see DATAMONGO-1471
	 */
	@Test
	public void exposesAvailabilityOfFields() {

		DocumentAccessor accessor = new DocumentAccessor(new Document("a", new BasicDBObject("c", "d")));
		MongoPersistentEntity<?> entity = context.getPersistentEntity(ProjectingType.class);

		assertThat(accessor.hasValue(entity.getPersistentProperty("foo")), is(false));
		assertThat(accessor.hasValue(entity.getPersistentProperty("a")), is(true));
		assertThat(accessor.hasValue(entity.getPersistentProperty("name")), is(false));
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
