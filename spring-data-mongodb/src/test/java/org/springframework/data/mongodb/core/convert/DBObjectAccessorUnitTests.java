/*
 * Copyright 2013-2017 the original author or authors.
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

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link DBObjectAccessor}.
 * 
 * @author Oliver Gierke
 */
public class DBObjectAccessorUnitTests {

	MongoMappingContext context = new MongoMappingContext();
	MongoPersistentEntity<?> projectingTypeEntity = context.getPersistentEntity(ProjectingType.class);
	MongoPersistentProperty fooProperty = projectingTypeEntity.getPersistentProperty("foo");

	@Test // DATAMONGO-766
	public void putsNestedFieldCorrectly() {

		DBObject dbObject = new BasicDBObject();

		DBObjectAccessor accessor = new DBObjectAccessor(dbObject);
		accessor.put(fooProperty, "FooBar");

		DBObject aDbObject = DBObjectTestUtils.getAsDBObject(dbObject, "a");
		assertThat(aDbObject.get("b"), is((Object) "FooBar"));
	}

	@Test // DATAMONGO-766
	public void getsNestedFieldCorrectly() {

		DBObject source = new BasicDBObject("a", new BasicDBObject("b", "FooBar"));

		DBObjectAccessor accessor = new DBObjectAccessor(source);
		assertThat(accessor.get(fooProperty), is((Object) "FooBar"));
	}

	@Test // DATAMONGO-766
	public void returnsNullForNonExistingFieldPath() {

		DBObjectAccessor accessor = new DBObjectAccessor(new BasicDBObject());
		assertThat(accessor.get(fooProperty), is(nullValue()));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-766
	public void rejectsNonBasicDBObjects() {
		new DBObjectAccessor(new BasicDBList());
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-766
	public void rejectsNullDBObject() {
		new DBObjectAccessor(null);
	}

	@Test // DATAMONGO-1335
	public void writesAllNestingsCorrectly() {

		MongoPersistentEntity<?> entity = context.getPersistentEntity(TypeWithTwoNestings.class);

		BasicDBObject target = new BasicDBObject();

		DBObjectAccessor accessor = new DBObjectAccessor(target);
		accessor.put(entity.getPersistentProperty("id"), "id");
		accessor.put(entity.getPersistentProperty("b"), "b");
		accessor.put(entity.getPersistentProperty("c"), "c");

		DBObject nestedA = DBObjectTestUtils.getAsDBObject(target, "a");

		assertThat(nestedA, is(notNullValue()));
		assertThat(nestedA.get("b"), is((Object) "b"));
		assertThat(nestedA.get("c"), is((Object) "c"));
	}

	@Test // DATAMONGO-1471
	public void exposesAvailabilityOfFields() {

		DBObjectAccessor accessor = new DBObjectAccessor(new BasicDBObject("a", new BasicDBObject("c", "d")));
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
