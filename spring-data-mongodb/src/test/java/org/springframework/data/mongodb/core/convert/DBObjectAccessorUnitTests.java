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

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectUtils;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link DbObjectAccessor}.
 * 
 * @see DATAMONGO-766
 * @author Oliver Gierke
 */
public class DBObjectAccessorUnitTests {

	MongoMappingContext context = new MongoMappingContext();
	MongoPersistentEntity<?> projectingTypeEntity = context.getPersistentEntity(ProjectingType.class);
	MongoPersistentProperty fooProperty = projectingTypeEntity.getPersistentProperty("foo");

	@Test
	public void putsNestedFieldCorrectly() {

		DBObject dbObject = new BasicDBObject();

		DBObjectAccessor accessor = new DBObjectAccessor(dbObject);
		accessor.put(fooProperty, "FooBar");

		DBObject aDbObject = DBObjectUtils.getAsDBObject(dbObject, "a");
		assertThat(aDbObject.get("b"), is((Object) "FooBar"));
	}

	@Test
	public void getsNestedFieldCorrectly() {

		DBObject source = new BasicDBObject("a", new BasicDBObject("b", "FooBar"));

		DBObjectAccessor accessor = new DBObjectAccessor(source);
		assertThat(accessor.get(fooProperty), is((Object) "FooBar"));
	}

	@Test
	public void returnsNullForNonExistingFieldPath() {

		DBObjectAccessor accessor = new DBObjectAccessor(new BasicDBObject());
		assertThat(accessor.get(fooProperty), is(nullValue()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNonBasicDBObjects() {
		new DBObjectAccessor(new BasicDBList());
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullDBObject() {
		new DBObjectAccessor(null);
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

}
