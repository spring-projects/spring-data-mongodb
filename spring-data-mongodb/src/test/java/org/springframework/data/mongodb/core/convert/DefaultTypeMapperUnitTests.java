/*
 * Copyright 2011 the original author or authors.
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

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link DefaultTypeMapper}.
 * 
 * @author Oliver Gierke
 */
public class DefaultTypeMapperUnitTests {

	DefaultTypeMapper mapper;

	@Before
	public void setUp() {
		mapper = new DefaultTypeMapper();
	}

	@Test
	public void addsFullyQualifiedClassNameUnderDefaultKeyByDefault() {
		writesTypeToField(DefaultTypeMapper.DEFAULT_TYPE_KEY, new BasicDBObject(), String.class);
	}

	@Test
	public void writesTypeToCustomFieldIfConfigured() {
		mapper.setTypeKey("_custom");
		writesTypeToField("_custom", new BasicDBObject(), String.class);
	}

	@Test
	public void doesNotWriteTypeInformationInCaseKeyIsSetToNull() {
		mapper.setTypeKey(null);
		writesTypeToField(null, new BasicDBObject(), String.class);
	}

	@Test
	public void readsTypeFromDefaultKeyByDefault() {
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, String.class.getName()), String.class);
	}

	@Test
	public void readsTypeFromCustomFieldConfigured() {
		mapper.setTypeKey("_custom");
		readsTypeFromField(new BasicDBObject("_custom", String.class.getName()), String.class);
	}

	@Test
	public void returnsListForBasicDBLists() {
		readsTypeFromField(new BasicDBList(), List.class);
	}

	@Test
	public void returnsNullIfNoTypeInfoInDBObject() {
		readsTypeFromField(new BasicDBObject(), null);
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, ""), null);
	}

	@Test
	public void returnsNullIfClassCannotBeLoaded() {
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, "fooBar"), null);
	}
	
	@Test
	public void returnsNullIfTypeKeySetToNull() {
		mapper.setTypeKey(null);
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, String.class), null);
	}
	
	@Test
	public void returnsCorrectTypeKey() {
		
		assertThat(mapper.isTypeKey(DefaultTypeMapper.DEFAULT_TYPE_KEY), is(true));
		
		mapper.setTypeKey("_custom");
		assertThat(mapper.isTypeKey("_custom"), is(true));
		assertThat(mapper.isTypeKey(DefaultTypeMapper.DEFAULT_TYPE_KEY), is(false));
		
		mapper.setTypeKey(null);
		assertThat(mapper.isTypeKey("_custom"), is(false));
		assertThat(mapper.isTypeKey(DefaultTypeMapper.DEFAULT_TYPE_KEY), is(false));
	}

	private void readsTypeFromField(DBObject dbObject, Class<?> type) {

		TypeInformation<?> typeInfo = mapper.readType(dbObject);

		if (type != null) {
			assertThat(typeInfo, is(notNullValue()));
			assertThat(typeInfo.getType(), is(typeCompatibleWith(type)));
		} else {
			assertThat(typeInfo, is(nullValue()));
		}
	}

	private void writesTypeToField(String field, DBObject dbObject, Class<?> type) {

		mapper.writeType(type, dbObject);

		if (field == null) {
			assertThat(dbObject.keySet().isEmpty(), is(true));
		} else {
			assertThat(dbObject.containsField(field), is(true));
			assertThat(dbObject.get(field), is((Object) type.getName()));
		}
	}
}
