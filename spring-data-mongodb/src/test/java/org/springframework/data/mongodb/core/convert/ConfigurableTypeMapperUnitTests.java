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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link ConfigurableTypeMapper}.
 *
 * @author Oliver Gierke
 */
public class ConfigurableTypeMapperUnitTests {
	
	ConfigurableTypeMapper mapper;
	
	@Before
	public void setUp() {
		mapper = new ConfigurableTypeMapper(Collections.singletonMap(String.class, "1"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullTypeMap() {
		new ConfigurableTypeMapper(null);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNonBijectionalMap() {
		Map<Class<?>, String> map = new HashMap<Class<?>, String>();
		map.put(String.class, "1");
		map.put(Object.class, "1");
		
		new ConfigurableTypeMapper(map);
	}
	
	@Test
	public void writesMapKeyForType() {
		writesTypeToField(new BasicDBObject(), String.class, "1");
		writesTypeToField(new BasicDBObject(), Object.class, null);
	}
	
	@Test
	public void writesClassNamesForUnmappedValuesIfConfigured() {
		mapper.setHandleUnmappedClasses(true);
		writesTypeToField(new BasicDBObject(), String.class, "1");
		writesTypeToField(new BasicDBObject(), Object.class, Object.class.getName());
	}
	
	@Test
	public void readsTypeForMapKey() {
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, "1"), String.class);
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, "unmapped"), null);
	}
	
	@Test
	public void readsTypeLoadingClassesForUnmappedTypesIfConfigured() {
		mapper.setHandleUnmappedClasses(true);
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, "1"), String.class);
		readsTypeFromField(new BasicDBObject(DefaultTypeMapper.DEFAULT_TYPE_KEY, Object.class.getName()), Object.class);
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

	private void writesTypeToField(DBObject dbObject, Class<?> type, Object value) {

		mapper.writeType(type, dbObject);

		if (value == null) {
			assertThat(dbObject.keySet().isEmpty(), is(true));
		} else {
			assertThat(dbObject.containsField(DefaultTypeMapper.DEFAULT_TYPE_KEY), is(true));
			assertThat(dbObject.get(DefaultTypeMapper.DEFAULT_TYPE_KEY), is(value));
		}
	}
}
