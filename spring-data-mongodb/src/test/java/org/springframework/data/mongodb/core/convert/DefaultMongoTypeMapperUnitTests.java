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

import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.ConfigurableTypeInformationMapper;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link ConfigurableTypeMapper}.
 * 
 * @author Oliver Gierke
 */
public class DefaultMongoTypeMapperUnitTests {

	ConfigurableTypeInformationMapper configurableTypeInformationMapper;
	SimpleTypeInformationMapper simpleTypeInformationMapper;

	DefaultMongoTypeMapper typeMapper;

	@Before
	public void setUp() {

		configurableTypeInformationMapper = new ConfigurableTypeInformationMapper(Collections.singletonMap(String.class,
				"1"));
		simpleTypeInformationMapper = SimpleTypeInformationMapper.INSTANCE;

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper));
	}

	@Test
	public void defaultInstanceWritesClasses() {

		typeMapper = new DefaultMongoTypeMapper();
		writesTypeToField(new BasicDBObject(), String.class, String.class.getName());
	}

	@Test
	public void defaultInstanceReadsClasses() {
		typeMapper = new DefaultMongoTypeMapper();
		DBObject dbObject = new BasicDBObject(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, String.class.getName());
		readsTypeFromField(dbObject, String.class);
	}

	@Test
	public void writesMapKeyForType() {
		writesTypeToField(new BasicDBObject(), String.class, "1");
		writesTypeToField(new BasicDBObject(), Object.class, null);
	}

	@Test
	public void writesClassNamesForUnmappedValuesIfConfigured() {

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Arrays.asList(
				configurableTypeInformationMapper, simpleTypeInformationMapper));

		writesTypeToField(new BasicDBObject(), String.class, "1");
		writesTypeToField(new BasicDBObject(), Object.class, Object.class.getName());
	}

	@Test
	public void readsTypeForMapKey() {
		readsTypeFromField(new BasicDBObject(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "1"), String.class);
		readsTypeFromField(new BasicDBObject(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "unmapped"), null);
	}

	@Test
	public void readsTypeLoadingClassesForUnmappedTypesIfConfigured() {

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Arrays.asList(
				configurableTypeInformationMapper, simpleTypeInformationMapper));

		readsTypeFromField(new BasicDBObject(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "1"), String.class);
		readsTypeFromField(new BasicDBObject(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Object.class.getName()), Object.class);
	}

	private void readsTypeFromField(DBObject dbObject, Class<?> type) {

		TypeInformation<?> typeInfo = typeMapper.readType(dbObject);

		if (type != null) {
			assertThat(typeInfo, is(notNullValue()));
			assertThat(typeInfo.getType(), is(typeCompatibleWith(type)));
		} else {
			assertThat(typeInfo, is(nullValue()));
		}
	}

	private void writesTypeToField(DBObject dbObject, Class<?> type, Object value) {

		typeMapper.writeType(type, dbObject);

		if (value == null) {
			assertThat(dbObject.keySet().isEmpty(), is(true));
		} else {
			assertThat(dbObject.containsField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(true));
			assertThat(dbObject.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(value));
		}
	}
}
