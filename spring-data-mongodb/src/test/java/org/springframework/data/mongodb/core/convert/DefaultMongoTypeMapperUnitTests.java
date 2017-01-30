/*
 * Copyright 2011-2017 the original author or authors.
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
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.convert.ConfigurableTypeInformationMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.util.TypeInformation;

/**
 * Unit tests for {@link DefaultMongoTypeMapper}.
 * 
 * @author Oliver Gierke
 */
public class DefaultMongoTypeMapperUnitTests {

	ConfigurableTypeInformationMapper configurableTypeInformationMapper;
	SimpleTypeInformationMapper simpleTypeInformationMapper;

	DefaultMongoTypeMapper typeMapper;

	@Before
	public void setUp() {

		configurableTypeInformationMapper = new ConfigurableTypeInformationMapper(
				Collections.singletonMap(String.class, "1"));
		simpleTypeInformationMapper = new SimpleTypeInformationMapper();

		typeMapper = new DefaultMongoTypeMapper();
	}

	@Test
	public void defaultInstanceWritesClasses() {

		writesTypeToField(new Document(), String.class, String.class.getName());
	}

	@Test
	public void defaultInstanceReadsClasses() {

		Document document = new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, String.class.getName());
		readsTypeFromField(document, String.class);
	}

	@Test
	public void writesMapKeyForType() {

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper));

		writesTypeToField(new Document(), String.class, "1");
		writesTypeToField(new Document(), Object.class, null);
	}

	@Test
	public void writesClassNamesForUnmappedValuesIfConfigured() {

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper, simpleTypeInformationMapper));

		writesTypeToField(new Document(), String.class, "1");
		writesTypeToField(new Document(), Object.class, Object.class.getName());
	}

	@Test
	public void readsTypeForMapKey() {

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper));

		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "1"), String.class);
		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "unmapped"), null);
	}

	@Test
	public void readsTypeLoadingClassesForUnmappedTypesIfConfigured() {

		typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY,
				Arrays.asList(configurableTypeInformationMapper, simpleTypeInformationMapper));

		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "1"), String.class);
		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Object.class.getName()), Object.class);
	}

	@Test // DATAMONGO-709
	public void writesTypeRestrictionsCorrectly() {

		Document result = new Document();

		typeMapper = new DefaultMongoTypeMapper();
		typeMapper.writeTypeRestrictions(result, Collections.<Class<?>> singleton(String.class));

		Document typeInfo = DocumentTestUtils.getAsDocument(result, DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);
		List<Object> aliases = DocumentTestUtils.getAsDBList(typeInfo, "$in");
		assertThat(aliases, hasSize(1));
		assertThat(aliases.get(0), is((Object) String.class.getName()));
	}

	@Test
	public void addsFullyQualifiedClassNameUnderDefaultKeyByDefault() {
		writesTypeToField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, new Document(), String.class);
	}

	@Test
	public void writesTypeToCustomFieldIfConfigured() {
		typeMapper = new DefaultMongoTypeMapper("_custom");
		writesTypeToField("_custom", new Document(), String.class);
	}

	@Test
	public void doesNotWriteTypeInformationInCaseKeyIsSetToNull() {
		typeMapper = new DefaultMongoTypeMapper(null);
		writesTypeToField(null, new Document(), String.class);
	}

	@Test
	public void readsTypeFromDefaultKeyByDefault() {
		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, String.class.getName()), String.class);
	}

	@Test
	public void readsTypeFromCustomFieldConfigured() {

		typeMapper = new DefaultMongoTypeMapper("_custom");
		readsTypeFromField(new Document("_custom", String.class.getName()), String.class);
	}

	@Test
	public void returnsListForBasicDBLists() {
		readsTypeFromField(new Document(), null);
	}

	@Test
	public void returnsNullIfNoTypeInfoInDocument() {
		readsTypeFromField(new Document(), null);
		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, ""), null);
	}

	@Test
	public void returnsNullIfClassCannotBeLoaded() {
		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, "fooBar"), null);
	}

	@Test
	public void returnsNullIfTypeKeySetToNull() {
		typeMapper = new DefaultMongoTypeMapper(null);
		readsTypeFromField(new Document(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, String.class), null);
	}

	@Test
	public void returnsCorrectTypeKey() {

		assertThat(typeMapper.isTypeKey(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(true));

		typeMapper = new DefaultMongoTypeMapper("_custom");
		assertThat(typeMapper.isTypeKey("_custom"), is(true));
		assertThat(typeMapper.isTypeKey(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(false));

		typeMapper = new DefaultMongoTypeMapper(null);
		assertThat(typeMapper.isTypeKey("_custom"), is(false));
		assertThat(typeMapper.isTypeKey(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(false));
	}

	private void readsTypeFromField(Document document, Class<?> type) {

		Optional<TypeInformation<?>> typeInfo = typeMapper.readType(document);

		if (type != null) {
			assertThat(typeInfo, is(notNullValue()));
			assertThat(typeInfo.get().getType(), is(typeCompatibleWith(type)));
		} else {
			assertThat(typeInfo, is(Optional.empty()));
		}
	}

	private void writesTypeToField(String field, Document document, Class<?> type) {

		typeMapper.writeType(type, document);

		if (field == null) {
			assertThat(document.keySet().isEmpty(), is(true));
		} else {
			assertThat(document.containsKey(field), is(true));
			assertThat(document.get(field), is((Object) type.getName()));
		}
	}

	private void writesTypeToField(Document document, Class<?> type, Object value) {

		typeMapper.writeType(type, document);

		if (value == null) {
			assertThat(document.keySet().isEmpty(), is(true));
		} else {
			assertThat(document.containsKey(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(true));
			assertThat(document.get(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY), is(value));
		}
	}
}
