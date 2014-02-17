/*
 * Copyright 2013-2014 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.DBObjectUtils.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link UpdateMapper}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class UpdateMapperUnitTests {

	@Mock MongoDbFactory factory;
	MappingMongoConverter converter;
	MongoMappingContext context;
	UpdateMapper mapper;

	private Converter<NestedEntity, DBObject> writingConverterSpy;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {

		this.writingConverterSpy = Mockito.spy(new NestedEntityWriteConverter());
		CustomConversions conversions = new CustomConversions(Arrays.asList(writingConverterSpy));

		this.context = new MongoMappingContext();
		this.context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		this.context.initialize();

		this.converter = new MappingMongoConverter(factory, context);
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();

		this.mapper = new UpdateMapper(converter);
	}

	/**
	 * @see DATAMONGO-721
	 */
	@Test
	public void updateMapperRetainsTypeInformationForCollectionField() {

		Update update = new Update().push("list", new ConcreteChildClass("2", "BAR"));
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject list = getAsDBObject(push, "aliased");

		assertThat(list.get("_class"), is((Object) ConcreteChildClass.class.getName()));
	}

	/**
	 * @see DATAMONGO-807
	 */
	@Test
	public void updateMapperShouldRetainTypeInformationForNestedEntities() {

		Update update = Update.update("model", new ModelImpl(1));
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		DBObject modelDbObject = (DBObject) set.get("model");
		assertThat(modelDbObject.get("_class"), not(nullValue()));
	}

	/**
	 * @see DATAMONGO-807
	 */
	@Test
	public void updateMapperShouldNotPersistTypeInformationForKnownSimpleTypes() {

		Update update = Update.update("model.value", 1);
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		assertThat(set.get("_class"), nullValue());
	}

	/**
	 * @see DATAMONGO-807
	 */
	@Test
	public void updateMapperShouldNotPersistTypeInformationForNullValues() {

		Update update = Update.update("model", null);
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		assertThat(set.get("_class"), nullValue());
	}

	/**
	 * @see DATAMONGO-407
	 */
	@Test
	public void updateMapperShouldRetainTypeInformationForNestedCollectionElements() {

		Update update = Update.update("list.$", new ConcreteChildClass("42", "bubu"));

		UpdateMapper mapper = new UpdateMapper(converter);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		DBObject modelDbObject = getAsDBObject(set, "aliased.$");
		assertThat(modelDbObject.get("_class"), is((Object) ConcreteChildClass.class.getName()));
	}

	/**
	 * @see DATAMONGO-407
	 */
	@Test
	public void updateMapperShouldSupportNestedCollectionElementUpdates() {

		Update update = Update.update("list.$.value", "foo").set("list.$.otherValue", "bar");

		UpdateMapper mapper = new UpdateMapper(converter);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		assertThat(set.get("aliased.$.value"), is((Object) "foo"));
		assertThat(set.get("aliased.$.otherValue"), is((Object) "bar"));
	}

	/**
	 * @see DATAMONGO-407
	 */
	@Test
	public void updateMapperShouldWriteTypeInformationForComplexNestedCollectionElementUpdates() {

		Update update = Update.update("list.$.value", "foo").set("list.$.someObject", new ConcreteChildClass("42", "bubu"));

		UpdateMapper mapper = new UpdateMapper(converter);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject dbo = getAsDBObject(mappedObject, "$set");
		assertThat(dbo.get("aliased.$.value"), is((Object) "foo"));

		DBObject someObject = getAsDBObject(dbo, "aliased.$.someObject");
		assertThat(someObject, is(notNullValue()));
		assertThat(someObject.get("_class"), is((Object) ConcreteChildClass.class.getName()));
		assertThat(someObject.get("value"), is((Object) "bubu"));
	}

	/**
	 * @see DATAMONGO-410
	 */
	@Test
	public void testUpdateMapperShouldConsiderCustomWriteTarget() {

		List<NestedEntity> someValues = Arrays.asList(new NestedEntity("spring"), new NestedEntity("data"),
				new NestedEntity("mongodb"));
		NestedEntity[] array = new NestedEntity[someValues.size()];

		Update update = new Update().pushAll("collectionOfNestedEntities", someValues.toArray(array));
		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DomainEntity.class));

		verify(writingConverterSpy, times(3)).convert(Mockito.any(NestedEntity.class));
	}

	static interface Model {}

	static class ModelImpl implements Model {
		public int value;

		public ModelImpl(int value) {
			this.value = value;
		}
	}

	public class ModelWrapper {
		Model model;
	}

	static class ParentClass {

		String id;

		@Field("aliased")//
		List<? extends AbstractChildClass> list;

		public ParentClass(String id, List<? extends AbstractChildClass> list) {
			this.id = id;
			this.list = list;
		}
	}

	static abstract class AbstractChildClass {

		String id;
		String value;
		String otherValue;
		AbstractChildClass someObject;

		public AbstractChildClass(String id, String value) {
			this.id = id;
			this.value = value;
			this.otherValue = "other_" + value;
		}
	}

	static class ConcreteChildClass extends AbstractChildClass {

		public ConcreteChildClass(String id, String value) {
			super(id, value);
		}
	}

	static class DomainEntity {
		List<NestedEntity> collectionOfNestedEntities;
	}

	static class NestedEntity {
		String name;

		public NestedEntity(String name) {
			super();
			this.name = name;
		}

	}

	@WritingConverter
	static class NestedEntityWriteConverter implements Converter<NestedEntity, DBObject> {

		@Override
		public DBObject convert(NestedEntity source) {
			return new BasicDBObject();
		}
	}
}
