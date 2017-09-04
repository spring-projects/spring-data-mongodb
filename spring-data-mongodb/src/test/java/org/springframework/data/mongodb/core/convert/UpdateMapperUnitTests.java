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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.convert.UpdateMapperUnitTests.ClassWithEnum.Allocation;
import org.springframework.data.mongodb.core.convert.UpdateMapperUnitTests.ClassWithEnum.AllocationToStringConverter;
import org.springframework.data.mongodb.core.convert.UpdateMapperUnitTests.ClassWithEnum.StringToAllocationConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.Update.Position;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * Unit tests for {@link UpdateMapper}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Pavel Vodrazka
 */
@RunWith(MockitoJUnitRunner.class)
public class UpdateMapperUnitTests {

	@Mock MongoDbFactory factory;
	MappingMongoConverter converter;
	MongoMappingContext context;
	UpdateMapper mapper;

	private Converter<NestedEntity, DBObject> writingConverterSpy;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {

		this.writingConverterSpy = Mockito.spy(new NestedEntityWriteConverter());
		CustomConversions conversions = new CustomConversions(Arrays.asList(writingConverterSpy));

		this.context = new MongoMappingContext();
		this.context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		this.context.initialize();

		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), context);
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();

		this.mapper = new UpdateMapper(converter);
	}

	@Test // DATAMONGO-721
	public void updateMapperRetainsTypeInformationForCollectionField() {

		Update update = new Update().push("list", new ConcreteChildClass("2", "BAR"));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject list = getAsDBObject(push, "aliased");

		assertThat(list.get("_class"), is((Object) ConcreteChildClass.class.getName()));
	}

	@Test // DATAMONGO-807
	public void updateMapperShouldRetainTypeInformationForNestedEntities() {

		Update update = Update.update("model", new ModelImpl(1));
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		DBObject modelDbObject = (DBObject) set.get("model");
		assertThat(modelDbObject.get("_class"), not(nullValue()));
	}

	@Test // DATAMONGO-807
	public void updateMapperShouldNotPersistTypeInformationForKnownSimpleTypes() {

		Update update = Update.update("model.value", 1);
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		assertThat(set.get("_class"), nullValue());
	}

	@Test // DATAMONGO-807
	public void updateMapperShouldNotPersistTypeInformationForNullValues() {

		Update update = Update.update("model", null);
		UpdateMapper mapper = new UpdateMapper(converter);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		assertThat(set.get("_class"), nullValue());
	}

	@Test // DATAMONGO-407
	public void updateMapperShouldRetainTypeInformationForNestedCollectionElements() {

		Update update = Update.update("list.$", new ConcreteChildClass("42", "bubu"));

		UpdateMapper mapper = new UpdateMapper(converter);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		DBObject modelDbObject = getAsDBObject(set, "aliased.$");
		assertThat(modelDbObject.get("_class"), is((Object) ConcreteChildClass.class.getName()));
	}

	@Test // DATAMONGO-407
	public void updateMapperShouldSupportNestedCollectionElementUpdates() {

		Update update = Update.update("list.$.value", "foo").set("list.$.otherValue", "bar");

		UpdateMapper mapper = new UpdateMapper(converter);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject set = getAsDBObject(mappedObject, "$set");
		assertThat(set.get("aliased.$.value"), is((Object) "foo"));
		assertThat(set.get("aliased.$.otherValue"), is((Object) "bar"));
	}

	@Test // DATAMONGO-407
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test // DATAMONGO-812
	public void updateMapperShouldConvertPushCorrectlyWhenCalledWithEachUsingSimpleTypes() {

		Update update = new Update().push("values").each("spring", "data", "mongodb");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Model.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject values = getAsDBObject(push, "values");
		BasicDBList each = getAsDBList(values, "$each");

		assertThat(push.get("_class"), nullValue());
		assertThat(values.get("_class"), nullValue());

		assertThat(each.toMap(), (Matcher) allOf(hasValue("spring"), hasValue("data"), hasValue("mongodb")));
	}

	@Test // DATAMONGO-812
	public void updateMapperShouldConvertPushWhithoutAddingClassInformationWhenUsedWithEvery() {

		Update update = new Update().push("values").each("spring", "data", "mongodb");

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Model.class));
		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject values = getAsDBObject(push, "values");

		assertThat(push.get("_class"), nullValue());
		assertThat(values.get("_class"), nullValue());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test // DATAMONGO-812
	public void updateMapperShouldConvertPushCorrectlyWhenCalledWithEachUsingCustomTypes() {

		Update update = new Update().push("models").each(new ListModel("spring", "data", "mongodb"));
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject model = getAsDBObject(push, "models");
		BasicDBList each = getAsDBList(model, "$each");
		BasicDBList values = getAsDBList((DBObject) each.get(0), "values");

		assertThat(values.toMap(), (Matcher) allOf(hasValue("spring"), hasValue("data"), hasValue("mongodb")));
	}

	@Test // DATAMONGO-812
	public void updateMapperShouldRetainClassInformationForPushCorrectlyWhenCalledWithEachUsingCustomTypes() {

		Update update = new Update().push("models").each(new ListModel("spring", "data", "mongodb"));
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject model = getAsDBObject(push, "models");
		BasicDBList each = getAsDBList(model, "$each");

		assertThat(((DBObject) each.get(0)).get("_class").toString(), equalTo(ListModel.class.getName()));
	}

	@Test // DATAMONGO-812
	public void testUpdateShouldAllowMultiplePushEachForDifferentFields() {

		Update update = new Update().push("category").each("spring", "data").push("type").each("mongodb");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		assertThat(getAsDBObject(push, "category").containsField("$each"), is(true));
		assertThat(getAsDBObject(push, "type").containsField("$each"), is(true));
	}

	@Test // DATAMONGO-943
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositiveIndexParameter() {

		Update update = new Update().push("key").atPosition(2).each(Arrays.asList("Arya", "Arry", "Weasel"));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "key");

		assertThat(key.containsField("$position"), is(true));
		assertThat((Integer) key.get("$position"), is(2));
		assertThat(getAsDBObject(push, "key").containsField("$each"), is(true));
	}

	@Test // DATAMONGO-943
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositionFirst() {

		Update update = new Update().push("key").atPosition(Position.FIRST).each(Arrays.asList("Arya", "Arry", "Weasel"));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "key");

		assertThat(key.containsField("$position"), is(true));
		assertThat((Integer) key.get("$position"), is(0));
		assertThat(getAsDBObject(push, "key").containsField("$each"), is(true));
	}

	@Test // DATAMONGO-943
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositionLast() {

		Update update = new Update().push("key").atPosition(Position.LAST).each(Arrays.asList("Arya", "Arry", "Weasel"));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "key");

		assertThat(key.containsField("$position"), is(false));
		assertThat(getAsDBObject(push, "key").containsField("$each"), is(true));
	}

	@Test // DATAMONGO-943
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositionNull() {

		Update update = new Update().push("key").atPosition(null).each(Arrays.asList("Arya", "Arry", "Weasel"));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "key");

		assertThat(key.containsField("$position"), is(false));
		assertThat(getAsDBObject(push, "key").containsField("$each"), is(true));
	}

	@Test // DATAMONGO-832
	public void updatePushEachWithSliceShouldRenderCorrectly() {

		Update update = new Update().push("key").slice(5).each(Arrays.asList("Arya", "Arry", "Weasel"));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "key");

		assertThat(key.containsField("$slice"), is(true));
		assertThat((Integer) key.get("$slice"), is(5));
		assertThat(key.containsField("$each"), is(true));
	}

	@Test // DATAMONGO-832
	public void updatePushEachWithSliceShouldRenderWhenUsingMultiplePushCorrectly() {

		Update update = new Update().push("key").slice(5).each(Arrays.asList("Arya", "Arry", "Weasel")).push("key-2")
				.slice(-2).each("The Beggar King", "Viserys III Targaryen");

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "key");

		assertThat(key.containsField("$slice"), is(true));
		assertThat((Integer) key.get("$slice"), is(5));
		assertThat(key.containsField("$each"), is(true));

		DBObject key2 = getAsDBObject(push, "key-2");

		assertThat(key2.containsField("$slice"), is(true));
		assertThat((Integer) key2.get("$slice"), is(-2));
		assertThat(key2.containsField("$each"), is(true));
	}

	@Test // DATAMONGO-1141
	public void updatePushEachWithValueSortShouldRenderCorrectly() {

		Update update = new Update().push("scores").sort(Direction.DESC).each(42, 23, 68);

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "scores");

		assertThat(key.containsField("$sort"), is(true));
		assertThat((Integer) key.get("$sort"), is(-1));
		assertThat(key.containsField("$each"), is(true));
	}

	@Test // DATAMONGO-1141
	public void updatePushEachWithDocumentSortShouldRenderCorrectly() {

		Update update = new Update().push("list")
				.sort(new Sort(new Order(Direction.ASC, "value"), new Order(Direction.ASC, "field")))
				.each(Collections.emptyList());

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithList.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key = getAsDBObject(push, "list");

		assertThat(key.containsField("$sort"), is(true));
		assertThat((DBObject) key.get("$sort"),
				equalTo(new BasicDBObjectBuilder().add("renamed-value", 1).add("field", 1).get()));
		assertThat(key.containsField("$each"), is(true));
	}

	@Test // DATAMONGO-1141
	public void updatePushEachWithSortShouldRenderCorrectlyWhenUsingMultiplePush() {

		Update update = new Update().push("authors").sort(Direction.ASC).each("Harry").push("chapters")
				.sort(new Sort(Direction.ASC, "order")).each(Collections.emptyList());

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject key1 = getAsDBObject(push, "authors");

		assertThat(key1.containsField("$sort"), is(true));
		assertThat((Integer) key1.get("$sort"), is(1));
		assertThat(key1.containsField("$each"), is(true));

		DBObject key2 = getAsDBObject(push, "chapters");

		assertThat(key2.containsField("$sort"), is(true));
		assertThat((DBObject) key2.get("$sort"), equalTo(new BasicDBObjectBuilder().add("order", 1).get()));
		assertThat(key2.containsField("$each"), is(true));
	}

	@Test // DATAMONGO-410
	public void testUpdateMapperShouldConsiderCustomWriteTarget() {

		List<NestedEntity> someValues = Arrays.asList(new NestedEntity("spring"), new NestedEntity("data"),
				new NestedEntity("mongodb"));
		NestedEntity[] array = new NestedEntity[someValues.size()];

		Update update = new Update().pushAll("collectionOfNestedEntities", someValues.toArray(array));
		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DomainEntity.class));

		verify(writingConverterSpy, times(3)).convert(Mockito.any(NestedEntity.class));
	}

	@Test // DATAMONGO-404
	public void createsDbRefForEntityIdOnPulls() {

		Update update = new Update().pull("dbRefAnnotatedList.id", "2");

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject pullClause = getAsDBObject(mappedObject, "$pull");
		assertThat(pullClause.get("dbRefAnnotatedList"), is((Object) new DBRef("entity", "2")));
	}

	@Test // DATAMONGO-404
	public void createsDbRefForEntityOnPulls() {

		Entity entity = new Entity();
		entity.id = "5";

		Update update = new Update().pull("dbRefAnnotatedList", entity);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject pullClause = getAsDBObject(mappedObject, "$pull");
		assertThat(pullClause.get("dbRefAnnotatedList"), is((Object) new DBRef("entity", entity.id)));
	}

	@Test(expected = MappingException.class) // DATAMONGO-404
	public void rejectsInvalidFieldReferenceForDbRef() {

		Update update = new Update().pull("dbRefAnnotatedList.name", "NAME");
		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DocumentWithDBRefCollection.class));
	}

	@Test // DATAMONGO-404
	public void rendersNestedDbRefCorrectly() {

		Update update = new Update().pull("nested.dbRefAnnotatedList.id", "2");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(Wrapper.class));

		DBObject pullClause = getAsDBObject(mappedObject, "$pull");
		assertThat(pullClause.containsField("mapped.dbRefAnnotatedList"), is(true));
	}

	@Test // DATAMONGO-468
	public void rendersUpdateOfDbRefPropertyWithDomainObjectCorrectly() {

		Entity entity = new Entity();
		entity.id = "5";

		Update update = new Update().set("dbRefProperty", entity);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject setClause = getAsDBObject(mappedObject, "$set");
		assertThat(setClause.get("dbRefProperty"), is((Object) new DBRef("entity", entity.id)));
	}

	@Test // DATAMONGO-862
	public void rendersUpdateAndPreservesKeyForPathsNotPointingToProperty() {

		Update update = new Update().set("listOfInterface.$.value", "expected-value");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject setClause = getAsDBObject(mappedObject, "$set");
		assertThat(setClause.containsField("listOfInterface.$.value"), is(true));
	}

	@Test // DATAMONGO-863
	public void doesNotConvertRawDbObjects() {

		Update update = new Update();
		update.pull("options",
				new BasicDBObject("_id", new BasicDBObject("$in", converter.convertToMongoType(Arrays.asList(1L, 2L)))));

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject setClause = getAsDBObject(mappedObject, "$pull");
		DBObject options = getAsDBObject(setClause, "options");
		DBObject idClause = getAsDBObject(options, "_id");
		BasicDBList inClause = getAsDBList(idClause, "$in");

		assertThat(inClause, IsIterableContainingInOrder.<Object> contains(1L, 2L));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test // DATAMONG0-471
	public void testUpdateShouldApply$addToSetCorrectlyWhenUsedWith$each() {

		Update update = new Update().addToSet("values").each("spring", "data", "mongodb");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModel.class));

		DBObject addToSet = getAsDBObject(mappedObject, "$addToSet");
		DBObject values = getAsDBObject(addToSet, "values");
		BasicDBList each = getAsDBList(values, "$each");

		assertThat(each.toMap(), (Matcher) allOf(hasValue("spring"), hasValue("data"), hasValue("mongodb")));
	}

	@Test // DATAMONG0-471
	public void testUpdateShouldRetainClassTypeInformationWhenUsing$addToSetWith$eachForCustomTypes() {

		Update update = new Update().addToSet("models").each(new ModelImpl(2014), new ModelImpl(1), new ModelImpl(28));
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject addToSet = getAsDBObject(mappedObject, "$addToSet");

		DBObject values = getAsDBObject(addToSet, "models");
		BasicDBList each = getAsDBList(values, "$each");

		for (Object updateValue : each) {
			assertThat(((DBObject) updateValue).get("_class").toString(),
					equalTo("org.springframework.data.mongodb.core.convert.UpdateMapperUnitTests$ModelImpl"));
		}
	}

	@Test // DATAMONGO-897
	public void updateOnDbrefPropertyOfInterfaceTypeWithoutExplicitGetterForIdShouldBeMappedCorrectly() {

		Update update = new Update().set("referencedDocument", new InterfaceDocumentDefinitionImpl("1", "Foo"));
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithReferenceToInterfaceImpl.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedObject, "$set");
		Object model = $set.get("referencedDocument");

		DBRef expectedDBRef = new DBRef("interfaceDocumentDefinitionImpl", "1");
		assertThat(model, allOf(instanceOf(DBRef.class), IsEqual.<Object> equalTo(expectedDBRef)));
	}

	@Test // DATAMONGO-847
	public void updateMapperConvertsNestedQueryCorrectly() {

		Update update = new Update().pull("list", Query.query(Criteria.where("value").in("foo", "bar")));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject $pull = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$pull");
		DBObject list = DBObjectTestUtils.getAsDBObject($pull, "aliased");
		DBObject value = DBObjectTestUtils.getAsDBObject(list, "value");
		BasicDBList $in = DBObjectTestUtils.getAsDBList(value, "$in");

		assertThat($in, IsIterableContainingInOrder.<Object> contains("foo", "bar"));
	}

	@Test // DATAMONGO-847
	public void updateMapperConvertsPullWithNestedQuerfyOnDBRefCorrectly() {

		Update update = new Update().pull("dbRefAnnotatedList", Query.query(Criteria.where("id").is("1")));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject $pull = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$pull");
		DBObject list = DBObjectTestUtils.getAsDBObject($pull, "dbRefAnnotatedList");

		assertThat(list, equalTo(new BasicDBObjectBuilder().add("_id", "1").get()));
	}

	@Test // DATAMONGO-1077
	public void shouldNotRemovePositionalParameter() {

		Update update = new Update();
		update.unset("dbRefAnnotatedList.$");

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject $unset = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$unset");

		assertThat($unset, equalTo(new BasicDBObjectBuilder().add("dbRefAnnotatedList.$", 1).get()));
	}

	@Test // DATAMONGO-1210
	public void mappingEachOperatorShouldNotAddTypeInfoForNonInterfaceNonAbstractTypes() {

		Update update = new Update().addToSet("nestedDocs").each(new NestedDocument("nested-1"),
				new NestedDocument("nested-2"));

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNestedCollection.class));

		assertThat(mappedUpdate, isBsonObject().notContaining("$addToSet.nestedDocs.$each.[0]._class"));
		assertThat(mappedUpdate, isBsonObject().notContaining("$addToSet.nestedDocs.$each.[1]._class"));
	}

	@Test // DATAMONGO-1210
	public void mappingEachOperatorShouldAddTypeHintForInterfaceTypes() {

		Update update = new Update().addToSet("models").each(new ModelImpl(1), new ModelImpl(2));

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModelWrapper.class));

		assertThat(mappedUpdate, isBsonObject().containing("$addToSet.models.$each.[0]._class", ModelImpl.class.getName()));
		assertThat(mappedUpdate, isBsonObject().containing("$addToSet.models.$each.[1]._class", ModelImpl.class.getName()));
	}

	@Test // DATAMONGO-1210
	public void mappingEachOperatorShouldAddTypeHintForAbstractTypes() {

		Update update = new Update().addToSet("list").each(new ConcreteChildClass("foo", "one"),
				new ConcreteChildClass("bar", "two"));

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		assertThat(mappedUpdate,
				isBsonObject().containing("$addToSet.aliased.$each.[0]._class", ConcreteChildClass.class.getName()));
		assertThat(mappedUpdate,
				isBsonObject().containing("$addToSet.aliased.$each.[1]._class", ConcreteChildClass.class.getName()));
	}

	@Test // DATAMONGO-1210
	public void mappingShouldOnlyRemoveTypeHintFromTopLevelTypeInCaseOfNestedDocument() {

		WrapperAroundInterfaceType wait = new WrapperAroundInterfaceType();
		wait.interfaceType = new ModelImpl(1);

		Update update = new Update().addToSet("listHoldingConcretyTypeWithInterfaceTypeAttribute").each(wait);
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DomainTypeWithListOfConcreteTypesHavingSingleInterfaceTypeAttribute.class));

		assertThat(mappedUpdate,
				isBsonObject().notContaining("$addToSet.listHoldingConcretyTypeWithInterfaceTypeAttribute.$each.[0]._class"));
		assertThat(mappedUpdate,
				isBsonObject().containing(
						"$addToSet.listHoldingConcretyTypeWithInterfaceTypeAttribute.$each.[0].interfaceType._class",
						ModelImpl.class.getName()));
	}

	@Test // DATAMONGO-1210
	public void mappingShouldRetainTypeInformationOfNestedListWhenUpdatingConcreteyParentType() {

		ListModelWrapper lmw = new ListModelWrapper();
		lmw.models = Collections.<Model> singletonList(new ModelImpl(1));

		Update update = new Update().set("concreteTypeWithListAttributeOfInterfaceType", lmw);
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DomainTypeWrappingConcreteyTypeHavingListOfInterfaceTypeAttributes.class));

		assertThat(mappedUpdate, isBsonObject().notContaining("$set.concreteTypeWithListAttributeOfInterfaceType._class"));
		assertThat(mappedUpdate, isBsonObject()
				.containing("$set.concreteTypeWithListAttributeOfInterfaceType.models.[0]._class", ModelImpl.class.getName()));
	}

	@Test // DATAMONGO-1236
	public void mappingShouldRetainTypeInformationForObjectValues() {

		Update update = new Update().set("value", new NestedDocument("kaladin"));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObject.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.value.name", "kaladin"));
		assertThat(mappedUpdate, isBsonObject().containing("$set.value._class", NestedDocument.class.getName()));
	}

	@Test // DATAMONGO-1236
	public void mappingShouldNotRetainTypeInformationForConcreteValues() {

		Update update = new Update().set("concreteValue", new NestedDocument("shallan"));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObject.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.concreteValue.name", "shallan"));
		assertThat(mappedUpdate, isBsonObject().notContaining("$set.concreteValue._class"));
	}

	@Test // DATAMONGO-1236
	public void mappingShouldRetainTypeInformationForObjectValuesWithAlias() {

		Update update = new Update().set("value", new NestedDocument("adolin"));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithAliasedObject.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.renamed-value.name", "adolin"));
		assertThat(mappedUpdate, isBsonObject().containing("$set.renamed-value._class", NestedDocument.class.getName()));
	}

	@Test // DATAMONGO-1236
	public void mappingShouldRetrainTypeInformationWhenValueTypeOfMapDoesNotMatchItsDeclaration() {

		Map<Object, Object> map = Collections.<Object, Object> singletonMap("szeth", new NestedDocument("son-son-vallano"));

		Update update = new Update().set("map", map);
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObjectMap.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.map.szeth.name", "son-son-vallano"));
		assertThat(mappedUpdate, isBsonObject().containing("$set.map.szeth._class", NestedDocument.class.getName()));
	}

	@Test // DATAMONGO-1236
	public void mappingShouldNotContainTypeInformationWhenValueTypeOfMapMatchesDeclaration() {

		Map<Object, NestedDocument> map = Collections.<Object, NestedDocument> singletonMap("jasnah",
				new NestedDocument("kholin"));

		Update update = new Update().set("concreteMap", map);
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObjectMap.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.concreteMap.jasnah.name", "kholin"));
		assertThat(mappedUpdate, isBsonObject().notContaining("$set.concreteMap.jasnah._class"));
	}

	@Test // DATAMONGO-1250
	@SuppressWarnings("unchecked")
	public void mapsUpdateWithBothReadingAndWritingConverterRegistered() {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(AllocationToStringConverter.INSTANCE, StringToAllocationConverter.INSTANCE));

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(mock(DbRefResolver.class), mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		UpdateMapper mapper = new UpdateMapper(converter);

		Update update = new Update().set("allocation", Allocation.AVAILABLE);
		DBObject result = mapper.getMappedObject(update.getUpdateObject(),
				mappingContext.getPersistentEntity(ClassWithEnum.class));

		assertThat(result, isBsonObject().containing("$set.allocation", Allocation.AVAILABLE.code));
	}

	@Test // DATAMONGO-1251
	public void mapsNullValueCorrectlyForSimpleTypes() {

		Update update = new Update().set("value", null);

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ConcreteChildClass.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$set");
		assertThat($set.containsField("value"), is(true));
		assertThat($set.get("value"), nullValue());
	}

	@Test // DATAMONGO-1251
	public void mapsNullValueCorrectlyForJava8Date() {

		Update update = new Update().set("date", null);

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ClassWithJava8Date.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$set");
		assertThat($set.containsField("date"), is(true));
		assertThat($set.get("value"), nullValue());
	}

	@Test // DATAMONGO-1251
	public void mapsNullValueCorrectlyForCollectionTypes() {

		Update update = new Update().set("values", null);

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModel.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$set");
		assertThat($set.containsField("values"), is(true));
		assertThat($set.get("value"), nullValue());
	}

	@Test // DATAMONGO-1251
	public void mapsNullValueCorrectlyForPropertyOfNestedDocument() {

		Update update = new Update().set("concreteValue.name", null);

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObject.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$set");
		assertThat($set.containsField("concreteValue.name"), is(true));
		assertThat($set.get("concreteValue.name"), nullValue());
	}

	@Test // DATAMONGO-1288
	public void mapsAtomicIntegerToIntegerCorrectly() {

		Update update = new Update().set("intValue", new AtomicInteger(10));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$set");
		assertThat($set.get("intValue"), Is.<Object> is(10));
	}

	@Test // DATAMONGO-1288
	public void mapsAtomicIntegerToPrimitiveIntegerCorrectly() {

		Update update = new Update().set("primIntValue", new AtomicInteger(10));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$set");
		assertThat($set.get("primIntValue"), Is.<Object> is(10));
	}

	@Test // DATAMONGO-1404
	public void mapsMinCorrectly() {

		Update update = new Update().min("minfield", 10);
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedUpdate, isBsonObject().containing("$min", new BasicDBObject("minfield", 10)));
	}

	@Test // DATAMONGO-1404
	public void mapsMaxCorrectly() {

		Update update = new Update().max("maxfield", 999);
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedUpdate, isBsonObject().containing("$max", new BasicDBObject("maxfield", 999)));
	}

	@Test // DATAMONGO-1423
	@SuppressWarnings("unchecked")
	public void mappingShouldConsiderCustomConvertersForEnumMapKeys() {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(AllocationToStringConverter.INSTANCE, StringToAllocationConverter.INSTANCE));

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(mock(DbRefResolver.class), mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		UpdateMapper mapper = new UpdateMapper(converter);

		Update update = new Update().set("enumAsMapKey", Collections.singletonMap(Allocation.AVAILABLE, 100));
		DBObject result = mapper.getMappedObject(update.getUpdateObject(),
				mappingContext.getPersistentEntity(ClassWithEnum.class));

		assertThat(result, isBsonObject().containing("$set.enumAsMapKey.V", 100));
	}

	@Test // DATAMONGO-1486
	public void mappingShouldConvertMapKeysToString() {

		Update update = new Update().set("map", Collections.singletonMap(25, "#StarTrek50"));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObjectMap.class));

		DBObject mapToSet = getAsDBObject(getAsDBObject(mappedUpdate, "$set"), "map");

		for (Object key : mapToSet.keySet()) {
			assertThat(key, is(instanceOf(String.class)));
		}
	}

	@Test // DATAMONGO-1772
	public void mappingShouldAddTypeKeyInListOfInterfaceTypeContainedInConcreteObjectCorrectly() {

		ConcreteInner inner = new ConcreteInner();
		inner.interfaceTypeList = Collections.<SomeInterfaceType> singletonList(new SomeInterfaceImpl());
		List<ConcreteInner> list = Collections.singletonList(inner);

		DBObject mappedUpdate = mapper.getMappedObject(new Update().set("concreteInnerList", list).getUpdateObject(),
				context.getPersistentEntity(Outer.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.concreteInnerList.[0].interfaceTypeList.[0]._class")
				.notContaining("$set.concreteInnerList.[0]._class"));
	}

	@Test // DATAMONGO-1772
	public void mappingShouldAddTypeKeyInListOfAbstractTypeContainedInConcreteObjectCorrectly() {

		ConcreteInner inner = new ConcreteInner();
		inner.abstractTypeList = Collections.<SomeAbstractType> singletonList(new SomeInterfaceImpl());
		List<ConcreteInner> list = Collections.singletonList(inner);

		DBObject mappedUpdate = mapper.getMappedObject(new Update().set("concreteInnerList", list).getUpdateObject(),
				context.getPersistentEntity(Outer.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.concreteInnerList.[0].abstractTypeList.[0]._class")
				.notContaining("$set.concreteInnerList.[0]._class"));
	}

	static class DomainTypeWrappingConcreteyTypeHavingListOfInterfaceTypeAttributes {
		ListModelWrapper concreteTypeWithListAttributeOfInterfaceType;
	}

	static class DomainTypeWithListOfConcreteTypesHavingSingleInterfaceTypeAttribute {
		List<WrapperAroundInterfaceType> listHoldingConcretyTypeWithInterfaceTypeAttribute;
	}

	static class WrapperAroundInterfaceType {
		Model interfaceType;
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "DocumentWithReferenceToInterface")
	static interface DocumentWithReferenceToInterface {

		String getId();

		InterfaceDocumentDefinitionWithoutId getReferencedDocument();

	}

	static interface InterfaceDocumentDefinitionWithoutId {

		String getValue();
	}

	static class InterfaceDocumentDefinitionImpl implements InterfaceDocumentDefinitionWithoutId {

		@Id String id;
		String value;

		public InterfaceDocumentDefinitionImpl(String id, String value) {

			this.id = id;
			this.value = value;
		}

		@Override
		public String getValue() {
			return this.value;
		}

	}

	static class DocumentWithReferenceToInterfaceImpl implements DocumentWithReferenceToInterface {

		private @Id String id;

		@org.springframework.data.mongodb.core.mapping.DBRef //
		private InterfaceDocumentDefinitionWithoutId referencedDocument;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setModel(InterfaceDocumentDefinitionWithoutId referencedDocument) {
			this.referencedDocument = referencedDocument;
		}

		@Override
		public InterfaceDocumentDefinitionWithoutId getReferencedDocument() {
			return this.referencedDocument;
		}

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

		public ModelWrapper() {}

		public ModelWrapper(Model model) {
			this.model = model;
		}
	}

	static class ListModelWrapper {

		List<Model> models;
	}

	static class ListModel {

		List<String> values;

		public ListModel(String... values) {
			this.values = Arrays.asList(values);
		}
	}

	static class ParentClass {

		String id;

		@Field("aliased") //
		List<? extends AbstractChildClass> list;

		@Field //
		List<Model> listOfInterface;

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

		public List<NestedEntity> getCollectionOfNestedEntities() {
			return collectionOfNestedEntities;
		}
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

	static class DocumentWithDBRefCollection {

		@Id public String id;

		@org.springframework.data.mongodb.core.mapping.DBRef //
		public List<Entity> dbRefAnnotatedList;

		@org.springframework.data.mongodb.core.mapping.DBRef //
		public Entity dbRefProperty;
	}

	static class Entity {

		@Id public String id;
		String name;
	}

	static class Wrapper {

		@Field("mapped") DocumentWithDBRefCollection nested;
	}

	static class DocumentWithNestedCollection {
		List<NestedDocument> nestedDocs;
	}

	static class NestedDocument {

		String name;

		public NestedDocument(String name) {
			super();
			this.name = name;
		}
	}

	static class EntityWithObject {

		Object value;
		NestedDocument concreteValue;
	}

	static class EntityWithList {
		List<EntityWithAliasedObject> list;
	}

	static class EntityWithAliasedObject {

		@Field("renamed-value") Object value;
		Object field;
	}

	static class EntityWithObjectMap {

		Map<Object, Object> map;
		Map<Object, NestedDocument> concreteMap;
	}

	static class ClassWithEnum {

		Allocation allocation;
		Map<Allocation, String> enumAsMapKey;

		static enum Allocation {

			AVAILABLE("V"), ALLOCATED("A");

			String code;

			private Allocation(String code) {
				this.code = code;
			}

			public static Allocation of(String code) {

				for (Allocation value : values()) {
					if (value.code.equals(code)) {
						return value;
					}
				}

				throw new IllegalArgumentException();
			}
		}

		static enum AllocationToStringConverter implements Converter<Allocation, String> {

			INSTANCE;

			@Override
			public String convert(Allocation source) {
				return source.code;
			}
		}

		static enum StringToAllocationConverter implements Converter<String, Allocation> {

			INSTANCE;

			@Override
			public Allocation convert(String source) {
				return Allocation.of(source);
			}
		}
	}

	static class ClassWithJava8Date {

		LocalDate date;
	}

	static class SimpleValueHolder {

		Integer intValue;
		int primIntValue;
	}

	static class Outer {
		List<ConcreteInner> concreteInnerList;
	}

	static class ConcreteInner {
		List<SomeInterfaceType> interfaceTypeList;
		List<SomeAbstractType> abstractTypeList;
	}

	interface SomeInterfaceType {

	}

	static abstract class SomeAbstractType {

	}

	static class SomeInterfaceImpl extends SomeAbstractType implements SomeInterfaceType {

	}

}
