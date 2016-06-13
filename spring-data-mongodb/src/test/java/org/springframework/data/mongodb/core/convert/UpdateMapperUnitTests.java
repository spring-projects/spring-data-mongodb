/*
 * Copyright 2013-2016 the original author or authors.
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
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
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
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.Update.Position;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link UpdateMapper}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class UpdateMapperUnitTests {

	@Mock MongoDbFactory factory;
	MappingMongoConverter converter;
	MongoMappingContext context;
	UpdateMapper mapper;

	private Converter<NestedEntity, Document> writingConverterSpy;

	@Before
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

	/**
	 * @see DATAMONGO-721
	 */
	@Test
	public void updateMapperRetainsTypeInformationForCollectionField() {

		Update update = new Update().push("list", new ConcreteChildClass("2", "BAR"));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document list = getAsDocument(push, "aliased");

		assertThat(list.get("_class"), is(ConcreteChildClass.class.getName()));
	}

	/**
	 * @see DATAMONGO-807
	 */
	@Test
	public void updateMapperShouldRetainTypeInformationForNestedEntities() {

		Update update = Update.update("model", new ModelImpl(1));
		UpdateMapper mapper = new UpdateMapper(converter);

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		Document set = getAsDocument(mappedObject, "$set");
		Document modelDbObject = (Document) set.get("model");
		assertThat(modelDbObject.get("_class"), not(nullValue()));
	}

	/**
	 * @see DATAMONGO-807
	 */
	@Test
	public void updateMapperShouldNotPersistTypeInformationForKnownSimpleTypes() {

		Update update = Update.update("model.value", 1);
		UpdateMapper mapper = new UpdateMapper(converter);

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		Document set = getAsDocument(mappedObject, "$set");
		assertThat(set.get("_class"), nullValue());
	}

	/**
	 * @see DATAMONGO-807
	 */
	@Test
	public void updateMapperShouldNotPersistTypeInformationForNullValues() {

		Update update = Update.update("model", null);
		UpdateMapper mapper = new UpdateMapper(converter);

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		Document set = getAsDocument(mappedObject, "$set");
		assertThat(set.get("_class"), nullValue());
	}

	/**
	 * @see DATAMONGO-407
	 */
	@Test
	public void updateMapperShouldRetainTypeInformationForNestedCollectionElements() {

		Update update = Update.update("list.$", new ConcreteChildClass("42", "bubu"));

		UpdateMapper mapper = new UpdateMapper(converter);
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document set = getAsDocument(mappedObject, "$set");
		Document modelDbObject = getAsDocument(set, "aliased.$");
		assertThat(modelDbObject.get("_class"), is(ConcreteChildClass.class.getName()));
	}

	/**
	 * @see DATAMONGO-407
	 */
	@Test
	public void updateMapperShouldSupportNestedCollectionElementUpdates() {

		Update update = Update.update("list.$.value", "foo").set("list.$.otherValue", "bar");

		UpdateMapper mapper = new UpdateMapper(converter);
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document set = getAsDocument(mappedObject, "$set");
		assertThat(set.get("aliased.$.value"), is("foo"));
		assertThat(set.get("aliased.$.otherValue"), is("bar"));
	}

	/**
	 * @see DATAMONGO-407
	 */
	@Test
	public void updateMapperShouldWriteTypeInformationForComplexNestedCollectionElementUpdates() {

		Update update = Update.update("list.$.value", "foo").set("list.$.someObject", new ConcreteChildClass("42", "bubu"));

		UpdateMapper mapper = new UpdateMapper(converter);
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document dbo = getAsDocument(mappedObject, "$set");
		assertThat(dbo.get("aliased.$.value"), is("foo"));

		Document someObject = getAsDocument(dbo, "aliased.$.someObject");
		assertThat(someObject, is(notNullValue()));
		assertThat(someObject.get("_class"), is(ConcreteChildClass.class.getName()));
		assertThat(someObject.get("value"), is("bubu"));
	}

	/**
	 * @see DATAMONGO-812
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void updateMapperShouldConvertPushCorrectlyWhenCalledWithEachUsingSimpleTypes() {

		Update update = new Update().push("values").each("spring", "data", "mongodb");
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Model.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document values = getAsDocument(push, "values");
		List<Object> each = getAsDBList(values, "$each");

		assertThat(push.get("_class"), nullValue());
		assertThat(values.get("_class"), nullValue());

		assertThat(each, IsIterableContainingInOrder.contains("spring", "data", "mongodb"));
	}

	/**
	 * @see DATAMONGO-812
	 */
	@Test
	public void updateMapperShouldConvertPushWhithoutAddingClassInformationWhenUsedWithEvery() {

		Update update = new Update().push("values").each("spring", "data", "mongodb");

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Model.class));
		Document push = getAsDocument(mappedObject, "$push");
		Document values = getAsDocument(push, "values");

		assertThat(push.get("_class"), nullValue());
		assertThat(values.get("_class"), nullValue());
	}

	/**
	 * @see DATAMONGO-812
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void updateMapperShouldConvertPushCorrectlyWhenCalledWithEachUsingCustomTypes() {

		Update update = new Update().push("models").each(new ListModel("spring", "data", "mongodb"));
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document model = getAsDocument(push, "models");
		List<Object> each = getAsDBList(model, "$each");
		List<Object> values = getAsDBList((Document) each.get(0), "values");

		assertThat(values, IsIterableContainingInOrder.contains("spring", "data", "mongodb"));
	}

	/**
	 * @see DATAMONGO-812
	 */
	@Test
	public void updateMapperShouldRetainClassInformationForPushCorrectlyWhenCalledWithEachUsingCustomTypes() {

		Update update = new Update().push("models").each(new ListModel("spring", "data", "mongodb"));
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document model = getAsDocument(push, "models");
		List<Object> each = getAsDBList(model, "$each");

		assertThat(((Document) each.get(0)).get("_class").toString(), equalTo(ListModel.class.getName()));
	}

	/**
	 * @see DATAMONGO-812
	 */
	@Test
	public void testUpdateShouldAllowMultiplePushEachForDifferentFields() {

		Update update = new Update().push("category").each("spring", "data").push("type").each("mongodb");
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		assertThat(getAsDocument(push, "category").containsKey("$each"), is(true));
		assertThat(getAsDocument(push, "type").containsKey("$each"), is(true));
	}

	/**
	 * @see DATAMONGO-943
	 */
	@Test
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositiveIndexParameter() {

		Update update = new Update().push("key").atPosition(2).each(Arrays.asList("Arya", "Arry", "Weasel"));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document key = getAsDocument(push, "key");

		assertThat(key.containsKey("$position"), is(true));
		assertThat(key.get("$position"), is(2));
		assertThat(getAsDocument(push, "key").containsKey("$each"), is(true));
	}

	/**
	 * @see DATAMONGO-943
	 */
	@Test
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositionFirst() {

		Update update = new Update().push("key").atPosition(Position.FIRST).each(Arrays.asList("Arya", "Arry", "Weasel"));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document key = getAsDocument(push, "key");

		assertThat(key.containsKey("$position"), is(true));
		assertThat(key.get("$position"), is(0));
		assertThat(getAsDocument(push, "key").containsKey("$each"), is(true));
	}

	/**
	 * @see DATAMONGO-943
	 */
	@Test
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositionLast() {

		Update update = new Update().push("key").atPosition(Position.LAST).each(Arrays.asList("Arya", "Arry", "Weasel"));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document key = getAsDocument(push, "key");

		assertThat(key.containsKey("$position"), is(false));
		assertThat(getAsDocument(push, "key").containsKey("$each"), is(true));
	}

	/**
	 * @see DATAMONGO-943
	 */
	@Test
	public void updatePushEachAtPositionWorksCorrectlyWhenGivenPositionNull() {

		Update update = new Update().push("key").atPosition(null).each(Arrays.asList("Arya", "Arry", "Weasel"));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document key = getAsDocument(push, "key");

		assertThat(key.containsKey("$position"), is(false));
		assertThat(getAsDocument(push, "key").containsKey("$each"), is(true));
	}

	/**
	 * @see DATAMONGO-832
	 */
	@Test
	public void updatePushEachWithSliceShouldRenderCorrectly() {

		Update update = new Update().push("key").slice(5).each(Arrays.asList("Arya", "Arry", "Weasel"));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document key = getAsDocument(push, "key");

		assertThat(key.containsKey("$slice"), is(true));
		assertThat(key.get("$slice"), is(5));
		assertThat(key.containsKey("$each"), is(true));
	}

	/**
	 * @see DATAMONGO-832
	 */
	@Test
	public void updatePushEachWithSliceShouldRenderWhenUsingMultiplePushCorrectly() {

		Update update = new Update().push("key").slice(5).each(Arrays.asList("Arya", "Arry", "Weasel")).push("key-2")
				.slice(-2).each("The Beggar King", "Viserys III Targaryen");

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		Document push = getAsDocument(mappedObject, "$push");
		Document key = getAsDocument(push, "key");

		assertThat(key.containsKey("$slice"), is(true));
		assertThat(key.get("$slice"), is(5));
		assertThat(key.containsKey("$each"), is(true));

		Document key2 = getAsDocument(push, "key-2");

		assertThat(key2.containsKey("$slice"), is(true));
		assertThat(key2.get("$slice"), is(-2));
		assertThat(key2.containsKey("$each"), is(true));
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

	/**
	 * @see DATAMONGO-404
	 */
	@Test
	public void createsDbRefForEntityIdOnPulls() {

		Update update = new Update().pull("dbRefAnnotatedList.id", "2");

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		Document pullClause = getAsDocument(mappedObject, "$pull");
		assertThat(pullClause.get("dbRefAnnotatedList"), is(new DBRef("entity", "2")));
	}

	/**
	 * @see DATAMONGO-404
	 */
	@Test
	public void createsDbRefForEntityOnPulls() {

		Entity entity = new Entity();
		entity.id = "5";

		Update update = new Update().pull("dbRefAnnotatedList", entity);
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		Document pullClause = getAsDocument(mappedObject, "$pull");
		assertThat(pullClause.get("dbRefAnnotatedList"), is(new DBRef("entity", entity.id)));
	}

	/**
	 * @see DATAMONGO-404
	 */
	@Test(expected = MappingException.class)
	public void rejectsInvalidFieldReferenceForDbRef() {

		Update update = new Update().pull("dbRefAnnotatedList.name", "NAME");
		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DocumentWithDBRefCollection.class));
	}

	/**
	 * @see DATAMONGO-404
	 */
	@Test
	public void rendersNestedDbRefCorrectly() {

		Update update = new Update().pull("nested.dbRefAnnotatedList.id", "2");
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(Wrapper.class));

		Document pullClause = getAsDocument(mappedObject, "$pull");
		assertThat(pullClause.containsKey("mapped.dbRefAnnotatedList"), is(true));
	}

	/**
	 * @see DATAMONGO-468
	 */
	@Test
	public void rendersUpdateOfDbRefPropertyWithDomainObjectCorrectly() {

		Entity entity = new Entity();
		entity.id = "5";

		Update update = new Update().set("dbRefProperty", entity);
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		Document setClause = getAsDocument(mappedObject, "$set");
		assertThat(setClause.get("dbRefProperty"), is(new DBRef("entity", entity.id)));
	}

	/**
	 * @see DATAMONGO-862
	 */
	@Test
	public void rendersUpdateAndPreservesKeyForPathsNotPointingToProperty() {

		Update update = new Update().set("listOfInterface.$.value", "expected-value");
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document setClause = getAsDocument(mappedObject, "$set");
		assertThat(setClause.containsKey("listOfInterface.$.value"), is(true));
	}

	/**
	 * @see DATAMONGO-863
	 */
	@Test
	public void doesNotConvertRawDbObjects() {

		Update update = new Update();
		update.pull("options",
				new Document("_id", new Document("$in", converter.convertToMongoType(Arrays.asList(1L, 2L)))));

		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document setClause = getAsDocument(mappedObject, "$pull");
		Document options = getAsDocument(setClause, "options");
		Document idClause = getAsDocument(options, "_id");
		List<Object> inClause = getAsDBList(idClause, "$in");

		assertThat(inClause, IsIterableContainingInOrder.contains(1L, 2L));
	}

	/**
	 * @see DATAMONG0-471
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUpdateShouldApply$addToSetCorrectlyWhenUsedWith$each() {

		Update update = new Update().addToSet("values").each("spring", "data", "mongodb");
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModel.class));

		Document addToSet = getAsDocument(mappedObject, "$addToSet");
		Document values = getAsDocument(addToSet, "values");
		List<Object> each = getAsDBList(values, "$each");

		assertThat(each, IsIterableContainingInOrder.contains("spring", "data", "mongodb"));
	}

	/**
	 * @see DATAMONG0-471
	 */
	@Test
	public void testUpdateShouldRetainClassTypeInformationWhenUsing$addToSetWith$eachForCustomTypes() {

		Update update = new Update().addToSet("models").each(new ModelImpl(2014), new ModelImpl(1), new ModelImpl(28));
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		Document addToSet = getAsDocument(mappedObject, "$addToSet");

		Document values = getAsDocument(addToSet, "models");
		List each = getAsDBList(values, "$each");

		for (Object updateValue : each) {
			assertThat(((Document) updateValue).get("_class").toString(),
					equalTo("org.springframework.data.mongodb.core.convert.UpdateMapperUnitTests$ModelImpl"));
		}
	}

	/**
	 * @see DATAMONGO-897
	 */
	@Test
	public void updateOnDbrefPropertyOfInterfaceTypeWithoutExplicitGetterForIdShouldBeMappedCorrectly() {

		Update update = new Update().set("referencedDocument", new InterfaceDocumentDefinitionImpl("1", "Foo"));
		Document mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithReferenceToInterfaceImpl.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedObject, "$set");
		Object model = $set.get("referencedDocument");

		DBRef expectedDBRef = new DBRef("interfaceDocumentDefinitionImpl", "1");
		assertThat(model, allOf(instanceOf(DBRef.class), IsEqual.equalTo(expectedDBRef)));
	}

	/**
	 * @see DATAMONGO-847
	 */
	@Test
	public void updateMapperConvertsNestedQueryCorrectly() {

		Update update = new Update().pull("list", Query.query(Criteria.where("value").in("foo", "bar")));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		Document $pull = DBObjectTestUtils.getAsDocument(mappedUpdate, "$pull");
		Document list = DBObjectTestUtils.getAsDocument($pull, "aliased");
		Document value = DBObjectTestUtils.getAsDocument(list, "value");
		List<Object> $in = DBObjectTestUtils.getAsDBList(value, "$in");

		assertThat($in, IsIterableContainingInOrder.contains("foo", "bar"));
	}

	/**
	 * @see DATAMONGO-847
	 */
	@Test
	public void updateMapperConvertsPullWithNestedQuerfyOnDBRefCorrectly() {

		Update update = new Update().pull("dbRefAnnotatedList", Query.query(Criteria.where("id").is("1")));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		Document $pull = DBObjectTestUtils.getAsDocument(mappedUpdate, "$pull");
		Document list = DBObjectTestUtils.getAsDocument($pull, "dbRefAnnotatedList");

		assertThat(list, equalTo(new org.bson.Document().append("_id", "1")));
	}

	/**
	 * @see DATAMONGO-1077
	 */
	@Test
	public void shouldNotRemovePositionalParameter() {

		Update update = new Update();
		update.unset("dbRefAnnotatedList.$");

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		Document $unset = DBObjectTestUtils.getAsDocument(mappedUpdate, "$unset");

		assertThat($unset, equalTo(new org.bson.Document().append("dbRefAnnotatedList.$", 1)));
	}

	/**
	 * @see DATAMONGO-1210
	 */
	@Test
	public void mappingEachOperatorShouldNotAddTypeInfoForNonInterfaceNonAbstractTypes() {

		Update update = new Update().addToSet("nestedDocs").each(new NestedDocument("nested-1"),
				new NestedDocument("nested-2"));

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNestedCollection.class));

		assertThat(mappedUpdate, isBsonObject().notContaining("$addToSet.nestedDocs.$each.[0]._class"));
		assertThat(mappedUpdate, isBsonObject().notContaining("$addToSet.nestedDocs.$each.[1]._class"));
	}

	/**
	 * @see DATAMONGO-1210
	 */
	@Test
	public void mappingEachOperatorShouldAddTypeHintForInterfaceTypes() {

		Update update = new Update().addToSet("models").each(new ModelImpl(1), new ModelImpl(2));

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModelWrapper.class));

		assertThat(mappedUpdate, isBsonObject().containing("$addToSet.models.$each.[0]._class", ModelImpl.class.getName()));
		assertThat(mappedUpdate, isBsonObject().containing("$addToSet.models.$each.[1]._class", ModelImpl.class.getName()));
	}

	/**
	 * @see DATAMONGO-1210
	 */
	@Test
	public void mappingEachOperatorShouldAddTypeHintForAbstractTypes() {

		Update update = new Update().addToSet("list").each(new ConcreteChildClass("foo", "one"),
				new ConcreteChildClass("bar", "two"));

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		assertThat(mappedUpdate,
				isBsonObject().containing("$addToSet.aliased.$each.[0]._class", ConcreteChildClass.class.getName()));
		assertThat(mappedUpdate,
				isBsonObject().containing("$addToSet.aliased.$each.[1]._class", ConcreteChildClass.class.getName()));
	}

	/**
	 * @see DATAMONGO-1210
	 */
	@Test
	public void mappingShouldOnlyRemoveTypeHintFromTopLevelTypeInCaseOfNestedDocument() {

		WrapperAroundInterfaceType wait = new WrapperAroundInterfaceType();
		wait.interfaceType = new ModelImpl(1);

		Update update = new Update().addToSet("listHoldingConcretyTypeWithInterfaceTypeAttribute").each(wait);
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DomainTypeWithListOfConcreteTypesHavingSingleInterfaceTypeAttribute.class));

		assertThat(mappedUpdate,
				isBsonObject().notContaining("$addToSet.listHoldingConcretyTypeWithInterfaceTypeAttribute.$each.[0]._class"));
		assertThat(mappedUpdate,
				isBsonObject().containing(
						"$addToSet.listHoldingConcretyTypeWithInterfaceTypeAttribute.$each.[0].interfaceType._class",
						ModelImpl.class.getName()));
	}

	/**
	 * @see DATAMONGO-1210
	 */
	@Test
	public void mappingShouldRetainTypeInformationOfNestedListWhenUpdatingConcreteyParentType() {

		ListModelWrapper lmw = new ListModelWrapper();
		lmw.models = Collections.singletonList(new ModelImpl(1));

		Update update = new Update().set("concreteTypeWithListAttributeOfInterfaceType", lmw);
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DomainTypeWrappingConcreteyTypeHavingListOfInterfaceTypeAttributes.class));

		assertThat(mappedUpdate, isBsonObject().notContaining("$set.concreteTypeWithListAttributeOfInterfaceType._class"));
		assertThat(mappedUpdate, isBsonObject()
				.containing("$set.concreteTypeWithListAttributeOfInterfaceType.models.[0]._class", ModelImpl.class.getName()));
	}

	/**
	 * @see DATAMONGO-1236
	 */
	@Test
	public void mappingShouldRetainTypeInformationForObjectValues() {

		Update update = new Update().set("value", new NestedDocument("kaladin"));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObject.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.value.name", "kaladin"));
		assertThat(mappedUpdate, isBsonObject().containing("$set.value._class", NestedDocument.class.getName()));
	}

	/**
	 * @see DATAMONGO-1236
	 */
	@Test
	public void mappingShouldNotRetainTypeInformationForConcreteValues() {

		Update update = new Update().set("concreteValue", new NestedDocument("shallan"));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObject.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.concreteValue.name", "shallan"));
		assertThat(mappedUpdate, isBsonObject().notContaining("$set.concreteValue._class"));
	}

	/**
	 * @see DATAMONGO-1236
	 */
	@Test
	public void mappingShouldRetainTypeInformationForObjectValuesWithAlias() {

		Update update = new Update().set("value", new NestedDocument("adolin"));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithAliasedObject.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.renamed-value.name", "adolin"));
		assertThat(mappedUpdate, isBsonObject().containing("$set.renamed-value._class", NestedDocument.class.getName()));
	}

	/**
	 * @see DATAMONGO-1236
	 */
	@Test
	public void mappingShouldRetrainTypeInformationWhenValueTypeOfMapDoesNotMatchItsDeclaration() {

		Map<Object, Object> map = Collections.singletonMap("szeth", new NestedDocument("son-son-vallano"));

		Update update = new Update().set("map", map);
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObjectMap.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.map.szeth.name", "son-son-vallano"));
		assertThat(mappedUpdate, isBsonObject().containing("$set.map.szeth._class", NestedDocument.class.getName()));
	}

	/**
	 * @see DATAMONGO-1236
	 */
	@Test
	public void mappingShouldNotContainTypeInformationWhenValueTypeOfMapMatchesDeclaration() {

		Map<Object, NestedDocument> map = Collections.singletonMap("jasnah",
				new NestedDocument("kholin"));

		Update update = new Update().set("concreteMap", map);
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObjectMap.class));

		assertThat(mappedUpdate, isBsonObject().containing("$set.concreteMap.jasnah.name", "kholin"));
		assertThat(mappedUpdate, isBsonObject().notContaining("$set.concreteMap.jasnah._class"));
	}

	/**
	 * @see DATAMONGO-1250
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void mapsUpdateWithBothReadingAndWritingConverterRegistered() {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(ClassWithEnum.AllocationToStringConverter.INSTANCE, ClassWithEnum.StringToAllocationConverter.INSTANCE));

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(mock(DbRefResolver.class), mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		UpdateMapper mapper = new UpdateMapper(converter);

		Update update = new Update().set("allocation", ClassWithEnum.Allocation.AVAILABLE);
		Document result = mapper.getMappedObject(update.getUpdateObject(),
				mappingContext.getPersistentEntity(ClassWithEnum.class));

		assertThat(result, isBsonObject().containing("$set.allocation", ClassWithEnum.Allocation.AVAILABLE.code));
	}

	/**
	 * @see DATAMONGO-1251
	 */
	@Test
	public void mapsNullValueCorrectlyForSimpleTypes() {

		Update update = new Update().set("value", null);

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ConcreteChildClass.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.containsKey("value"), is(true));
		assertThat($set.get("value"), nullValue());
	}

	/**
	 * @see DATAMONGO-1251
	 */
	@Test
	public void mapsNullValueCorrectlyForJava8Date() {

		Update update = new Update().set("date", null);

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ClassWithJava8Date.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.containsKey("date"), is(true));
		assertThat($set.get("value"), nullValue());
	}

	/**
	 * @see DATAMONGO-1251
	 */
	@Test
	public void mapsNullValueCorrectlyForCollectionTypes() {

		Update update = new Update().set("values", null);

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModel.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.containsKey("values"), is(true));
		assertThat($set.get("value"), nullValue());
	}

	/**
	 * @see DATAMONGO-1251
	 */
	@Test
	public void mapsNullValueCorrectlyForPropertyOfNestedDocument() {

		Update update = new Update().set("concreteValue.name", null);

		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObject.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.containsKey("concreteValue.name"), is(true));
		assertThat($set.get("concreteValue.name"), nullValue());
	}

	/**
	 * @see DATAMONGO-1288
	 */
	@Test
	public void mapsAtomicIntegerToIntegerCorrectly() {

		Update update = new Update().set("intValue", new AtomicInteger(10));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.get("intValue"), Is.is(10));
	}

	/**
	 * @see DATAMONGO-1288
	 */
	@Test
	public void mapsAtomicIntegerToPrimitiveIntegerCorrectly() {

		Update update = new Update().set("primIntValue", new AtomicInteger(10));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.get("primIntValue"), Is.is(10));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void mapsMinCorrectly() {

		Update update = new Update().min("minfield", 10);
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedUpdate, isBsonObject().containing("$min", new Document("minfield", 10)));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void mapsMaxCorrectly() {

		Update update = new Update().max("maxfield", 999);
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedUpdate, isBsonObject().containing("$max", new Document("maxfield", 999)));
	}

	/**
	 * @see DATAMONGO-1423
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void mappingShouldConsiderCustomConvertersForEnumMapKeys() {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(ClassWithEnum.AllocationToStringConverter.INSTANCE, ClassWithEnum.StringToAllocationConverter.INSTANCE));

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(mock(DbRefResolver.class), mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		UpdateMapper mapper = new UpdateMapper(converter);

		Update update = new Update().set("enumAsMapKey", Collections.singletonMap(ClassWithEnum.Allocation.AVAILABLE, 100));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				mappingContext.getPersistentEntity(ClassWithEnum.class));

		Document $set = DBObjectTestUtils.getAsDocument(mappedUpdate, "$set");
		assertThat($set.containsKey("enumAsMapKey"), is(true));

		Document enumAsMapKey = $set.get("enumAsMapKey", Document.class);
		assertThat(enumAsMapKey.get("AVAILABLE"), is(100));
	}

	/**
	 * @see DATAMONGO-1176
	 */
	@Test
	public void mappingShouldPrepareUpdateObjectForMixedOperatorsAndFields() {

		Document document = new Document("key", "value").append("$set", new Document("a", "b").append("x", "y"));

		Document mappedObject = mapper.getMappedObject(document, context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedObject.get("$set"), is(equalTo(new Document("a", "b").append("x", "y").append("key", "value"))));
		assertThat(mappedObject.size(), is(1));
	}

	/**
	 * @see DATAMONGO-1176
	 */
	@Test
	public void mappingShouldReturnReplaceObject() {

		Document document = new Document("key", "value").append("a", "b").append("x", "y");

		Document mappedObject = mapper.getMappedObject(document, context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedObject.get("key"), is(equalTo("value")));
		assertThat(mappedObject.get("a"), is(equalTo("b")));
		assertThat(mappedObject.get("x"), is(equalTo("y")));
		assertThat(mappedObject.size(), is(3));
	}

	/**
	 * @see DATAMONGO-1176
	 */
	@Test
	public void mappingShouldReturnUpdateObject() {

		Document document = new Document("$push", new Document("x", "y")).append("$set", new Document("a", "b"));

		Document mappedObject = mapper.getMappedObject(document, context.getPersistentEntity(SimpleValueHolder.class));

		assertThat(mappedObject.get("$push"), is(equalTo(new Document("x", "y"))));
		assertThat(mappedObject.get("$set"), is(equalTo(new Document("a", "b"))));
		assertThat(mappedObject.size(), is(2));
	}

	/**
	 * @see DATAMONGO-1486
	 */
	@Test
	public void mappingShouldConvertMapKeysToString() {

		Update update = new Update().set("map", Collections.singletonMap(25, "#StarTrek50"));
		Document mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(EntityWithObjectMap.class));

		Document mapToSet = getAsDocument(getAsDocument(mappedUpdate, "$set"), "map");

		for (Object key : mapToSet.keySet()) {
			assertThat(key, is(instanceOf(String.class)));
		}
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
	interface DocumentWithReferenceToInterface {

		String getId();

		InterfaceDocumentDefinitionWithoutId getReferencedDocument();

	}

	interface InterfaceDocumentDefinitionWithoutId {

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

	interface Model {}

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
	static class NestedEntityWriteConverter implements Converter<NestedEntity, Document> {

		@Override
		public Document convert(NestedEntity source) {
			return new Document();
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

	static class EntityWithAliasedObject {

		@Field("renamed-value") Object value;
	}

	static class EntityWithObjectMap {

		Map<Object, Object> map;
		Map<Object, NestedDocument> concreteMap;
	}

	static class ClassWithEnum {

		Allocation allocation;
		Map<Allocation, String> enumAsMapKey;

		enum Allocation {

			AVAILABLE("V"), ALLOCATED("A");

			String code;

			Allocation(String code) {
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

		enum AllocationToStringConverter implements Converter<Allocation, String> {

			INSTANCE;

			@Override
			public String convert(Allocation source) {
				return source.code;
			}
		}

		enum StringToAllocationConverter implements Converter<String, Allocation> {

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
}
