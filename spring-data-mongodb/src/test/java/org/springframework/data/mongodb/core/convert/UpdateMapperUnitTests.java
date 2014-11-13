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
import static org.hamcrest.collection.IsMapContaining.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.InvalidDataAccessApiUsageException;
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
 */
@RunWith(MockitoJUnitRunner.class)
public class UpdateMapperUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	@Mock MongoDbFactory factory;
	MappingMongoConverter converter;
	MongoMappingContext context;
	UpdateMapper mapper;

	private Converter<NestedEntity, DBObject> writingConverterSpy;

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
	 * @see DATAMONGO-812
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
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

	/**
	 * @see DATAMONGO-812
	 */
	@Test
	public void updateMapperShouldConvertPushWhithoutAddingClassInformationWhenUsedWithEvery() {

		Update update = new Update().push("values").each("spring", "data", "mongodb");

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Model.class));
		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject values = getAsDBObject(push, "values");

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
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject model = getAsDBObject(push, "models");
		BasicDBList each = getAsDBList(model, "$each");
		BasicDBList values = getAsDBList((DBObject) each.get(0), "values");

		assertThat(values.toMap(), (Matcher) allOf(hasValue("spring"), hasValue("data"), hasValue("mongodb")));
	}

	/**
	 * @see DATAMONGO-812
	 */
	@Test
	public void updateMapperShouldRetainClassInformationForPushCorrectlyWhenCalledWithEachUsingCustomTypes() {

		Update update = new Update().push("models").each(new ListModel("spring", "data", "mongodb"));
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ModelWrapper.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		DBObject model = getAsDBObject(push, "models");
		BasicDBList each = getAsDBList(model, "$each");

		assertThat(((DBObject) each.get(0)).get("_class").toString(), equalTo(ListModel.class.getName()));
	}

	/**
	 * @see DATAMONGO-812
	 */
	@Test
	public void testUpdateShouldAllowMultiplePushEachForDifferentFields() {

		Update update = new Update().push("category").each("spring", "data").push("type").each("mongodb");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Object.class));

		DBObject push = getAsDBObject(mappedObject, "$push");
		assertThat(getAsDBObject(push, "category").containsField("$each"), is(true));
		assertThat(getAsDBObject(push, "type").containsField("$each"), is(true));
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

		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject pullClause = getAsDBObject(mappedObject, "$pull");
		assertThat(pullClause.get("dbRefAnnotatedList"), is((Object) new DBRef(null, "entity", "2")));
	}

	/**
	 * @see DATAMONGO-404
	 */
	@Test
	public void createsDbRefForEntityOnPulls() {

		Entity entity = new Entity();
		entity.id = "5";

		Update update = new Update().pull("dbRefAnnotatedList", entity);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject pullClause = getAsDBObject(mappedObject, "$pull");
		assertThat(pullClause.get("dbRefAnnotatedList"), is((Object) new DBRef(null, "entity", entity.id)));
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
		DBObject mappedObject = mapper
				.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(Wrapper.class));

		DBObject pullClause = getAsDBObject(mappedObject, "$pull");
		assertThat(pullClause.containsField("mapped.dbRefAnnotatedList"), is(true));
	}

	/**
	 * @see DATAMONGO-468
	 */
	@Test
	public void rendersUpdateOfDbRefPropertyWithDomainObjectCorrectly() {

		Entity entity = new Entity();
		entity.id = "5";

		Update update = new Update().set("dbRefProperty", entity);
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject setClause = getAsDBObject(mappedObject, "$set");
		assertThat(setClause.get("dbRefProperty"), is((Object) new DBRef(null, "entity", entity.id)));
	}

	/**
	 * @see DATAMONGO-862
	 */
	@Test
	public void rendersUpdateAndPreservesKeyForPathsNotPointingToProperty() {

		Update update = new Update().set("listOfInterface.$.value", "expected-value");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ParentClass.class));

		DBObject setClause = getAsDBObject(mappedObject, "$set");
		assertThat(setClause.containsField("listOfInterface.$.value"), is(true));
	}

	/**
	 * @see DATAMONGO-863
	 */
	@Test
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

	/**
	 * @see DATAMONG0-471
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUpdateShouldApply$addToSetCorrectlyWhenUsedWith$each() {

		Update update = new Update().addToSet("values").each("spring", "data", "mongodb");
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(ListModel.class));

		DBObject addToSet = getAsDBObject(mappedObject, "$addToSet");
		DBObject values = getAsDBObject(addToSet, "values");
		BasicDBList each = getAsDBList(values, "$each");

		assertThat(each.toMap(), (Matcher) allOf(hasValue("spring"), hasValue("data"), hasValue("mongodb")));
	}

	/**
	 * @see DATAMONG0-471
	 */
	@Test
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

	/**
	 * @see DATAMONGO-897
	 */
	@Test
	public void updateOnDbrefPropertyOfInterfaceTypeWithoutExplicitGetterForIdShouldBeMappedCorrectly() {

		Update update = new Update().set("referencedDocument", new InterfaceDocumentDefinitionImpl("1", "Foo"));
		DBObject mappedObject = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithReferenceToInterfaceImpl.class));

		DBObject $set = DBObjectTestUtils.getAsDBObject(mappedObject, "$set");
		Object model = $set.get("referencedDocument");

		DBRef expectedDBRef = new DBRef(factory.getDb(), "interfaceDocumentDefinitionImpl", "1");
		assertThat(model, allOf(instanceOf(DBRef.class), IsEqual.<Object> equalTo(expectedDBRef)));
	}

	/**
	 * @see DATAMONGO-847
	 */
	@Test
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

	/**
	 * @see DATAMONGO-847
	 */
	@Test
	public void updateMapperConvertsPullWithNestedQuerfyOnDBRefCorrectly() {

		Update update = new Update().pull("dbRefAnnotatedList", Query.query(Criteria.where("id").is("1")));
		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject $pull = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$pull");
		DBObject list = DBObjectTestUtils.getAsDBObject($pull, "dbRefAnnotatedList");

		assertThat(list, equalTo(new BasicDBObjectBuilder().add("_id", "1").get()));
	}

	/**
	 * @see DATAMONGO-1077
	 */
	@Test
	public void shouldNotRemovePositionalParameter() {

		Update update = new Update();
		update.unset("dbRefAnnotatedList.$");

		DBObject mappedUpdate = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithDBRefCollection.class));

		DBObject $unset = DBObjectTestUtils.getAsDBObject(mappedUpdate, "$unset");

		assertThat($unset, equalTo(new BasicDBObjectBuilder().add("dbRefAnnotatedList.$", 1).get()));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldMapMinCorrectlyWhenGivenDouble() {

		Update update = new Update().min("doubleVal", 10D);

		DBObject mapped = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNumbers.class));

		assertThat(DBObjectTestUtils.<Double> getValue(mapped, "$min.doubleVal"), equalTo(10D));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldMapMinCorrectlyWhenReferencingNonExistingProperty() {

		Update update = new Update().min("xyz", 10D);

		DBObject mapped = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNumbers.class));

		assertThat(DBObjectTestUtils.<Double> getValue(mapped, "$min.xyz"), equalTo(10D));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldThrowErrorWhenMappingMinWithBigDecimal() {

		exception.expect(InvalidDataAccessApiUsageException.class);
		exception.expectMessage(BigDecimal.class.getName() + " is not supported for $min");

		Update update = new Update().min("bigDeciamVal", new BigDecimal(100));

		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DocumentWithNumbers.class));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldThrowErrorWhenMappingMinToPropertyThatIsBigDecimal() {

		exception.expect(InvalidDataAccessApiUsageException.class);
		exception.expectMessage(BigDecimal.class.getName() + " is not supported for $min");

		Update update = new Update().min("bigDeciamVal", 10);

		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DocumentWithNumbers.class));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldMapMinCorrectlyWhenMappingMinToPropertyThatIsPrimitiveValue() {

		Update update = new Update().min("primitiveIntVal", 10);

		DBObject mapped = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNumbers.class));

		assertThat(DBObjectTestUtils.<Integer> getValue(mapped, "$min.primitiveIntVal"), equalTo(10));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldMapMinOnNestedPropertyCorrectly() {

		Update update = new Update().min("nested.primitiveIntVal", 10);

		DBObject mapped = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNumbersWrapper.class));

		DBObject $min = DBObjectTestUtils.getValue(mapped, "$min");
		assertThat((Integer) $min.get("nested.primitiveIntVal"), is(10));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldThrowErrorWhenMappingMinToNestedPropertyThatIsBigDecimal() {

		exception.expect(InvalidDataAccessApiUsageException.class);
		exception.expectMessage(BigDecimal.class.getName() + " is not supported for $min");

		Update update = new Update().min("nested.bigDeciamVal", 10);

		mapper.getMappedObject(update.getUpdateObject(), context.getPersistentEntity(DocumentWithNumbersWrapper.class));
	}

	/**
	 * @see DATAMONGO-941
	 */
	@Test
	public void shouldMapMinOnNestedPropertyThatDoesNotExistCorrectly() {

		Update update = new Update().min("nested.xyz", 10);

		DBObject mapped = mapper.getMappedObject(update.getUpdateObject(),
				context.getPersistentEntity(DocumentWithNumbersWrapper.class));

		DBObject $min = DBObjectTestUtils.getValue(mapped, "$min");
		assertThat((Integer) $min.get("nested.xyz"), is(10));
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

		@org.springframework.data.mongodb.core.mapping.DBRef//
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

		@Field("aliased")//
		List<? extends AbstractChildClass> list;

		@Field//
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

		@org.springframework.data.mongodb.core.mapping.DBRef//
		public List<Entity> dbRefAnnotatedList;

		@org.springframework.data.mongodb.core.mapping.DBRef//
		public Entity dbRefProperty;
	}

	static class Entity {

		@Id public String id;
		String name;
	}

	static class Wrapper {

		@Field("mapped") DocumentWithDBRefCollection nested;
	}

	static class DocumentWithNumbersWrapper {
		DocumentWithNumbers nested;
	}

	static class DocumentWithNumbers {

		BigInteger bigIntVal;
		BigDecimal bigDeciamVal;
		Byte byteVal;
		Double doubleVal;
		Float floatVal;
		Integer intVal;
		Long longVal;

		byte primitiveByteVal;
		double primitiveDoubleVal;
		float primitiveFloatVal;
		int primitiveIntVal;
		long primitiveLongVal;
	}
}
