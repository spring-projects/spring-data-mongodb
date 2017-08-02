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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;
import java.util.List;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.test.util.BasicDbListBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link TypeBasedAggregationOperationContext}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class TypeBasedAggregationOperationContextUnitTests {

	MongoMappingContext context;
	MappingMongoConverter converter;
	QueryMapper mapper;

	@Mock DbRefResolver dbRefResolver;

	@Before
	public void setUp() {

		this.context = new MongoMappingContext();
		this.converter = new MappingMongoConverter(dbRefResolver, context);
		this.mapper = new QueryMapper(converter);
	}

	@Test
	public void findsSimpleReference() {
		assertThat(getContext(Foo.class).getReference("bar"), is(notNullValue()));
	}

	@Test(expected = MappingException.class)
	public void rejectsInvalidFieldReference() {
		getContext(Foo.class).getReference("foo");
	}

	@Test // DATAMONGO-741
	public void returnsReferencesToNestedFieldsCorrectly() {

		AggregationOperationContext context = getContext(Foo.class);

		Field field = field("bar.name");

		assertThat(context.getReference("bar.name"), is(notNullValue()));
		assertThat(context.getReference(field), is(notNullValue()));
		assertThat(context.getReference(field), is(context.getReference("bar.name")));
	}

	@Test // DATAMONGO-806
	public void aliasesIdFieldCorrectly() {

		AggregationOperationContext context = getContext(Foo.class);
		assertThat(context.getReference("id"),
				is((FieldReference) new DirectFieldReference(new ExposedField(field("id", "_id"), true))));
	}

	@Test // DATAMONGO-912
	public void shouldUseCustomConversionIfPresentAndConversionIsRequiredInFirstStage() {

		CustomConversions customConversions = customAgeConversions();
		converter.setCustomConversions(customConversions);
		customConversions.registerConvertersIn((GenericConversionService) converter.getConversionService());

		AggregationOperationContext context = getContext(FooPerson.class);

		MatchOperation matchStage = match(Criteria.where("age").is(new Age(10)));
		ProjectionOperation projectStage = project("age", "name");

		DBObject agg = newAggregation(matchStage, projectStage).toDbObject("test", context);

		DBObject age = getValue((DBObject) getValue(getPipelineElementFromAggregationAt(agg, 0), "$match"), "age");
		assertThat(age, is((DBObject) new BasicDBObject("v", 10)));
	}

	@Test // DATAMONGO-912
	public void shouldUseCustomConversionIfPresentAndConversionIsRequiredInLaterStage() {

		CustomConversions customConversions = customAgeConversions();
		converter.setCustomConversions(customConversions);
		customConversions.registerConvertersIn((GenericConversionService) converter.getConversionService());

		AggregationOperationContext context = getContext(FooPerson.class);

		MatchOperation matchStage = match(Criteria.where("age").is(new Age(10)));
		ProjectionOperation projectStage = project("age", "name");

		DBObject agg = newAggregation(projectStage, matchStage).toDbObject("test", context);

		DBObject age = getValue((DBObject) getValue(getPipelineElementFromAggregationAt(agg, 1), "$match"), "age");
		assertThat(age, is((DBObject) new BasicDBObject("v", 10)));
	}

	@Test // DATAMONGO-960
	public void rendersAggregationOptionsInTypedAggregationContextCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class, project("name", "age")) //
				.withOptions(
						newAggregationOptions().allowDiskUse(true).explain(true).cursor(new BasicDBObject("foo", 1)).build());

		DBObject dbo = agg.toDbObject("person", context);

		DBObject projection = getPipelineElementFromAggregationAt(dbo, 0);
		assertThat(projection.containsField("$project"), is(true));

		assertThat(projection.get("$project"), is((Object) new BasicDBObject("name", 1).append("age", 1)));

		assertThat(dbo.get("allowDiskUse"), is((Object) true));
		assertThat(dbo.get("explain"), is((Object) true));
		assertThat(dbo.get("cursor"), is((Object) new BasicDBObject("foo", 1)));
	}

	@Test // DATAMONGO-1585
	public void rendersSortOfProjectedFieldCorrectly() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class, project().and("counterName").as("counter"), //
				sort(Direction.ASC, "counter"));

		DBObject dbo = agg.toDbObject("meterData", context);
		DBObject sort = getPipelineElementFromAggregationAt(dbo, 1);

		DBObject definition = (DBObject) sort.get("$sort");
		assertThat(definition.get("counter"), is(equalTo((Object) 1)));
	}

	@Test // DATAMONGO-1586
	public void rendersFieldAliasingProjectionCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class,
				project() //
						.and("name").as("person_name") //
						.and("age.value").as("age"));

		DBObject dbo = agg.toDbObject("person", context);

		DBObject projection = getPipelineElementFromAggregationAt(dbo, 0);
		assertThat(getAsDBObject(projection, "$project"),
				isBsonObject() //
						.containing("person_name", "$name") //
						.containing("age", "$age.value"));
	}

	@Test // DATAMONGO-1133
	public void shouldHonorAliasedFieldsInGroupExpressions() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				group("counterName").sum("counterVolume").as("totalCounterVolume"));

		DBObject dbo = agg.toDbObject("meterData", context);
		DBObject group = getPipelineElementFromAggregationAt(dbo, 0);

		DBObject definition = (DBObject) group.get("$group");

		assertThat(definition.get("_id"), is(equalTo((Object) "$counter_name")));
	}

	@Test // DATAMONGO-1326, DATAMONGO-1585
	public void lookupShouldInheritFieldsFromInheritingAggregationOperation() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"), //
				sort(Direction.ASC, "resourceId", "counterName"));

		DBObject dbo = agg.toDbObject("meterData", context);
		DBObject sort = getPipelineElementFromAggregationAt(dbo, 1);

		DBObject definition = (DBObject) sort.get("$sort");

		assertThat(definition.get("resourceId"), is(equalTo((Object) 1)));
		assertThat(definition.get("counter_name"), is(equalTo((Object) 1)));
	}

	@Test // DATAMONGO-1326
	public void groupLookupShouldInheritFieldsFromPreviousAggregationOperation() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class, group().min("resourceId").as("foreignKey"),
				lookup("OtherCollection", "foreignKey", "otherId", "lookup"), sort(Direction.ASC, "foreignKey"));

		DBObject dbo = agg.toDbObject("meterData", context);
		DBObject sort = getPipelineElementFromAggregationAt(dbo, 2);

		DBObject definition = (DBObject) sort.get("$sort");

		assertThat(definition.get("foreignKey"), is(equalTo((Object) 1)));
	}

	@Test // DATAMONGO-1326
	public void lookupGroupAggregationShouldUseCorrectGroupField() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"),
				group().min("lookup.otherkey").as("something_totally_different"));

		DBObject dbo = agg.toDbObject("meterData", context);
		DBObject group = getPipelineElementFromAggregationAt(dbo, 1);

		DBObject definition = (DBObject) group.get("$group");
		DBObject field = (DBObject) definition.get("something_totally_different");

		assertThat(field.get("$min"), is(equalTo((Object) "$lookup.otherkey")));
	}

	@Test // DATAMONGO-1326
	public void lookupGroupAggregationShouldOverwriteExposedFields() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"),
				group().min("lookup.otherkey").as("something_totally_different"),
				sort(Direction.ASC, "something_totally_different"));

		DBObject dbo = agg.toDbObject("meterData", context);
		DBObject sort = getPipelineElementFromAggregationAt(dbo, 2);

		DBObject definition = (DBObject) sort.get("$sort");

		assertThat(definition.get("something_totally_different"), is(equalTo((Object) 1)));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1326
	public void lookupGroupAggregationShouldFailInvalidFieldReference() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"),
				group().min("lookup.otherkey").as("something_totally_different"), sort(Direction.ASC, "resourceId"));

		agg.toDbObject("meterData", context);
	}

	@Test // DATAMONGO-861
	public void rendersAggregationConditionalInTypedAggregationContextCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class,
				project("name") //
						.and("age") //
						.applyCondition(
								ConditionalOperators.when(Criteria.where("age.value").lt(10)).then(new Age(0)).otherwiseValueOf("age")) //
		);

		DBObject dbo = agg.toDbObject("person", context);

		DBObject projection = getPipelineElementFromAggregationAt(dbo, 0);
		assertThat(projection.containsField("$project"), is(true));

		DBObject project = getValue(projection, "$project");
		DBObject age = getValue(project, "age");

		assertThat((DBObject) getValue(age, "$cond"), isBsonObject().containing("then.value", 0));
		assertThat((DBObject) getValue(age, "$cond"), isBsonObject().containing("then._class", Age.class.getName()));
		assertThat((DBObject) getValue(age, "$cond"), isBsonObject().containing("else", "$age"));
	}

	@Test // DATAMONGO-861, DATAMONGO-1542
	public void rendersAggregationIfNullInTypedAggregationContextCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class,
				project("name") //
						.and("age") //
						.applyCondition(ConditionalOperators.ifNull("age").then(new Age(0))) //
		);

		DBObject dbo = agg.toDbObject("person", context);

		DBObject projection = getPipelineElementFromAggregationAt(dbo, 0);
		assertThat(projection.containsField("$project"), is(true));

		DBObject project = getValue(projection, "$project");
		DBObject age = getValue(project, "age");

		assertThat(age, is(JSON.parse(
				"{ $ifNull: [ \"$age\", { \"_class\":\"org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContextUnitTests$Age\",  \"value\": 0} ] }")));

		assertThat(age, isBsonObject().containing("$ifNull.[0]", "$age"));
		assertThat(age, isBsonObject().containing("$ifNull.[1].value", 0));
		assertThat(age, isBsonObject().containing("$ifNull.[1]._class", Age.class.getName()));
	}

	@Test // DATAMONGO-1756
	public void projectOperationShouldRenderNestedFieldNamesCorrectlyForTypedAggregation() {

		AggregationOperationContext context = getContext(Wrapper.class);

		DBObject agg = newAggregation(Wrapper.class, project().and("nested1.value1").plus("nested2.value2").as("val"))
				.toDbObject("collection", context);

		BasicDBObject project = (BasicDBObject) getPipelineElementFromAggregationAt(agg, 0).get("$project");
		assertThat(project, is(equalTo(new BasicDBObject("val", new BasicDBObject("$add",
				new BasicDbListBuilder().add("$nested1.value1").add("$field2.nestedValue2").get())))));
	}

	@Document(collection = "person")
	public static class FooPerson {

		final ObjectId id;
		final String name;
		final Age age;

		@PersistenceConstructor
		FooPerson(ObjectId id, String name, Age age) {
			this.id = id;
			this.name = name;
			this.age = age;
		}
	}

	public static class Age {

		final int value;

		Age(int value) {
			this.value = value;
		}
	}

	public CustomConversions customAgeConversions() {
		return new CustomConversions(Arrays.<Converter<?, ?>> asList(ageWriteConverter(), ageReadConverter()));
	}

	Converter<Age, DBObject> ageWriteConverter() {
		return new Converter<Age, DBObject>() {
			@Override
			public DBObject convert(Age age) {
				return new BasicDBObject("v", age.value);
			}
		};
	}

	Converter<DBObject, Age> ageReadConverter() {
		return new Converter<DBObject, Age>() {
			@Override
			public Age convert(DBObject dbObject) {
				return new Age(((Integer) dbObject.get("v")));
			}
		};
	}

	@SuppressWarnings("unchecked")
	static DBObject getPipelineElementFromAggregationAt(DBObject agg, int index) {
		return ((List<DBObject>) agg.get("pipeline")).get(index);
	}

	@SuppressWarnings("unchecked")
	static <T> T getValue(DBObject o, String key) {
		return (T) o.get(key);
	}

	private TypeBasedAggregationOperationContext getContext(Class<?> type) {
		return new TypeBasedAggregationOperationContext(type, context, mapper);
	}

	static class Foo {

		@Id String id;
		Bar bar;
	}

	static class Bar {

		String name;
	}

	static class Wrapper {

		Nested nested1;
		@org.springframework.data.mongodb.core.mapping.Field("field2") Nested nested2;
	}

	static class Nested {
		String value1;
		@org.springframework.data.mongodb.core.mapping.Field("nestedValue2") String value2;
	}
}
