/*
 * Copyright 2013-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import static org.springframework.data.mongodb.core.DocumentTestUtils.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.Variable;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.SetOperators.SetUnion;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link TypeBasedAggregationOperationContext}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
public class TypeBasedAggregationOperationContextUnitTests {

	MongoMappingContext context;
	MappingMongoConverter converter;
	QueryMapper mapper;

	@Mock DbRefResolver dbRefResolver;

	@BeforeEach
	public void setUp() {

		this.context = new MongoMappingContext();
		this.converter = new MappingMongoConverter(dbRefResolver, context);
		this.mapper = new QueryMapper(converter);
	}

	@Test
	public void findsSimpleReference() {
		assertThat(getContext(Foo.class).getReference("bar")).isNotNull();
	}

	@Test
	public void rejectsInvalidFieldReference() {
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> getContext(Foo.class).getReference("foo"));
	}

	@Test // DATAMONGO-741
	public void returnsReferencesToNestedFieldsCorrectly() {

		AggregationOperationContext context = getContext(Foo.class);

		Field field = field("bar.name");

		assertThat(context.getReference("bar.name")).isNotNull();
		assertThat(context.getReference(field)).isNotNull();
		assertThat(context.getReference(field)).isEqualTo(context.getReference("bar.name"));
	}

	@Test // DATAMONGO-806
	public void aliasesIdFieldCorrectly() {

		AggregationOperationContext context = getContext(Foo.class);
		assertThat(context.getReference("id"))
				.isEqualTo(new DirectFieldReference(new ExposedField(field("id", "_id"), true)));
	}

	@Test // DATAMONGO-912
	public void shouldUseCustomConversionIfPresentAndConversionIsRequiredInFirstStage() {

		CustomConversions customConversions = customAgeConversions();
		converter.setCustomConversions(customConversions);
		customConversions.registerConvertersIn((GenericConversionService) converter.getConversionService());

		AggregationOperationContext context = getContext(FooPerson.class);

		MatchOperation matchStage = match(Criteria.where("age").is(new Age(10)));
		ProjectionOperation projectStage = project("age", "name");

		org.bson.Document agg = newAggregation(matchStage, projectStage).toDocument("test", context);

		org.bson.Document age = getValue(getValue(getPipelineElementFromAggregationAt(agg, 0), "$match"), "age");
		assertThat(age).isEqualTo(new Document("v", 10));
	}

	@Test // DATAMONGO-912
	public void shouldUseCustomConversionIfPresentAndConversionIsRequiredInLaterStage() {

		CustomConversions customConversions = customAgeConversions();
		converter.setCustomConversions(customConversions);
		customConversions.registerConvertersIn((GenericConversionService) converter.getConversionService());

		AggregationOperationContext context = getContext(FooPerson.class);

		MatchOperation matchStage = match(Criteria.where("age").is(new Age(10)));
		ProjectionOperation projectStage = project("age", "name");

		org.bson.Document agg = newAggregation(projectStage, matchStage).toDocument("test", context);

		org.bson.Document age = getValue(getValue(getPipelineElementFromAggregationAt(agg, 1), "$match"), "age");
		assertThat(age).isEqualTo(new Document("v", 10));
	}

	@Test // DATAMONGO-960
	public void rendersAggregationOptionsInTypedAggregationContextCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class, project("name", "age")) //
				.withOptions(
						newAggregationOptions().allowDiskUse(true).explain(true).cursor(new org.bson.Document("foo", 1)).build());

		org.bson.Document document = agg.toDocument("person", context);

		org.bson.Document projection = getPipelineElementFromAggregationAt(document, 0);
		assertThat(projection.containsKey("$project")).isTrue();

		assertThat(projection.get("$project")).isEqualTo(new Document("name", 1).append("age", 1));

		assertThat(document.get("allowDiskUse")).isEqualTo(true);
		assertThat(document.get("explain")).isEqualTo(true);
		assertThat(document.get("cursor")).isEqualTo(new Document("foo", 1));
	}

	@Test // DATAMONGO-1585
	public void rendersSortOfProjectedFieldCorrectly() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class, project().and("counterName").as("counter"), //
				sort(Direction.ASC, "counter"));

		Document dbo = agg.toDocument("meterData", context);
		Document sort = getPipelineElementFromAggregationAt(dbo, 1);

		Document definition = (Document) sort.get("$sort");
		assertThat(definition.get("counter")).isEqualTo(1);
	}

	@Test // DATAMONGO-1586
	public void rendersFieldAliasingProjectionCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class, project() //
				.and("name").as("person_name") //
				.and("age.value").as("age"));

		Document dbo = agg.toDocument("person", context);

		Document projection = getPipelineElementFromAggregationAt(dbo, 0);
		assertThat(getAsDocument(projection, "$project")).containsEntry("person_name", "$name") //
				.containsEntry("age", "$age.value");
	}

	@Test // DATAMONGO-1893
	public void considersIncludedFieldsFromSingleExclusionsCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class, project() //
				.andExclude("name"), sort(Sort.by("age.value", "lastName")));

		Document dbo = agg.toDocument("person", context);

		Document sort = getPipelineElementFromAggregationAt(dbo, 1);
		assertThat(getAsDocument(sort, "$sort")).isEqualTo(new Document("age.value", 1).append("last_name", 1));
	}

	@Test // DATAMONGO-1133
	public void shouldHonorAliasedFieldsInGroupExpressions() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				group("counterName").sum("counterVolume").as("totalCounterVolume"));

		org.bson.Document document = agg.toDocument("meterData", context);
		org.bson.Document group = getPipelineElementFromAggregationAt(document, 0);

		org.bson.Document definition = (org.bson.Document) group.get("$group");

		assertThat(definition.get("_id")).isEqualTo("$counter_name");
	}

	@Test // DATAMONGO-1326, DATAMONGO-1585
	public void lookupShouldInheritFieldsFromInheritingAggregationOperation() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"), //
				sort(Direction.ASC, "resourceId", "counterName"));

		org.bson.Document document = agg.toDocument("meterData", context);
		org.bson.Document sort = getPipelineElementFromAggregationAt(document, 1);

		org.bson.Document definition = (org.bson.Document) sort.get("$sort");

		assertThat(definition.get("resourceId")).isEqualTo(1);
		assertThat(definition.get("counter_name")).isEqualTo(1);
	}

	@Test // DATAMONGO-1326
	public void groupLookupShouldInheritFieldsFromPreviousAggregationOperation() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class, group().min("resourceId").as("foreignKey"),
				lookup("OtherCollection", "foreignKey", "otherId", "lookup"), sort(Direction.ASC, "foreignKey"));

		org.bson.Document document = agg.toDocument("meterData", context);
		org.bson.Document sort = getPipelineElementFromAggregationAt(document, 2);

		org.bson.Document definition = (org.bson.Document) sort.get("$sort");

		assertThat(definition.get("foreignKey")).isEqualTo(1);
	}

	@Test // DATAMONGO-1326
	public void lookupGroupAggregationShouldUseCorrectGroupField() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"),
				group().min("lookup.otherkey").as("something_totally_different"));

		org.bson.Document document = agg.toDocument("meterData", context);
		org.bson.Document group = getPipelineElementFromAggregationAt(document, 1);

		org.bson.Document definition = (org.bson.Document) group.get("$group");
		org.bson.Document field = (org.bson.Document) definition.get("something_totally_different");

		assertThat(field.get("$min")).isEqualTo("$lookup.otherkey");
	}

	@Test // DATAMONGO-1326
	public void lookupGroupAggregationShouldOverwriteExposedFields() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"),
				group().min("lookup.otherkey").as("something_totally_different"),
				sort(Direction.ASC, "something_totally_different"));

		org.bson.Document document = agg.toDocument("meterData", context);
		org.bson.Document sort = getPipelineElementFromAggregationAt(document, 2);

		org.bson.Document definition = (org.bson.Document) sort.get("$sort");

		assertThat(definition.get("something_totally_different")).isEqualTo(1);
	}

	@Test // DATAMONGO-1326
	public void lookupGroupAggregationShouldFailInvalidFieldReference() {

		TypeBasedAggregationOperationContext context = getContext(MeterData.class);
		TypedAggregation<MeterData> agg = newAggregation(MeterData.class,
				lookup("OtherCollection", "resourceId", "otherId", "lookup"),
				group().min("lookup.otherkey").as("something_totally_different"), sort(Direction.ASC, "resourceId"));

		assertThatIllegalArgumentException().isThrownBy(() -> agg.toDocument("meterData", context));
	}

	@Test // DATAMONGO-861
	public void rendersAggregationConditionalInTypedAggregationContextCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class, project("name") //
				.and("age") //
				.applyCondition(
						ConditionalOperators.when(Criteria.where("age.value").lt(10)).then(new Age(0)).otherwiseValueOf("age")) //
		);

		Document document = agg.toDocument("person", context);

		Document projection = getPipelineElementFromAggregationAt(document, 0);
		assertThat(projection.containsKey("$project")).isTrue();

		Document project = getValue(projection, "$project");
		Document age = getValue(project, "age");

		assertThat((Document) getValue(age, "$cond")).containsEntry("then.value", 0);
		assertThat((Document) getValue(age, "$cond")).containsEntry("then._class", Age.class.getName());
		assertThat((Document) getValue(age, "$cond")).containsEntry("else", "$age");
	}

	/**
	 * .AggregationUnitTests
	 */
	@Test // DATAMONGO-861, DATAMONGO-1542
	public void rendersAggregationIfNullInTypedAggregationContextCorrectly() {

		AggregationOperationContext context = getContext(FooPerson.class);
		TypedAggregation<FooPerson> agg = newAggregation(FooPerson.class, project("name") //
				.and("age") //
				.applyCondition(ConditionalOperators.ifNull("age").then(new Age(0))) //
		);

		Document document = agg.toDocument("person", context);

		Document projection = getPipelineElementFromAggregationAt(document, 0);
		assertThat(projection.containsKey("$project")).isTrue();

		Document project = getValue(projection, "$project");
		Document age = getValue(project, "age");

		assertThat(age).isEqualTo(Document.parse(
				"{ $ifNull: [ \"$age\", { \"_class\":\"org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContextUnitTests$Age\",  \"value\": 0} ] }"));

		assertThat(age).containsEntry("$ifNull.[0]", "$age");
		assertThat(age).containsEntry("$ifNull.[1].value", 0);
		assertThat(age).containsEntry("$ifNull.[1]._class", Age.class.getName());
	}

	@Test // DATAMONGO-1756
	public void projectOperationShouldRenderNestedFieldNamesCorrectlyForTypedAggregation() {

		AggregationOperationContext context = getContext(Wrapper.class);

		Document agg = newAggregation(Wrapper.class, project().and("nested1.value1").plus("nested2.value2").as("val"))
				.toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", new Document("$add", Arrays.asList("$nested1.value1", "$field2.nestedValue2"))));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnUnwrappableTypeFieldCorrectly() {

		AggregationOperationContext context = getContext(WithUnwrapped.class);

		Document agg = newAggregation(WithUnwrapped.class, project().and("unwrappedValue.stringValue").as("val"))
				.toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$stringValue"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnUnwrappedFieldWithAtFieldAnnotationCorrectly() {

		AggregationOperationContext context = getContext(WithUnwrapped.class);

		Document agg = newAggregation(WithUnwrapped.class, project().and("unwrappedValue.atFieldAnnotatedValue").as("val"))
				.toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$with-at-field-annotation"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnPrefixedUnwrappedFieldCorrectly() {

		AggregationOperationContext context = getContext(WithUnwrapped.class);

		Document agg = newAggregation(WithUnwrapped.class, project().and("prefixedUnwrappedValue.stringValue").as("val"))
				.toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$prefix-stringValue"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnPrefixedUnwrappedFieldWithAtFieldAnnotationCorrectly() {

		AggregationOperationContext context = getContext(WithUnwrapped.class);

		Document agg = newAggregation(WithUnwrapped.class,
				project().and("prefixedUnwrappedValue.atFieldAnnotatedValue").as("val")).toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$prefix-with-at-field-annotation"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnNestedUnwrappedFieldCorrectly() {

		AggregationOperationContext context = getContext(WrapperAroundWithUnwrapped.class);

		Document agg = newAggregation(WrapperAroundWithUnwrapped.class,
				project().and("withUnwrapped.unwrappedValue.stringValue").as("val")).toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$withUnwrapped.stringValue"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnNestedUnwrappedFieldWithAtFieldAnnotationCorrectly() {

		AggregationOperationContext context = getContext(WrapperAroundWithUnwrapped.class);

		Document agg = newAggregation(WrapperAroundWithUnwrapped.class,
				project().and("withUnwrapped.unwrappedValue.atFieldAnnotatedValue").as("val")).toDocument("collection",
						context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$withUnwrapped.with-at-field-annotation"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnNestedPrefixedUnwrappedFieldCorrectly() {

		AggregationOperationContext context = getContext(WrapperAroundWithUnwrapped.class);

		Document agg = newAggregation(WrapperAroundWithUnwrapped.class,
				project().and("withUnwrapped.prefixedUnwrappedValue.stringValue").as("val")).toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$withUnwrapped.prefix-stringValue"));
	}

	@Test // DATAMONGO-1902
	void rendersProjectOnNestedPrefixedUnwrappedFieldWithAtFieldAnnotationCorrectly() {

		AggregationOperationContext context = getContext(WrapperAroundWithUnwrapped.class);

		Document agg = newAggregation(WrapperAroundWithUnwrapped.class,
				project().and("withUnwrapped.prefixedUnwrappedValue.atFieldAnnotatedValue").as("val")).toDocument("collection",
						context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project"))
				.isEqualTo(new Document("val", "$withUnwrapped.prefix-with-at-field-annotation"));
	}

	@Test // GH-4070
	void rendersLocalVariables() {

		AggregationOperationContext context = getContext(WithLists.class);

		Document agg = newAggregation(WithLists.class,
				project()
						.and(Reduce.arrayOf("listOfListOfString").withInitialValue(field("listOfString"))
								.reduce(SetUnion.arrayAsSet(Variable.VALUE.getTarget()).union(Variable.THIS.getTarget())))
						.as("listOfString")).toDocument("collection", context);

		assertThat(getPipelineElementFromAggregationAt(agg, 0).get("$project")).isEqualTo(Document.parse("""
				{
					"listOfString" : {
						"$reduce" : {
							"in" : { "$setUnion" : ["$$value", "$$this"] },
							"initialValue" : "$listOfString",
							"input" : "$listOfListOfString"
						}
					}
				}
				"""));
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "person")
	public static class FooPerson {

		final ObjectId id;
		final String name;
		@org.springframework.data.mongodb.core.mapping.Field("last_name") final String lastName;
		final Age age;

		public FooPerson(ObjectId id, String name, String lastName, Age age) {
			this.id = id;
			this.name = name;
			this.lastName = lastName;
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
		return new MongoCustomConversions(Arrays.asList(ageWriteConverter(), ageReadConverter()));
	}

	Converter<Age, org.bson.Document> ageWriteConverter() {
		return new Converter<Age, org.bson.Document>() {
			@Override
			public org.bson.Document convert(Age age) {
				return new org.bson.Document("v", age.value);
			}
		};
	}

	Converter<org.bson.Document, Age> ageReadConverter() {
		return new Converter<org.bson.Document, Age>() {
			@Override
			public Age convert(org.bson.Document document) {
				return new Age(((Integer) document.get("v")));
			}
		};
	}

	@SuppressWarnings("unchecked")
	static org.bson.Document getPipelineElementFromAggregationAt(org.bson.Document agg, int index) {
		return ((List<org.bson.Document>) agg.get("pipeline")).get(index);
	}

	@SuppressWarnings("unchecked")
	static <T> T getValue(org.bson.Document o, String key) {
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

	static class WrapperAroundWithUnwrapped {

		String id;
		WithUnwrapped withUnwrapped;
	}

	static class WithUnwrapped {

		String id;

		@Unwrapped.Nullable UnwrappableType unwrappedValue;
		@Unwrapped.Nullable("prefix-") UnwrappableType prefixedUnwrappedValue;
	}

	static class UnwrappableType {

		String stringValue;

		@org.springframework.data.mongodb.core.mapping.Field("with-at-field-annotation") //
		String atFieldAnnotatedValue;
	}

	static class WithLists {
		public List<String> listOfString;
		public List<List<String>> listOfListOfString;
	}
}
