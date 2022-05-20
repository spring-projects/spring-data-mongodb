/*
 * Copyright 2013-2022 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperationUnitTests.BookWithFieldAnnotation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;

/**
 * Unit tests for {@link Aggregation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class AggregationUnitTests {

	@Test
	void rejectsNullAggregationOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> newAggregation((AggregationOperation[]) null));
	}

	@Test
	void rejectsNullTypedAggregationOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> newAggregation(String.class, (AggregationOperation[]) null));
	}

	@Test
	void rejectsNoAggregationOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> newAggregation(new AggregationOperation[0]));
	}

	@Test
	void rejectsNoTypedAggregationOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> newAggregation(String.class, new AggregationOperation[0]));
	}

	@Test // DATAMONGO-753
	void checkForCorrectFieldScopeTransfer() {

		assertThatIllegalArgumentException().isThrownBy(() -> {
			newAggregation( //
					project("a", "b"), //
					group("a").count().as("cnt"), // a was introduced to the context by the project operation
					project("cnt", "b") // b was removed from the context by the group operation
			).toDocument("foo", Aggregation.DEFAULT_CONTEXT); // -> triggers IllegalArgumentException
		});
	}

	@Test // DATAMONGO-753
	void unwindOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a"), //
				project("a", "b") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	void unwindOperationWithIndexShouldPreserveFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a", "x"), //
				project("a", "b") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	void unwindOperationWithIndexShouldAddIndexField() {

		newAggregation( //
				project("a", "b"), //
				unwind("a", "x"), //
				project("a", "x") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	void fullUnwindOperationShouldBuildCorrectClause() {

		Document agg = newAggregation( //
				unwind("a", "x", true)).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document unwind = ((List<Document>) agg.get("pipeline")).get(0);
		assertThat(unwind.get("$unwind", Document.class)). //
				containsEntry("includeArrayIndex", "x"). //
				containsEntry("preserveNullAndEmptyArrays", true);
	}

	@Test // DATAMONGO-1391
	void unwindOperationWithPreserveNullShouldBuildCorrectClause() {

		Document agg = newAggregation( //
				unwind("a", true)).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document unwind = ((List<Document>) agg.get("pipeline")).get(0);
		assertThat(unwind) //
				.doesNotContainKey("$unwind.includeArrayIndex") //
				.containsEntry("$unwind.preserveNullAndEmptyArrays", true);
	}

	@Test // DATAMONGO-1550
	void replaceRootOperationShouldBuildCorrectClause() {

		Document agg = newAggregation( //
				replaceRoot().withDocument().andValue("value").as("field")) //
						.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document unwind = ((List<Document>) agg.get("pipeline")).get(0);
		assertThat(unwind).containsEntry("$replaceRoot.newRoot", new Document("field", "value"));
	}

	@Test // DATAMONGO-753
	void matchOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				match(where("a").gte(1)), //
				project("a", "b") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-788
	void referencesToGroupIdsShouldBeRenderedAsReferences() {

		Document agg = newAggregation( //
				project("a"), //
				group("a").count().as("aCnt"), //
				project("aCnt", "a") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(secondProjection, "$project");
		assertThat(fields.get("aCnt")).isEqualTo(1);
		assertThat(fields.get("a")).isEqualTo("$_id.a");
	}

	@Test // DATAMONGO-791
	void allowAggregationOperationsToBePassedAsIterable() {

		List<AggregationOperation> ops = new ArrayList<AggregationOperation>();
		ops.add(project("a"));
		ops.add(group("a").count().as("aCnt"));
		ops.add(project("aCnt", "a"));

		Document agg = newAggregation(ops).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(secondProjection, "$project");
		assertThat(fields.get("aCnt")).isEqualTo(1);
		assertThat(fields.get("a")).isEqualTo("$_id.a");
	}

	@Test // DATAMONGO-791
	void allowTypedAggregationOperationsToBePassedAsIterable() {

		List<AggregationOperation> ops = new ArrayList<>();
		ops.add(project("a"));
		ops.add(group("a").count().as("aCnt"));
		ops.add(project("aCnt", "a"));

		Document agg = newAggregation(Document.class, ops).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(secondProjection, "$project");
		assertThat(fields.get("aCnt")).isEqualTo((Object) 1);
		assertThat(fields.get("a")).isEqualTo((Object) "$_id.a");
	}

	@Test // DATAMONGO-838
	void expressionBasedFieldsShouldBeReferencableInFollowingOperations() {

		Document agg = newAggregation( //
				project("a").andExpression("b+c").as("foo"), //
				group("a").sum("foo").as("foosum") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(1);
		Document fields = getAsDocument(secondProjection, "$group");
		assertThat(fields.get("foosum")).isEqualTo(new Document("$sum", "$foo"));
	}

	@Test // DATAMONGO-908
	void shouldSupportReferingToNestedPropertiesInGroupOperation() {

		Document agg = newAggregation( //
				project("cmsParameterId", "rules"), //
				unwind("rules"), //
				group("cmsParameterId", "rules.ruleType").count().as("totol") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isNotNull();

		Document group = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(group, "$group");
		Document id = getAsDocument(fields, "_id");

		assertThat(id.get("ruleType")).isEqualTo("$rules.ruleType");
	}

	@Test // DATAMONGO-1585
	void shouldSupportSortingBySyntheticAndExposedGroupFields() {

		Document agg = newAggregation( //
				group("cmsParameterId").addToSet("title").as("titles"), //
				sort(Direction.ASC, "cmsParameterId", "titles") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isNotNull();

		Document sort = ((List<Document>) agg.get("pipeline")).get(1);

		assertThat(getAsDocument(sort, "$sort"))
				.isEqualTo(Document.parse("{ \"_id.cmsParameterId\" : 1 , \"titles\" : 1}"));
	}

	@Test // DATAMONGO-1585
	void shouldSupportSortingByProjectedFields() {

		Document agg = newAggregation( //
				project("cmsParameterId") //
						.and(SystemVariable.CURRENT + ".titles").as("titles") //
						.and("field").as("alias"), //
				sort(Direction.ASC, "cmsParameterId", "titles", "alias") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isNotNull();

		Document sort = ((List<Document>) agg.get("pipeline")).get(1);

		assertThat(getAsDocument(sort, "$sort")).containsEntry("cmsParameterId", 1) //
				.containsEntry("titles", 1) //
				.containsEntry("alias", 1);
	}

	@Test // DATAMONGO-924
	void referencingProjectionAliasesFromPreviousStepShouldReferToTheSameFieldTarget() {

		Document agg = newAggregation( //
				project().and("foo.bar").as("ba") //
				, project().and("ba").as("b") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document projection0 = extractPipelineElement(agg, 0, "$project");
		assertThat(projection0).isEqualTo(new Document("ba", "$foo.bar"));

		Document projection1 = extractPipelineElement(agg, 1, "$project");
		assertThat(projection1).isEqualTo(new Document("b", "$ba"));
	}

	@Test // DATAMONGO-960
	void shouldRenderAggregationWithDefaultOptionsCorrectly() {

		Document agg = newAggregation( //
				project().and("a").as("aa") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ \"aggregate\" : \"foo\" , \"pipeline\" : [ { \"$project\" : { \"aa\" : \"$a\"}}]}"));
	}

	@Test // DATAMONGO-960
	void shouldRenderAggregationWithCustomOptionsCorrectly() {

		AggregationOptions aggregationOptions = newAggregationOptions().explain(true).cursor(new Document("foo", 1))
				.allowDiskUse(true).build();

		Document agg = newAggregation( //
				project().and("a").as("aa") //
		) //
				.withOptions(aggregationOptions) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ \"aggregate\" : \"foo\" , " //
				+ "\"pipeline\" : [ { \"$project\" : { \"aa\" : \"$a\"}}] , " //
				+ "\"allowDiskUse\" : true , " //
				+ "\"explain\" : true , " //
				+ "\"cursor\" : { \"foo\" : 1}}") //
		);
	}

	@Test // DATAMONGO-954, DATAMONGO-1585
	void shouldSupportReferencingSystemVariables() {

		Document agg = newAggregation( //
				project("someKey") //
						.and("a").as("a1") //
						.and(Aggregation.CURRENT + ".a").as("a2") //
				, sort(Direction.DESC, "a1") //
				, group("someKey").first(Aggregation.ROOT).as("doc") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document projection0 = extractPipelineElement(agg, 0, "$project");
		assertThat(projection0).isEqualTo(new Document("someKey", 1).append("a1", "$a").append("a2", "$$CURRENT.a"));

		Document sort = extractPipelineElement(agg, 1, "$sort");
		assertThat(sort).isEqualTo(new Document("a1", -1));

		Document group = extractPipelineElement(agg, 2, "$group");
		assertThat(group).isEqualTo(new Document("_id", "$someKey").append("doc", new Document("$first", "$$ROOT")));
	}

	@Test // DATAMONGO-1254
	void shouldExposeAliasedFieldnameForProjectionsIncludingOperationsDownThePipeline() {

		Document agg = Aggregation.newAggregation(//
				project("date") //
						.and("tags").minus(10).as("tags_count")//
				, group("date")//
						.sum("tags_count").as("count")//
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document group = extractPipelineElement(agg, 1, "$group");
		assertThat(getAsDocument(group, "count")).isEqualTo(new Document().append("$sum", "$tags_count"));
	}

	@Test // DATAMONGO-1254
	void shouldUseAliasedFieldnameForProjectionsIncludingOperationsDownThePipelineWhenUsingSpEL() {

		Document agg = Aggregation.newAggregation(//
				project("date") //
						.andExpression("tags-10")//
				, group("date")//
						.sum("tags_count").as("count")//
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document group = extractPipelineElement(agg, 1, "$group");
		assertThat(getAsDocument(group, "count")).isEqualTo(new Document().append("$sum", "$tags_count"));
	}

	@Test // DATAMONGO-861
	void conditionExpressionBasedFieldsShouldBeReferencableInFollowingOperations() {

		Document agg = newAggregation( //
				project("a", "answer"), //
				group("a")
						.first(Cond.newBuilder().when(Criteria.where("a").gte(42)).thenValueOf("answer").otherwise("no-answer"))
						.as("foosum") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(1);
		Document fields = getAsDocument(secondProjection, "$group");
		assertThat(getAsDocument(fields, "foosum")).containsKey("$first");
		assertThat(getAsDocument(fields, "foosum")).containsEntry("$first.$cond.then", "$answer");
		assertThat(getAsDocument(fields, "foosum")).containsEntry("$first.$cond.else", "no-answer");
	}

	@Test // DATAMONGO-861
	void shouldRenderProjectionConditionalExpressionCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project().and(ConditionalOperators.Cond.newBuilder() //
						.when("isYellow") //
						.then("bright") //
						.otherwise("dark")).as("color"))
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 0, "$project");
		Document expectedCondition = new Document() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "color")).containsEntry("$cond", expectedCondition);
	}

	@Test // DATAMONGO-861
	void shouldRenderProjectionConditionalCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project().and("color").applyCondition(ConditionalOperators.Cond.newBuilder() //
						.when("isYellow") //
						.then("bright") //
						.otherwise("dark")))
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 0, "$project");
		Document expectedCondition = new Document() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "color")).containsEntry("$cond", expectedCondition);
	}

	@Test // DATAMONGO-861
	void shouldRenderProjectionConditionalWithCriteriaCorrectly() {

		Document agg = Aggregation.newAggregation(project()//
				.and("color")//
				.applyCondition(ConditionalOperators.Cond.newBuilder().when(Criteria.where("key").gt(5)) //
						.then("bright").otherwise("dark"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 0, "$project");
		Document expectedCondition = new Document() //
				.append("if", new Document("$gt", Arrays.<Object> asList("$key", 5))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "color")).containsEntry("$cond", expectedCondition);
	}

	@Test // DATAMONGO-861, DATAMONGO-2242
	void referencingProjectionAliasesShouldRenderProjectionConditionalWithFieldReferenceCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project().and("color").as("chroma"), project().and("luminosity") //
						.applyCondition(ConditionalOperators //
								.when("chroma") //
								.then("bright") //
								.otherwise("dark"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");
		Document expectedCondition = new Document() //
				.append("if", "$chroma") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "luminosity")).containsEntry("$cond", expectedCondition);
	}

	@Test // DATAMONGO-861
	void referencingProjectionAliasesShouldRenderProjectionConditionalWithCriteriaReferenceCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project().and("color").as("chroma"), project().and("luminosity") //
						.applyCondition(ConditionalOperators.Cond.newBuilder().when(Criteria.where("chroma") //
								.is(100)) //
								.then("bright").otherwise("dark"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");
		Document expectedCondition = new Document() //
				.append("if", new Document("$eq", Arrays.<Object> asList("$chroma", 100))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "luminosity")).containsEntry("$cond", expectedCondition);
	}

	@Test // DATAMONGO-861
	void shouldRenderProjectionIfNullWithFieldReferenceCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project().and("color"), //
				project().and("luminosity") //
						.applyCondition(ConditionalOperators //
								.ifNull("chroma") //
								.then("unknown"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");

		assertThat(getAsDocument(project, "luminosity")).containsEntry("$ifNull", Arrays.asList("$chroma", "unknown"));
	}

	@Test // DATAMONGO-861
	void shouldRenderProjectionIfNullWithFallbackFieldReferenceCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project("fallback").and("color").as("chroma"), project().and("luminosity") //
						.applyCondition(ConditionalOperators.ifNull("chroma") //
								.thenValueOf("fallback"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");

		assertThat(getAsDocument(project, "luminosity")).containsEntry("$ifNull", Arrays.asList("$chroma", "$fallback"));
	}

	@Test // DATAMONGO-1552
	void shouldHonorDefaultCountField() {

		Document agg = Aggregation.newAggregation(//
				bucket("year"), //
				project("count")) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");

		assertThat(project).containsEntry("count", 1);
	}

	@Test // DATAMONGO-1533
	void groupOperationShouldAllowUsageOfDerivedSpELAggregationExpression() {

		Document agg = newAggregation( //
				project("a"), //
				group("a").first(AggregationSpELExpression.expressionOf("cond(a >= 42, 'answer', 'no-answer')")).as("foosum") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(1);
		Document fields = getAsDocument(secondProjection, "$group");
		assertThat(getAsDocument(fields, "foosum")).containsKey("$first");
		assertThat(getAsDocument(fields, "foosum")).containsEntry("$first.$cond.if",
				new Document("$gte", Arrays.asList("$a", 42)));
		assertThat(getAsDocument(fields, "foosum")).containsEntry("$first.$cond.then", "answer");
		assertThat(getAsDocument(fields, "foosum")).containsEntry("$first.$cond.else", "no-answer");
	}

	@Test // DATAMONGO-1756
	void projectOperationShouldRenderNestedFieldNamesCorrectly() {

		Document agg = newAggregation(project().and("value1.value").plus("value2.value").as("val")).toDocument("collection",
				Aggregation.DEFAULT_CONTEXT);

		Document expected = new Document("val", new Document("$add", Arrays.asList("$value1.value", "$value2.value")));
		assertThat(extractPipelineElement(agg, 0, "$project")).isEqualTo(expected);
	}

	@Test // DATAMONGO-1871
	void providedAliasShouldAllowNestingExpressionWithAliasCorrectly() {

		Document condition = new Document("$and",
				Arrays.asList(new Document("$gte", Arrays.asList("$$est.dt", "2015-12-29")), //
						new Document("$lte", Arrays.asList("$$est.dt", "2017-12-29")) //
				));

		Aggregation agg = newAggregation(project("_id", "dId", "aId", "cty", "cat", "plts.plt")
				.and(ArrayOperators.arrayOf("plts.ests").filter().as("est").by(condition)).as("plts.ests"));

		Document $project = extractPipelineElement(agg.toDocument("collection-1", Aggregation.DEFAULT_CONTEXT), 0,
				"$project");

		assertThat($project).containsKey("plts.ests");
	}

	@Test // DATAMONGO-2377
	void shouldAllowInternalThisAndValueReferences() {

		Document untyped = newAggregation( //
				Arrays.asList( //
						(group("uid", "data.sourceId") //
								.push("data.attributeRecords").as("attributeRecordArrays")), //
						(project() //
								.and(ArrayOperators.arrayOf("attributeRecordArrays") //
										.reduce(ArrayOperators.arrayOf("$$value").concat("$$this")) //
										.startingWith(Collections.emptyList())) //
								.as("attributeRecordArrays")) //
				)).toDocument("collection-1", DEFAULT_CONTEXT);

		assertThat(extractPipelineElement(untyped, 1, "$project")).isEqualTo(Document.parse(
				"{\"attributeRecordArrays\": {\"$reduce\": {\"input\": \"$attributeRecordArrays\", \"initialValue\": [], \"in\": {\"$concatArrays\": [\"$$value\", \"$$this\"]}}}}"));
	}

	@Test // DATAMONGO-2644
	void projectOnIdIsAlwaysValid() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		Document target = new Aggregation(bucket("start"), project("_id")).toDocument("collection-1",
				new RelaxedTypeBasedAggregationOperationContext(BookWithFieldAnnotation.class, mappingContext,
						new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext))));

		assertThat(extractPipelineElement(target, 1, "$project")).isEqualTo(Document.parse(" { \"_id\" : \"$_id\" }"));
	}

	@Test // GH-3898
	void shouldNotConvertIncludeExcludeValuesForProjectOperation() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		RelaxedTypeBasedAggregationOperationContext context = new RelaxedTypeBasedAggregationOperationContext(
				WithRetypedIdField.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));
		Document document = project(WithRetypedIdField.class).toDocument(context);
		assertThat(document).isEqualTo(new Document("$project", new Document("_id", 1).append("renamed-field", 1)));
	}

	@Test // GH-4038
	void createsBasicAggregationOperationFromJsonString() {

		AggregationOperation stage = stage("{ $project : { name : 1} }");
		Document target = newAggregation(stage).toDocument("col-1", DEFAULT_CONTEXT);
		assertThat(extractPipelineElement(target, 0, "$project")).containsEntry("name", 1);
	}

	@Test // GH-4038
	void createsBasicAggregationOperationFromBson() {

		AggregationOperation stage = stage(Aggregates.project(Projections.fields(Projections.include("name"))));
		Document target = newAggregation(stage).toDocument("col-1", DEFAULT_CONTEXT);
		assertThat(extractPipelineElement(target, 0, "$project")).containsKey("name");
	}

	private Document extractPipelineElement(Document agg, int index, String operation) {

		List<Document> pipeline = (List<Document>) agg.get("pipeline");
		Object value = pipeline.get(index).get(operation);
		if (value instanceof Document document) {
			return document;
		}
		if (value instanceof Map map) {
			return new Document(map);
		}
		throw new IllegalArgumentException();
	}

	public class WithRetypedIdField {

		@Id @org.springframework.data.mongodb.core.mapping.Field private String id;

		@org.springframework.data.mongodb.core.mapping.Field("renamed-field") private String foo;

	}
}
