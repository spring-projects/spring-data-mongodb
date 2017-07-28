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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.test.util.BasicDbListBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link Aggregation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class AggregationUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullAggregationOperation() {
		newAggregation((AggregationOperation[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullTypedAggregationOperation() {
		newAggregation(String.class, (AggregationOperation[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNoAggregationOperation() {
		newAggregation(new AggregationOperation[0]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNoTypedAggregationOperation() {
		newAggregation(String.class, new AggregationOperation[0]);
	}

	@Test // DATAMONGO-753
	public void checkForCorrectFieldScopeTransfer() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid reference");
		exception.expectMessage("'b'");

		newAggregation( //
				project("a", "b"), //
				group("a").count().as("cnt"), // a was introduced to the context by the project operation
				project("cnt", "b") // b was removed from the context by the group operation
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT); // -> triggers IllegalArgumentException
	}

	@Test // DATAMONGO-753
	public void unwindOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a"), //
				project("a", "b") // b should still be available
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	public void unwindOperationWithIndexShouldPreserveFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a", "x"), //
				project("a", "b") // b should still be available
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	public void unwindOperationWithIndexShouldAddIndexField() {

		newAggregation( //
				project("a", "b"), //
				unwind("a", "x"), //
				project("a", "x") // b should still be available
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	public void fullUnwindOperationShouldBuildCorrectClause() {

		DBObject agg = newAggregation( //
				unwind("a", "x", true)).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject unwind = ((List<DBObject>) agg.get("pipeline")).get(0);
		assertThat((DBObject) unwind.get("$unwind"),
				isBsonObject(). //
						containing("includeArrayIndex", "x").//
						containing("preserveNullAndEmptyArrays", true));
	}

	@Test // DATAMONGO-1391
	public void unwindOperationWithPreserveNullShouldBuildCorrectClause() {

		DBObject agg = newAggregation( //
				unwind("a", true)).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject unwind = ((List<DBObject>) agg.get("pipeline")).get(0);
		assertThat(unwind,
				isBsonObject().notContaining("includeArrayIndex").containing("preserveNullAndEmptyArrays", true));
	}

	@Test // DATAMONGO-1550
	public void replaceRootOperationShouldBuildCorrectClause() {

		DBObject agg = newAggregation( //
				replaceRoot().withDocument().andValue("value").as("field")) //
						.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject unwind = ((List<DBObject>) agg.get("pipeline")).get(0);
		assertThat(unwind, isBsonObject().containing("$replaceRoot.newRoot", new BasicDBObject("field", "value")));
	}

	@Test // DATAMONGO-753
	public void matchOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				match(where("a").gte(1)), //
				project("a", "b") // b should still be available
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-788
	public void referencesToGroupIdsShouldBeRenderedAsReferences() {

		DBObject agg = newAggregation( //
				project("a"), //
				group("a").count().as("aCnt"), //
				project("aCnt", "a") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(2);
		DBObject fields = getAsDBObject(secondProjection, "$project");
		assertThat(fields.get("aCnt"), is((Object) 1));
		assertThat(fields.get("a"), is((Object) "$_id.a"));
	}

	@Test // DATAMONGO-791
	public void allowAggregationOperationsToBePassedAsIterable() {

		List<AggregationOperation> ops = new ArrayList<AggregationOperation>();
		ops.add(project("a"));
		ops.add(group("a").count().as("aCnt"));
		ops.add(project("aCnt", "a"));

		DBObject agg = newAggregation(ops).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(2);
		DBObject fields = getAsDBObject(secondProjection, "$project");
		assertThat(fields.get("aCnt"), is((Object) 1));
		assertThat(fields.get("a"), is((Object) "$_id.a"));
	}

	@Test // DATAMONGO-791
	public void allowTypedAggregationOperationsToBePassedAsIterable() {

		List<AggregationOperation> ops = new ArrayList<AggregationOperation>();
		ops.add(project("a"));
		ops.add(group("a").count().as("aCnt"));
		ops.add(project("aCnt", "a"));

		DBObject agg = newAggregation(DBObject.class, ops).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(2);
		DBObject fields = getAsDBObject(secondProjection, "$project");
		assertThat(fields.get("aCnt"), is((Object) 1));
		assertThat(fields.get("a"), is((Object) "$_id.a"));
	}

	@Test // DATAMONGO-838
	public void expressionBasedFieldsShouldBeReferencableInFollowingOperations() {

		DBObject agg = newAggregation( //
				project("a").andExpression("b+c").as("foo"), //
				group("a").sum("foo").as("foosum") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(1);
		DBObject fields = getAsDBObject(secondProjection, "$group");
		assertThat(fields.get("foosum"), is((Object) new BasicDBObject("$sum", "$foo")));
	}

	@Test // DATAMONGO-908
	public void shouldSupportReferingToNestedPropertiesInGroupOperation() {

		DBObject agg = newAggregation( //
				project("cmsParameterId", "rules"), //
				unwind("rules"), //
				group("cmsParameterId", "rules.ruleType").count().as("totol") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(notNullValue()));

		DBObject group = ((List<DBObject>) agg.get("pipeline")).get(2);
		DBObject fields = getAsDBObject(group, "$group");
		DBObject id = getAsDBObject(fields, "_id");

		assertThat(id.get("ruleType"), is((Object) "$rules.ruleType"));
	}

	@Test // DATAMONGO-1585
	public void shouldSupportSortingBySyntheticAndExposedGroupFields() {

		DBObject agg = newAggregation( //
				group("cmsParameterId").addToSet("title").as("titles"), //
				sort(Direction.ASC, "cmsParameterId", "titles") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(notNullValue()));

		DBObject sort = ((List<DBObject>) agg.get("pipeline")).get(1);

		assertThat(getAsDBObject(sort, "$sort"), is(JSON.parse("{ \"_id.cmsParameterId\" : 1 , \"titles\" : 1}")));
	}

	@Test // DATAMONGO-1585
	public void shouldSupportSortingByProjectedFields() {

		DBObject agg = newAggregation( //
				project("cmsParameterId") //
						.and(SystemVariable.CURRENT + ".titles").as("titles") //
						.and("field").as("alias"), //
				sort(Direction.ASC, "cmsParameterId", "titles", "alias") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(notNullValue()));

		DBObject sort = ((List<DBObject>) agg.get("pipeline")).get(1);

		assertThat(getAsDBObject(sort, "$sort"),
				isBsonObject().containing("cmsParameterId", 1) //
						.containing("titles", 1) //
						.containing("alias", 1));
	}

	@Test // DATAMONGO-924
	public void referencingProjectionAliasesFromPreviousStepShouldReferToTheSameFieldTarget() {

		DBObject agg = newAggregation( //
				project().and("foo.bar").as("ba") //
				, project().and("ba").as("b") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject projection0 = extractPipelineElement(agg, 0, "$project");
		assertThat(projection0, is((DBObject) new BasicDBObject("ba", "$foo.bar")));

		DBObject projection1 = extractPipelineElement(agg, 1, "$project");
		assertThat(projection1, is((DBObject) new BasicDBObject("b", "$ba")));
	}

	@Test // DATAMONGO-960
	public void shouldRenderAggregationWithDefaultOptionsCorrectly() {

		DBObject agg = newAggregation( //
				project().and("a").as("aa") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg.toString(),
				is("{ \"aggregate\" : \"foo\" , \"pipeline\" : [ { \"$project\" : { \"aa\" : \"$a\"}}]}"));
	}

	@Test // DATAMONGO-960
	public void shouldRenderAggregationWithCustomOptionsCorrectly() {

		AggregationOptions aggregationOptions = newAggregationOptions().explain(true).cursor(new BasicDBObject("foo", 1))
				.allowDiskUse(true).build();

		DBObject agg = newAggregation( //
				project().and("a").as("aa") //
		) //
				.withOptions(aggregationOptions) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg.toString(),
				is("{ \"aggregate\" : \"foo\" , " //
						+ "\"pipeline\" : [ { \"$project\" : { \"aa\" : \"$a\"}}] , " //
						+ "\"allowDiskUse\" : true , " //
						+ "\"explain\" : true , " //
						+ "\"cursor\" : { \"foo\" : 1}}" //
				));
	}

	@Test // DATAMONGO-954, DATAMONGO-1585
	public void shouldSupportReferencingSystemVariables() {

		DBObject agg = newAggregation( //
				project("someKey") //
						.and("a").as("a1") //
						.and(Aggregation.CURRENT + ".a").as("a2") //
				, sort(Direction.DESC, "a1") //
				, group("someKey").first(Aggregation.ROOT).as("doc") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject projection0 = extractPipelineElement(agg, 0, "$project");
		assertThat(projection0,
				is((DBObject) new BasicDBObject("someKey", 1).append("a1", "$a").append("a2", "$$CURRENT.a")));

		DBObject sort = extractPipelineElement(agg, 1, "$sort");
		assertThat(sort, is((DBObject) new BasicDBObject("a1", -1)));

		DBObject group = extractPipelineElement(agg, 2, "$group");
		assertThat(group,
				is((DBObject) new BasicDBObject("_id", "$someKey").append("doc", new BasicDBObject("$first", "$$ROOT"))));
	}

	@Test // DATAMONGO-1254
	public void shouldExposeAliasedFieldnameForProjectionsIncludingOperationsDownThePipeline() {

		DBObject agg = Aggregation.newAggregation(//
				project("date") //
						.and("tags").minus(10).as("tags_count")//
				, group("date")//
						.sum("tags_count").as("count")//
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject group = extractPipelineElement(agg, 1, "$group");
		assertThat(getAsDBObject(group, "count"), is(new BasicDBObjectBuilder().add("$sum", "$tags_count").get()));
	}

	@Test // DATAMONGO-1254
	public void shouldUseAliasedFieldnameForProjectionsIncludingOperationsDownThePipelineWhenUsingSpEL() {

		DBObject agg = Aggregation.newAggregation(//
				project("date") //
						.andExpression("tags-10")//
				, group("date")//
						.sum("tags_count").as("count")//
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject group = extractPipelineElement(agg, 1, "$group");
		assertThat(getAsDBObject(group, "count"), is(new BasicDBObjectBuilder().add("$sum", "$tags_count").get()));
	}

	@Test // DATAMONGO-861
	public void conditionExpressionBasedFieldsShouldBeReferencableInFollowingOperations() {

		DBObject agg = newAggregation( //
				project("a"), //
				group("a")
						.first(ConditionalOperators.when(Criteria.where("a").gte(42)).thenValueOf("answer").otherwise("no-answer"))
						.as("foosum") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(1);
		DBObject fields = getAsDBObject(secondProjection, "$group");
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first"));
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first.$cond.then", "answer"));
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first.$cond.else", "no-answer"));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionConditionalExpressionCorrectly() {

		DBObject agg = Aggregation.newAggregation(//
				project().and(ConditionalOperators.Cond.newBuilder() //
						.when("isYellow") //
						.then("bright") //
						.otherwise("dark")).as("color"))
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 0, "$project");
		DBObject expectedCondition = new BasicDBObject() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDBObject(project, "color"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionConditionalCorrectly() {

		DBObject agg = Aggregation.newAggregation(//
				project().and("color")
						.applyCondition(ConditionalOperators.Cond.newBuilder() //
								.when("isYellow") //
								.then("bright") //
								.otherwise("dark")))
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 0, "$project");
		DBObject expectedCondition = new BasicDBObject() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDBObject(project, "color"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionConditionalWithCriteriaCorrectly() {

		DBObject agg = Aggregation
				.newAggregation(project()//
						.and("color")//
						.applyCondition(ConditionalOperators.Cond.newBuilder().when(Criteria.where("key").gt(5)) //
								.then("bright").otherwise("dark"))) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 0, "$project");
		DBObject expectedCondition = new BasicDBObject() //
				.append("if", new BasicDBObject("$gt", Arrays.<Object> asList("$key", 5))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDBObject(project, "color"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void referencingProjectionAliasesShouldRenderProjectionConditionalWithFieldReferenceCorrectly() {

		DBObject agg = Aggregation
				.newAggregation(//
						project().and("color").as("chroma"),
						project().and("luminosity") //
								.applyCondition(ConditionalOperators //
										.when("chroma") //
										.thenValueOf("bright") //
										.otherwise("dark"))) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 1, "$project");
		DBObject expectedCondition = new BasicDBObject() //
				.append("if", "$chroma") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDBObject(project, "luminosity"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void referencingProjectionAliasesShouldRenderProjectionConditionalWithCriteriaReferenceCorrectly() {

		DBObject agg = Aggregation
				.newAggregation(//
						project().and("color").as("chroma"),
						project().and("luminosity") //
								.applyCondition(ConditionalOperators.Cond.newBuilder()
										.when(Criteria.where("chroma") //
												.is(100)) //
										.then("bright").otherwise("dark"))) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 1, "$project");
		DBObject expectedCondition = new BasicDBObject() //
				.append("if", new BasicDBObject("$eq", Arrays.<Object> asList("$chroma", 100))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDBObject(project, "luminosity"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionIfNullWithFieldReferenceCorrectly() {

		DBObject agg = Aggregation
				.newAggregation(//
						project().and("color"), //
						project().and("luminosity") //
								.applyCondition(ConditionalOperators //
										.ifNull("chroma") //
										.then("unknown"))) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 1, "$project");

		assertThat(getAsDBObject(project, "luminosity"),
				isBsonObject().containing("$ifNull", Arrays.<Object> asList("$chroma", "unknown")));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionIfNullWithFallbackFieldReferenceCorrectly() {

		DBObject agg = Aggregation
				.newAggregation(//
						project("fallback").and("color").as("chroma"),
						project().and("luminosity") //
								.applyCondition(ConditionalOperators.ifNull("chroma") //
										.thenValueOf("fallback"))) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 1, "$project");

		assertThat(getAsDBObject(project, "luminosity"),
				isBsonObject().containing("$ifNull", Arrays.asList("$chroma", "$fallback")));
	}

	@Test // DATAMONGO-1552
	public void shouldHonorDefaultCountField() {

		DBObject agg = Aggregation
				.newAggregation(//
						bucket("year"), //
						project("count")) //
				.toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		DBObject project = extractPipelineElement(agg, 1, "$project");

		assertThat(project, isBsonObject().containing("count", 1));
	}

	@Test // DATAMONGO-1533
	public void groupOperationShouldAllowUsageOfDerivedSpELAggregationExpression() {

		DBObject agg = newAggregation( //
				project("a"), //
				group("a").first(AggregationSpELExpression.expressionOf("cond(a >= 42, 'answer', 'no-answer')")).as("foosum") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(1);
		DBObject fields = getAsDBObject(secondProjection, "$group");
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first"));
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first.$cond.if",
				new BasicDBObject("$gte", new BasicDbListBuilder().add("$a").add(42).get())));
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first.$cond.then", "answer"));
		assertThat(getAsDBObject(fields, "foosum"), isBsonObject().containing("$first.$cond.else", "no-answer"));
	}

	@Test // DATAMONGO-1756
	public void projectOperationShouldRenderNestedFieldNamesCorrectly() {

		DBObject agg = newAggregation(project().and("value1.value").plus("value2.value").as("val")).toDbObject("collection",
				Aggregation.DEFAULT_CONTEXT);

		assertThat((BasicDBObject) extractPipelineElement(agg, 0, "$project"), is(equalTo(new BasicDBObject("val",
				new BasicDBObject("$add", new BasicDbListBuilder().add("$value1.value").add("$value2.value").get())))));
	}

	private DBObject extractPipelineElement(DBObject agg, int index, String operation) {

		List<DBObject> pipeline = (List<DBObject>) agg.get("pipeline");
		return (DBObject) pipeline.get(index).get(operation);
	}
}
