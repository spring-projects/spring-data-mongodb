/*
 * Copyright 2013-2018 the original author or authors.
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
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.test.util.BasicDbListBuilder;

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
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT); // -> triggers IllegalArgumentException
	}

	@Test // DATAMONGO-753
	public void unwindOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a"), //
				project("a", "b") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	public void unwindOperationWithIndexShouldPreserveFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a", "x"), //
				project("a", "b") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	public void unwindOperationWithIndexShouldAddIndexField() {

		newAggregation( //
				project("a", "b"), //
				unwind("a", "x"), //
				project("a", "x") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-1391
	public void fullUnwindOperationShouldBuildCorrectClause() {

		Document agg = newAggregation( //
				unwind("a", "x", true)).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document unwind = ((List<Document>) agg.get("pipeline")).get(0);
		assertThat((Document) unwind.get("$unwind"),
				isBsonObject(). //
						containing("includeArrayIndex", "x").//
						containing("preserveNullAndEmptyArrays", true));
	}

	@Test // DATAMONGO-1391
	public void unwindOperationWithPreserveNullShouldBuildCorrectClause() {

		Document agg = newAggregation( //
				unwind("a", true)).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document unwind = ((List<Document>) agg.get("pipeline")).get(0);
		assertThat(unwind,
				isBsonObject().notContaining("includeArrayIndex").containing("preserveNullAndEmptyArrays", true));
	}

	@Test // DATAMONGO-1550
	public void replaceRootOperationShouldBuildCorrectClause() {

		Document agg = newAggregation( //
				replaceRoot().withDocument().andValue("value").as("field")) //
						.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document unwind = ((List<Document>) agg.get("pipeline")).get(0);
		assertThat(unwind, isBsonObject().containing("$replaceRoot.newRoot", new Document("field", "value")));
	}

	@Test // DATAMONGO-753
	public void matchOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				match(where("a").gte(1)), //
				project("a", "b") // b should still be available
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);
	}

	@Test // DATAMONGO-788
	public void referencesToGroupIdsShouldBeRenderedAsReferences() {

		Document agg = newAggregation( //
				project("a"), //
				group("a").count().as("aCnt"), //
				project("aCnt", "a") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(secondProjection, "$project");
		assertThat(fields.get("aCnt"), is((Object) 1));
		assertThat(fields.get("a"), is((Object) "$_id.a"));
	}

	@Test // DATAMONGO-791
	public void allowAggregationOperationsToBePassedAsIterable() {

		List<AggregationOperation> ops = new ArrayList<AggregationOperation>();
		ops.add(project("a"));
		ops.add(group("a").count().as("aCnt"));
		ops.add(project("aCnt", "a"));

		Document agg = newAggregation(ops).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(secondProjection, "$project");
		assertThat(fields.get("aCnt"), is((Object) 1));
		assertThat(fields.get("a"), is((Object) "$_id.a"));
	}

	@Test // DATAMONGO-791
	public void allowTypedAggregationOperationsToBePassedAsIterable() {

		List<AggregationOperation> ops = new ArrayList<AggregationOperation>();
		ops.add(project("a"));
		ops.add(group("a").count().as("aCnt"));
		ops.add(project("aCnt", "a"));

		Document agg = newAggregation(Document.class, ops).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(secondProjection, "$project");
		assertThat(fields.get("aCnt"), is((Object) 1));
		assertThat(fields.get("a"), is((Object) "$_id.a"));
	}

	@Test // DATAMONGO-838
	public void expressionBasedFieldsShouldBeReferencableInFollowingOperations() {

		Document agg = newAggregation( //
				project("a").andExpression("b+c").as("foo"), //
				group("a").sum("foo").as("foosum") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(1);
		Document fields = getAsDocument(secondProjection, "$group");
		assertThat(fields.get("foosum"), is((Object) new Document("$sum", "$foo")));
	}

	@Test // DATAMONGO-908
	public void shouldSupportReferingToNestedPropertiesInGroupOperation() {

		Document agg = newAggregation( //
				project("cmsParameterId", "rules"), //
				unwind("rules"), //
				group("cmsParameterId", "rules.ruleType").count().as("totol") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(notNullValue()));

		Document group = ((List<Document>) agg.get("pipeline")).get(2);
		Document fields = getAsDocument(group, "$group");
		Document id = getAsDocument(fields, "_id");

		assertThat(id.get("ruleType"), is((Object) "$rules.ruleType"));
	}

	@Test // DATAMONGO-1585
	public void shouldSupportSortingBySyntheticAndExposedGroupFields() {

		Document agg = newAggregation( //
				group("cmsParameterId").addToSet("title").as("titles"), //
				sort(Direction.ASC, "cmsParameterId", "titles") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(notNullValue()));

		Document sort = ((List<Document>) agg.get("pipeline")).get(1);

		assertThat(getAsDocument(sort, "$sort"), is(Document.parse("{ \"_id.cmsParameterId\" : 1 , \"titles\" : 1}")));
	}

	@Test // DATAMONGO-1585
	public void shouldSupportSortingByProjectedFields() {

		Document agg = newAggregation( //
				project("cmsParameterId") //
						.and(SystemVariable.CURRENT + ".titles").as("titles") //
						.and("field").as("alias"), //
				sort(Direction.ASC, "cmsParameterId", "titles", "alias") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(notNullValue()));

		Document sort = ((List<Document>) agg.get("pipeline")).get(1);

		assertThat(getAsDocument(sort, "$sort"),
				isBsonObject().containing("cmsParameterId", 1) //
						.containing("titles", 1) //
						.containing("alias", 1));
	}

	@Test // DATAMONGO-924
	public void referencingProjectionAliasesFromPreviousStepShouldReferToTheSameFieldTarget() {

		Document agg = newAggregation( //
				project().and("foo.bar").as("ba") //
				, project().and("ba").as("b") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document projection0 = extractPipelineElement(agg, 0, "$project");
		assertThat(projection0, is((Document) new Document("ba", "$foo.bar")));

		Document projection1 = extractPipelineElement(agg, 1, "$project");
		assertThat(projection1, is((Document) new Document("b", "$ba")));
	}

	@Test // DATAMONGO-960
	public void shouldRenderAggregationWithDefaultOptionsCorrectly() {

		Document agg = newAggregation( //
				project().and("a").as("aa") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ \"aggregate\" : \"foo\" , \"pipeline\" : [ { \"$project\" : { \"aa\" : \"$a\"}}]}")));
	}

	@Test // DATAMONGO-960
	public void shouldRenderAggregationWithCustomOptionsCorrectly() {

		AggregationOptions aggregationOptions = newAggregationOptions().explain(true).cursor(new Document("foo", 1))
				.allowDiskUse(true).build();

		Document agg = newAggregation( //
				project().and("a").as("aa") //
		) //
				.withOptions(aggregationOptions) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ \"aggregate\" : \"foo\" , " //
						+ "\"pipeline\" : [ { \"$project\" : { \"aa\" : \"$a\"}}] , " //
						+ "\"allowDiskUse\" : true , " //
						+ "\"explain\" : true , " //
						+ "\"cursor\" : { \"foo\" : 1}}") //
				));
	}

	@Test // DATAMONGO-954, DATAMONGO-1585
	public void shouldSupportReferencingSystemVariables() {

		Document agg = newAggregation( //
				project("someKey") //
						.and("a").as("a1") //
						.and(Aggregation.CURRENT + ".a").as("a2") //
				, sort(Direction.DESC, "a1") //
				, group("someKey").first(Aggregation.ROOT).as("doc") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document projection0 = extractPipelineElement(agg, 0, "$project");
		assertThat(projection0, is((Document) new Document("someKey", 1).append("a1", "$a").append("a2", "$$CURRENT.a")));

		Document sort = extractPipelineElement(agg, 1, "$sort");
		assertThat(sort, is((Document) new Document("a1", -1)));

		Document group = extractPipelineElement(agg, 2, "$group");
		assertThat(group, is((Document) new Document("_id", "$someKey").append("doc", new Document("$first", "$$ROOT"))));
	}

	@Test // DATAMONGO-1254
	public void shouldExposeAliasedFieldnameForProjectionsIncludingOperationsDownThePipeline() {

		Document agg = Aggregation.newAggregation(//
				project("date") //
						.and("tags").minus(10).as("tags_count")//
				, group("date")//
						.sum("tags_count").as("count")//
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document group = extractPipelineElement(agg, 1, "$group");
		assertThat(getAsDocument(group, "count"), is(new Document().append("$sum", "$tags_count")));
	}

	@Test // DATAMONGO-1254
	public void shouldUseAliasedFieldnameForProjectionsIncludingOperationsDownThePipelineWhenUsingSpEL() {

		Document agg = Aggregation.newAggregation(//
				project("date") //
						.andExpression("tags-10")//
				, group("date")//
						.sum("tags_count").as("count")//
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document group = extractPipelineElement(agg, 1, "$group");
		assertThat(getAsDocument(group, "count"), is(new Document().append("$sum", "$tags_count")));
	}

	@Test // DATAMONGO-861
	public void conditionExpressionBasedFieldsShouldBeReferencableInFollowingOperations() {

		Document agg = newAggregation( //
				project("a", "answer"), //
				group("a")
						.first(Cond.newBuilder().when(Criteria.where("a").gte(42)).thenValueOf("answer").otherwise("no-answer"))
						.as("foosum") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(1);
		Document fields = getAsDocument(secondProjection, "$group");
		assertThat(getAsDocument(fields, "foosum"), isBsonObject().containing("$first"));
		assertThat(getAsDocument(fields, "foosum"), isBsonObject().containing("$first.$cond.then", "$answer"));
		assertThat(getAsDocument(fields, "foosum"), isBsonObject().containing("$first.$cond.else", "no-answer"));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionConditionalExpressionCorrectly() {

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

		assertThat(getAsDocument(project, "color"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionConditionalCorrectly() {

		Document agg = Aggregation.newAggregation(//
				project().and("color")
						.applyCondition(ConditionalOperators.Cond.newBuilder() //
								.when("isYellow") //
								.then("bright") //
								.otherwise("dark")))
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 0, "$project");
		Document expectedCondition = new Document() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "color"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionConditionalWithCriteriaCorrectly() {

		Document agg = Aggregation
				.newAggregation(project()//
						.and("color")//
						.applyCondition(ConditionalOperators.Cond.newBuilder().when(Criteria.where("key").gt(5)) //
								.then("bright").otherwise("dark"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 0, "$project");
		Document expectedCondition = new Document() //
				.append("if", new Document("$gt", Arrays.<Object> asList("$key", 5))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "color"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void referencingProjectionAliasesShouldRenderProjectionConditionalWithFieldReferenceCorrectly() {

		Document agg = Aggregation
				.newAggregation(//
						project().and("color").as("chroma"),
						project().and("luminosity") //
								.applyCondition(ConditionalOperators //
										.when("chroma") //
										.thenValueOf("bright") //
										.otherwise("dark"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");
		Document expectedCondition = new Document() //
				.append("if", "$chroma") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "luminosity"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void referencingProjectionAliasesShouldRenderProjectionConditionalWithCriteriaReferenceCorrectly() {

		Document agg = Aggregation
				.newAggregation(//
						project().and("color").as("chroma"),
						project().and("luminosity") //
								.applyCondition(ConditionalOperators.Cond.newBuilder()
										.when(Criteria.where("chroma") //
												.is(100)) //
										.then("bright").otherwise("dark"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");
		Document expectedCondition = new Document() //
				.append("if", new Document("$eq", Arrays.<Object> asList("$chroma", 100))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(getAsDocument(project, "luminosity"), isBsonObject().containing("$cond", expectedCondition));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionIfNullWithFieldReferenceCorrectly() {

		Document agg = Aggregation
				.newAggregation(//
						project().and("color"), //
						project().and("luminosity") //
								.applyCondition(ConditionalOperators //
										.ifNull("chroma") //
										.then("unknown"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");

		assertThat(getAsDocument(project, "luminosity"),
				isBsonObject().containing("$ifNull", Arrays.<Object> asList("$chroma", "unknown")));
	}

	@Test // DATAMONGO-861
	public void shouldRenderProjectionIfNullWithFallbackFieldReferenceCorrectly() {

		Document agg = Aggregation
				.newAggregation(//
						project("fallback").and("color").as("chroma"),
						project().and("luminosity") //
								.applyCondition(ConditionalOperators.ifNull("chroma") //
										.thenValueOf("fallback"))) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");

		assertThat(getAsDocument(project, "luminosity"),
				isBsonObject().containing("$ifNull", Arrays.asList("$chroma", "$fallback")));
	}

	@Test // DATAMONGO-1552
	public void shouldHonorDefaultCountField() {

		Document agg = Aggregation
				.newAggregation(//
						bucket("year"), //
						project("count")) //
				.toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		Document project = extractPipelineElement(agg, 1, "$project");

		assertThat(project, isBsonObject().containing("count", 1));
	}

	@Test // DATAMONGO-1533
	public void groupOperationShouldAllowUsageOfDerivedSpELAggregationExpression() {

		Document agg = newAggregation( //
				project("a"), //
				group("a").first(AggregationSpELExpression.expressionOf("cond(a >= 42, 'answer', 'no-answer')")).as("foosum") //
		).toDocument("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		Document secondProjection = ((List<Document>) agg.get("pipeline")).get(1);
		Document fields = getAsDocument(secondProjection, "$group");
		assertThat(getAsDocument(fields, "foosum"), isBsonObject().containing("$first"));
		assertThat(getAsDocument(fields, "foosum"),
				isBsonObject().containing("$first.$cond.if", new Document("$gte", new Document("$a", 42))));
		assertThat(getAsDocument(fields, "foosum"), isBsonObject().containing("$first.$cond.then", "answer"));
		assertThat(getAsDocument(fields, "foosum"), isBsonObject().containing("$first.$cond.else", "no-answer"));
	}

	@Test // DATAMONGO-1756
	public void projectOperationShouldRenderNestedFieldNamesCorrectly() {

		Document agg = newAggregation(project().and("value1.value").plus("value2.value").as("val")).toDocument("collection",
				Aggregation.DEFAULT_CONTEXT);

		assertThat(extractPipelineElement(agg, 0, "$project"),
				is(equalTo(new Document("val", new Document("$add", Arrays.asList("$value1.value", "$value2.value"))))));
	}

	private Document extractPipelineElement(Document agg, int index, String operation) {

		List<Document> pipeline = (List<Document>) agg.get("pipeline");
		return (Document) pipeline.get(index).get(operation);
	}
}
