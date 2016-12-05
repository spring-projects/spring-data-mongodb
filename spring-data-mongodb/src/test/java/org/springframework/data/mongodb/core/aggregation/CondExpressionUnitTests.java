/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Cond.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Cond;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ConditionalOperators;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link Cond}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class CondExpressionUnitTests {

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsEmptyFieldName() {
		newBuilder().when("");
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsNullFieldName() {
		newBuilder().when((DBObject) null);
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsNullCriteriaName() {
		newBuilder().when((Criteria) null);
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsBuilderAsThenValue() {
		newBuilder().when("isYellow").then(newBuilder().when("field").then("then-value")).otherwise("otherwise");
	}

	/**
	 * @see DATAMONGO-861, DATAMONGO-1542
	 */
	@Test
	public void simpleBuilderShouldRenderCorrectly() {

		Cond operator = ConditionalOperators.when("isYellow").thenValueOf("bright").otherwise("dark");
		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		DBObject expectedCondition = new BasicDBObject() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(dbObject, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861, DATAMONGO-1542
	 */
	@Test
	public void simpleCriteriaShouldRenderCorrectly() {

		Cond operator = ConditionalOperators.when(Criteria.where("luminosity").gte(100)).thenValueOf("bright")
				.otherwise("dark");
		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		DBObject expectedCondition = new BasicDBObject() //
				.append("if", new BasicDBObject("$gte", Arrays.<Object> asList("$luminosity", 100))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(dbObject, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void andCriteriaShouldRenderCorrectly() {

		Cond operator = ConditionalOperators.when(Criteria.where("luminosity").gte(100) //
				.andOperator(Criteria.where("hue").is(50), //
						Criteria.where("saturation").lt(11)))
				.thenValueOf("bright").otherwiseValueOf("dark-field");

		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		BasicDBObject luminosity = new BasicDBObject("$gte", Arrays.<Object> asList("$luminosity", 100));
		BasicDBObject hue = new BasicDBObject("$eq", Arrays.<Object> asList("$hue", 50));
		BasicDBObject saturation = new BasicDBObject("$lt", Arrays.<Object> asList("$saturation", 11));

		DBObject expectedCondition = new BasicDBObject() //
				.append("if", Arrays.<Object> asList(luminosity, new BasicDBObject("$and", Arrays.asList(hue, saturation)))) //
				.append("then", "bright") //
				.append("else", "$dark-field");

		assertThat(dbObject, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861, DATAMONGO-1542
	 */
	@Test
	public void twoArgsCriteriaShouldRenderCorrectly() {

		Criteria criteria = Criteria.where("luminosity").gte(100) //
				.and("saturation").and("chroma").is(200);
		Cond operator = ConditionalOperators.when(criteria).thenValueOf("bright").otherwise("dark");

		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		BasicDBObject gte = new BasicDBObject("$gte", Arrays.<Object> asList("$luminosity", 100));
		BasicDBObject is = new BasicDBObject("$eq", Arrays.<Object> asList("$chroma", 200));

		DBObject expectedCondition = new BasicDBObject() //
				.append("if", Arrays.asList(gte, is)) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(dbObject, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861, DATAMONGO-1542
	 */
	@Test
	public void nestedCriteriaShouldRenderCorrectly() {

		Cond operator = ConditionalOperators.when(Criteria.where("luminosity").gte(100)) //
				.thenValueOf(newBuilder() //
						.when(Criteria.where("luminosity").gte(200)) //
						.then("verybright") //
						.otherwise("not-so-bright")) //
				.otherwise(newBuilder() //
						.when(Criteria.where("luminosity").lt(50)) //
						.then("very-dark") //
						.otherwise("not-so-dark"));

		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		DBObject trueCondition = new BasicDBObject() //
				.append("if", new BasicDBObject("$gte", Arrays.<Object> asList("$luminosity", 200))) //
				.append("then", "verybright") //
				.append("else", "not-so-bright");

		DBObject falseCondition = new BasicDBObject() //
				.append("if", new BasicDBObject("$lt", Arrays.<Object> asList("$luminosity", 50))) //
				.append("then", "very-dark") //
				.append("else", "not-so-dark");

		assertThat(dbObject, isBsonObject().containing("$cond.then.$cond", trueCondition));
		assertThat(dbObject, isBsonObject().containing("$cond.else.$cond", falseCondition));
	}
}
