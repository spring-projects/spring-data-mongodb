/*
 * Copyright 2010-2015 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Test cases for {@link Update}.
 * 
 * @author Oliver Gierke
 * @author Thomas Risberg
 * @author Becca Gaspard
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public class UpdateTests {

	@Test
	public void testSet() {

		Update u = new Update().set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject().toString(), is("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	@Test
	public void testSetSet() {

		Update u = new Update().set("directory", "/Users/Test/Desktop").set("size", 0);
		assertThat(u.getUpdateObject().toString(),
				is("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\" , \"size\" : 0}}"));
	}

	@Test
	public void testInc() {

		Update u = new Update().inc("size", 1);
		assertThat(u.getUpdateObject().toString(), is("{ \"$inc\" : { \"size\" : 1}}"));
	}

	@Test
	public void testIncInc() {

		Update u = new Update().inc("size", 1).inc("count", 1);
		assertThat(u.getUpdateObject().toString(), is("{ \"$inc\" : { \"size\" : 1 , \"count\" : 1}}"));
	}

	@Test
	public void testIncAndSet() {

		Update u = new Update().inc("size", 1).set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject().toString(),
				is("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	@Test
	public void testUnset() {

		Update u = new Update().unset("directory");
		assertThat(u.getUpdateObject().toString(), is("{ \"$unset\" : { \"directory\" : 1}}"));
	}

	@Test
	public void testPush() {

		Update u = new Update().push("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject().toString(), is("{ \"$push\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test
	public void testPushAll() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pushAll("authors", new Object[] { m1, m2 });
		assertThat(u.getUpdateObject().toString(),
				is("{ \"$pushAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}"));
	}

	/**
	 * @see DATAMONGO-354
	 */
	@Test
	public void testMultiplePushAllShouldBePossibleWhenUsingDifferentFields() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pushAll("authors", new Object[] { m1, m2 });
		u.pushAll("books", new Object[] { "Spring in Action" });

		assertThat(
				u.getUpdateObject().toString(),
				is("{ \"$pushAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}] , \"books\" : [ \"Spring in Action\"]}}"));
	}

	@Test
	public void testAddToSet() {

		Update u = new Update().addToSet("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject().toString(), is("{ \"$addToSet\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test
	public void testPop() {

		Update u = new Update().pop("authors", Update.Position.FIRST);
		assertThat(u.getUpdateObject().toString(), is("{ \"$pop\" : { \"authors\" : -1}}"));

		u = new Update().pop("authors", Update.Position.LAST);
		assertThat(u.getUpdateObject().toString(), is("{ \"$pop\" : { \"authors\" : 1}}"));
	}

	@Test
	public void testPull() {

		Update u = new Update().pull("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject().toString(), is("{ \"$pull\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test
	public void testPullAll() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pullAll("authors", new Object[] { m1, m2 });
		assertThat(u.getUpdateObject().toString(),
				is("{ \"$pullAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}"));
	}

	@Test
	public void testRename() {

		Update u = new Update().rename("directory", "folder");
		assertThat(u.getUpdateObject().toString(), is("{ \"$rename\" : { \"directory\" : \"folder\"}}"));
	}

	@Test
	public void testBasicUpdateInc() {

		Update u = new Update().inc("size", 1);
		assertThat(u.getUpdateObject().toString(), is("{ \"$inc\" : { \"size\" : 1}}"));
	}

	@Test
	public void testBasicUpdateIncAndSet() {

		Update u = new BasicUpdate("{ \"$inc\" : { \"size\" : 1}}").set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject().toString(),
				is("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	/**
	 * @see DATAMONGO-630
	 */
	@Test
	public void testSetOnInsert() {

		Update u = new Update().setOnInsert("size", 1);
		assertThat(u.getUpdateObject().toString(), is("{ \"$setOnInsert\" : { \"size\" : 1}}"));
	}

	/**
	 * @see DATAMONGO-630
	 */
	@Test
	public void testSetOnInsertSetOnInsert() {

		Update u = new Update().setOnInsert("size", 1).setOnInsert("count", 1);
		assertThat(u.getUpdateObject().toString(), is("{ \"$setOnInsert\" : { \"size\" : 1 , \"count\" : 1}}"));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnTrueWhenMultiFieldOperationAddedForField() {

		Update update = new Update().set("foo", "bar");
		assertThat(update.modifies("foo"), is(true));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnFalseWhenMultiFieldOperationAddedForField() {

		Update update = new Update().set("foo", "bar");
		assertThat(update.modifies("oof"), is(false));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnTrueWhenSingleFieldOperationAddedForField() {

		Update update = new Update().pullAll("foo", new Object[] { "bar" });
		assertThat(update.modifies("foo"), is(true));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnFalseWhenSingleFieldOperationAddedForField() {

		Update update = new Update().pullAll("foo", new Object[] { "bar" });
		assertThat(update.modifies("oof"), is(false));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnFalseWhenCalledOnEmptyUpdate() {
		assertThat(new Update().modifies("foo"), is(false));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnTrueWhenUpdateWithKeyCreatedFromDbObject() {

		Update update = new Update().set("foo", "bar");
		Update clone = Update.fromDBObject(update.getUpdateObject());

		assertThat(clone.modifies("foo"), is(true));
	}

	/**
	 * @see DATAMONGO-852
	 */
	@Test
	public void testUpdateAffectsFieldShouldReturnFalseWhenUpdateWithoutKeyCreatedFromDbObject() {

		Update update = new Update().set("foo", "bar");
		Update clone = Update.fromDBObject(update.getUpdateObject());

		assertThat(clone.modifies("oof"), is(false));
	}

	/**
	 * @see DATAMONGO-853
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testAddingMultiFieldOperationThrowsExceptionWhenCalledWithNullKey() {
		new Update().addMultiFieldOperation("$op", null, "exprected to throw IllegalArgumentException.");
	}

	/**
	 * @see DATAMONGO-853
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testAddingSingleFieldOperationThrowsExceptionWhenCalledWithNullKey() {
		new Update().addFieldOperation("$op", null, "exprected to throw IllegalArgumentException.");
	}

	/**
	 * @see DATAMONGO-853
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testCreatingUpdateWithNullKeyThrowsException() {
		Update.update(null, "value");
	}

	/**
	 * @see DATAMONGO-953
	 */
	@Test
	public void testEquality() {

		Update actualUpdate = new Update() //
				.inc("size", 1) //
				.set("nl", null) //
				.set("directory", "/Users/Test/Desktop") //
				.push("authors", Collections.singletonMap("name", "Sven")) //
				.pop("authors", Update.Position.FIRST) //
				.set("foo", "bar");

		Update expectedUpdate = new Update() //
				.inc("size", 1) //
				.set("nl", null) //
				.set("directory", "/Users/Test/Desktop") //
				.push("authors", Collections.singletonMap("name", "Sven")) //
				.pop("authors", Update.Position.FIRST) //
				.set("foo", "bar");

		assertThat(actualUpdate, is(equalTo(actualUpdate)));
		assertThat(actualUpdate.hashCode(), is(equalTo(actualUpdate.hashCode())));
		assertThat(actualUpdate, is(equalTo(expectedUpdate)));
		assertThat(actualUpdate.hashCode(), is(equalTo(expectedUpdate.hashCode())));
	}

	/**
	 * @see DATAMONGO-953
	 */
	@Test
	public void testToString() {

		Update actualUpdate = new Update() //
				.inc("size", 1) //
				.set("nl", null) //
				.set("directory", "/Users/Test/Desktop") //
				.push("authors", Collections.singletonMap("name", "Sven")) //
				.pop("authors", Update.Position.FIRST) //
				.set("foo", "bar");

		Update expectedUpdate = new Update() //
				.inc("size", 1) //
				.set("nl", null) //
				.set("directory", "/Users/Test/Desktop") //
				.push("authors", Collections.singletonMap("name", "Sven")) //
				.pop("authors", Update.Position.FIRST) //
				.set("foo", "bar");

		assertThat(actualUpdate.toString(), is(equalTo(expectedUpdate.toString())));
		assertThat(actualUpdate.toString(), is("{ \"$inc\" : { \"size\" : 1} ," //
				+ " \"$set\" : { \"nl\" :  null  , \"directory\" : \"/Users/Test/Desktop\" , \"foo\" : \"bar\"} , " //
				+ "\"$push\" : { \"authors\" : { \"name\" : \"Sven\"}} " //
				+ ", \"$pop\" : { \"authors\" : -1}}")); //
	}

	/**
	 * @see DATAMONGO-944
	 */
	@Test
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForSingleFieldWhenUsingDate() {

		Update update = new Update().currentDate("foo");
		assertThat(update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$currentDate", new BasicDBObject("foo", true)).get()));
	}

	/**
	 * @see DATAMONGO-944
	 */
	@Test
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForMultipleFieldsWhenUsingDate() {

		Update update = new Update().currentDate("foo").currentDate("bar");
		assertThat(update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$currentDate", new BasicDBObject("foo", true).append("bar", true))
						.get()));
	}

	/**
	 * @see DATAMONGO-944
	 */
	@Test
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForSingleFieldWhenUsingTimestamp() {

		Update update = new Update().currentTimestamp("foo");
		assertThat(
				update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$currentDate",
						new BasicDBObject("foo", new BasicDBObject("$type", "timestamp"))).get()));
	}

	/**
	 * @see DATAMONGO-944
	 */
	@Test
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForMultipleFieldsWhenUsingTimestamp() {

		Update update = new Update().currentTimestamp("foo").currentTimestamp("bar");
		assertThat(
				update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add(
						"$currentDate",
						new BasicDBObject("foo", new BasicDBObject("$type", "timestamp")).append("bar", new BasicDBObject("$type",
								"timestamp"))).get()));
	}

	/**
	 * @see DATAMONGO-944
	 */
	@Test
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyWhenUsingMixedDateAndTimestamp() {

		Update update = new Update().currentDate("foo").currentTimestamp("bar");
		assertThat(
				update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$currentDate",
						new BasicDBObject("foo", true).append("bar", new BasicDBObject("$type", "timestamp"))).get()));
	}

	/**
	 * @see DATAMONGO-1002
	 */
	@Test
	public void toStringWorksForUpdateWithComplexObject() {

		Update update = new Update().addToSet("key", new DateTime());
		assertThat(update.toString(), is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-1097
	 */
	@Test(expected = IllegalArgumentException.class)
	public void multiplyShouldThrowExceptionForNullMultiplier() {
		new Update().multiply("key", null);
	}

	/**
	 * @see DATAMONGO-1097
	 */
	@Test
	public void multiplyShouldAddMultiplierAsItsDoubleValue() {

		Update update = new Update().multiply("key", 10);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$mul", new BasicDBObject("key", 10D))
				.get()));
	}

	/**
	 * @see DATAMONGO-1101
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseAnd() {

		Update update = new Update().bitwise("key").and(10L);

		assertThat(update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$bit", new BasicDBObject("key", new BasicDBObject("and", 10L))).get()));
	}

	/**
	 * @see DATAMONGO-1101
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseOr() {

		Update update = new Update().bitwise("key").or(10L);

		assertThat(update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$bit", new BasicDBObject("key", new BasicDBObject("or", 10L))).get()));
	}

	/**
	 * @see DATAMONGO-1101
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseXor() {

		Update update = new Update().bitwise("key").xor(10L);

		assertThat(update.getUpdateObject(),
				equalTo(new BasicDBObjectBuilder().add("$bit", new BasicDBObject("key", new BasicDBObject("xor", 10L))).get()));
	}

	/**
	 * @see DATAMONGO-943
	 */
	@Test(expected = IllegalArgumentException.class)
	public void pushShouldThrowExceptionWhenGivenNegativePosition() {
		new Update().push("foo").atPosition(-1).each("booh");
	}

	/**
	 * @see DATAMONGO-1346
	 */
	@Test
	public void registersMultiplePullAllClauses() {

		Update update = new Update();
		update.pullAll("field1", new String[] { "foo" });
		update.pullAll("field2", new String[] { "bar" });

		DBObject updateObject = update.getUpdateObject();

		DBObject pullAll = DBObjectTestUtils.getAsDBObject(updateObject, "$pullAll");

		assertThat(pullAll.get("field1"), is(notNullValue()));
		assertThat(pullAll.get("field2"), is(notNullValue()));
	}


	/**
	 * @see DATAMONGO-1404
	 */
	@Test(expected = IllegalArgumentException.class)
	public void maxShouldThrowExceptionForNullMultiplier() {
		new Update().max("key", null);
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test(expected = IllegalArgumentException.class)
	public void minShouldThrowExceptionForNullMultiplier() {
		new Update().min("key", null);
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectRepresentationForMax() {

		Update update = new Update().max("key", 10);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$max", new BasicDBObject("key", 10))
				.get()));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectRepresentationForMin() {

		Update update = new Update().min("key", 10);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$min", new BasicDBObject("key", 10))
				.get()));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void shouldSuppressPreviousValueForMax() {

		Update update = new Update().max("key", 10);

		update.max("key", 99);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$max", new BasicDBObject("key", 99))
				.get()));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void shouldSuppressPreviousValueForMin() {

		Update update = new Update().min("key", 10);

		update.max("key", 99);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$min9", new BasicDBObject("key", 99))
				.get()));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectDateRepresentationForMax() {

		final java.util.Date date = new java.util.Date();

		Update update = new Update().max("key", date);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$max", new BasicDBObject("key", date))
				.get()));
	}

	/**
	 * @see DATAMONGO-1404
	 */
	@Test
	public void getUpdateObjectShouldReturnCorrectDateRepresentationForMin() {

		final java.util.Date date = new java.util.Date();

		Update update = new Update().min("key", date);

		assertThat(update.getUpdateObject(), equalTo(new BasicDBObjectBuilder().add("$min", new BasicDBObject("key", date))
				.get()));
	}
}
