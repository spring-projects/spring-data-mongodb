/*
 * Copyright 2010-2017 the original author or authors.
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
import java.util.Date;
import java.util.Map;

import org.bson.Document;
import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.query.Update.Position;

/**
 * Test cases for {@link Update}.
 *
 * @author Oliver Gierke
 * @author Thomas Risberg
 * @author Becca Gaspard
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Alexey Plotnik
 */
public class UpdateTests {

	@Test
	public void testSet() {

		Update u = new Update().set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}")));
	}

	@Test
	public void testSetSet() {

		Update u = new Update().set("directory", "/Users/Test/Desktop").set("size", 0);
		assertThat(u.getUpdateObject(),
				is(Document.parse("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\" , \"size\" : 0}}")));
	}

	@Test
	public void testInc() {

		Update u = new Update().inc("size", 1);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$inc\" : { \"size\" : 1}}")));
	}

	@Test
	public void testIncInc() {

		Update u = new Update().inc("size", 1).inc("count", 1);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$inc\" : { \"size\" : 1 , \"count\" : 1}}")));
	}

	@Test
	public void testIncAndSet() {

		Update u = new Update().inc("size", 1).set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject(),
				is(Document.parse("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}")));
	}

	@Test
	public void testUnset() {

		Update u = new Update().unset("directory");
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$unset\" : { \"directory\" : 1}}")));
	}

	@Test
	public void testPush() {

		Update u = new Update().push("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$push\" : { \"authors\" : { \"name\" : \"Sven\"}}}")));
	}

	@Test
	public void testPushAll() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pushAll("authors", new Object[] { m1, m2 });
		assertThat(u.getUpdateObject(),
				is(Document.parse("{ \"$pushAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}")));
	}

	@Test // DATAMONGO-354
	public void testMultiplePushAllShouldBePossibleWhenUsingDifferentFields() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pushAll("authors", new Object[] { m1, m2 });
		u.pushAll("books", new Object[] { "Spring in Action" });

		assertThat(u.getUpdateObject(), is(Document.parse(
				"{ \"$pushAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}] , \"books\" : [ \"Spring in Action\"]}}")));
	}

	@Test
	public void testAddToSet() {

		Update u = new Update().addToSet("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$addToSet\" : { \"authors\" : { \"name\" : \"Sven\"}}}")));
	}

	@Test
	public void testPop() {

		Update u = new Update().pop("authors", Update.Position.FIRST);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$pop\" : { \"authors\" : -1}}")));

		u = new Update().pop("authors", Update.Position.LAST);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$pop\" : { \"authors\" : 1}}")));
	}

	@Test
	public void testPull() {

		Update u = new Update().pull("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$pull\" : { \"authors\" : { \"name\" : \"Sven\"}}}")));
	}

	@Test
	public void testPullAll() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pullAll("authors", new Object[] { m1, m2 });
		assertThat(u.getUpdateObject(),
				is(Document.parse("{ \"$pullAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}")));
	}

	@Test
	public void testRename() {

		Update u = new Update().rename("directory", "folder");
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$rename\" : { \"directory\" : \"folder\"}}")));
	}

	@Test
	public void testBasicUpdateInc() {

		Update u = new Update().inc("size", 1);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$inc\" : { \"size\" : 1}}")));
	}

	@Test
	public void testBasicUpdateIncAndSet() {

		Update u = new BasicUpdate("{ \"$inc\" : { \"size\" : 1}}").set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject(),
				is(Document.parse("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}")));
	}

	@Test // DATAMONGO-630
	public void testSetOnInsert() {

		Update u = new Update().setOnInsert("size", 1);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$setOnInsert\" : { \"size\" : 1}}")));
	}

	@Test // DATAMONGO-630
	public void testSetOnInsertSetOnInsert() {

		Update u = new Update().setOnInsert("size", 1).setOnInsert("count", 1);
		assertThat(u.getUpdateObject(), is(Document.parse("{ \"$setOnInsert\" : { \"size\" : 1 , \"count\" : 1}}")));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnTrueWhenMultiFieldOperationAddedForField() {

		Update update = new Update().set("foo", "bar");
		assertThat(update.modifies("foo"), is(true));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnFalseWhenMultiFieldOperationAddedForField() {

		Update update = new Update().set("foo", "bar");
		assertThat(update.modifies("oof"), is(false));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnTrueWhenSingleFieldOperationAddedForField() {

		Update update = new Update().pullAll("foo", new Object[] { "bar" });
		assertThat(update.modifies("foo"), is(true));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnFalseWhenSingleFieldOperationAddedForField() {

		Update update = new Update().pullAll("foo", new Object[] { "bar" });
		assertThat(update.modifies("oof"), is(false));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnFalseWhenCalledOnEmptyUpdate() {
		assertThat(new Update().modifies("foo"), is(false));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnTrueWhenUpdateWithKeyCreatedFromDocument() {

		Update update = new Update().set("foo", "bar");
		Update clone = Update.fromDocument(update.getUpdateObject());

		assertThat(clone.modifies("foo"), is(true));
	}

	@Test // DATAMONGO-852
	public void testUpdateAffectsFieldShouldReturnFalseWhenUpdateWithoutKeyCreatedFromDocument() {

		Update update = new Update().set("foo", "bar");
		Update clone = Update.fromDocument(update.getUpdateObject());

		assertThat(clone.modifies("oof"), is(false));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-853
	public void testAddingMultiFieldOperationThrowsExceptionWhenCalledWithNullKey() {
		new Update().addMultiFieldOperation("$op", null, "exprected to throw IllegalArgumentException.");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-853
	public void testAddingSingleFieldOperationThrowsExceptionWhenCalledWithNullKey() {
		new Update().addFieldOperation("$op", null, "exprected to throw IllegalArgumentException.");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-853
	public void testCreatingUpdateWithNullKeyThrowsException() {
		Update.update(null, "value");
	}

	@Test // DATAMONGO-953
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

	@Test // DATAMONGO-953
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
		assertThat(actualUpdate.getUpdateObject(),
				is(Document.parse("{ \"$inc\" : { \"size\" : 1} ," //
						+ " \"$set\" : { \"nl\" :  null  , \"directory\" : \"/Users/Test/Desktop\" , \"foo\" : \"bar\"} , " //
						+ "\"$push\" : { \"authors\" : { \"name\" : \"Sven\"}} " //
						+ ", \"$pop\" : { \"authors\" : -1}}"))); //
	}

	@Test // DATAMONGO-944
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForSingleFieldWhenUsingDate() {

		Update update = new Update().currentDate("foo");
		assertThat(update.getUpdateObject(), equalTo(new Document().append("$currentDate", new Document("foo", true))));
	}

	@Test // DATAMONGO-944
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForMultipleFieldsWhenUsingDate() {

		Update update = new Update().currentDate("foo").currentDate("bar");
		assertThat(update.getUpdateObject(),
				equalTo(new Document().append("$currentDate", new Document("foo", true).append("bar", true))));
	}

	@Test // DATAMONGO-944
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForSingleFieldWhenUsingTimestamp() {

		Update update = new Update().currentTimestamp("foo");
		assertThat(update.getUpdateObject(),
				equalTo(new Document().append("$currentDate", new Document("foo", new Document("$type", "timestamp")))));
	}

	@Test // DATAMONGO-944
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyForMultipleFieldsWhenUsingTimestamp() {

		Update update = new Update().currentTimestamp("foo").currentTimestamp("bar");
		assertThat(update.getUpdateObject(), equalTo(new Document().append("$currentDate",
				new Document("foo", new Document("$type", "timestamp")).append("bar", new Document("$type", "timestamp")))));
	}

	@Test // DATAMONGO-944
	public void getUpdateObjectShouldReturnCurrentDateCorrectlyWhenUsingMixedDateAndTimestamp() {

		Update update = new Update().currentDate("foo").currentTimestamp("bar");
		assertThat(update.getUpdateObject(), equalTo(new Document().append("$currentDate",
				new Document("foo", true).append("bar", new Document("$type", "timestamp")))));
	}

	@Test // DATAMONGO-1002
	public void toStringWorksForUpdateWithComplexObject() {

		Update update = new Update().addToSet("key", new DateTime());
		assertThat(update.toString(), is(notNullValue()));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1097
	public void multiplyShouldThrowExceptionForNullMultiplier() {
		new Update().multiply("key", null);
	}

	@Test // DATAMONGO-1097
	public void multiplyShouldAddMultiplierAsItsDoubleValue() {

		Update update = new Update().multiply("key", 10);

		assertThat(update.getUpdateObject(), equalTo(new Document().append("$mul", new Document("key", 10D))));
	}

	@Test // DATAMONGO-1101
	public void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseAnd() {

		Update update = new Update().bitwise("key").and(10L);

		assertThat(update.getUpdateObject(),
				equalTo(new Document().append("$bit", new Document("key", new Document("and", 10L)))));
	}

	@Test // DATAMONGO-1101
	public void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseOr() {

		Update update = new Update().bitwise("key").or(10L);

		assertThat(update.getUpdateObject(),
				equalTo(new Document().append("$bit", new Document("key", new Document("or", 10L)))));
	}

	@Test // DATAMONGO-1101
	public void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseXor() {

		Update update = new Update().bitwise("key").xor(10L);

		assertThat(update.getUpdateObject(),
				equalTo(new Document().append("$bit", new Document("key", new Document("xor", 10L)))));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-943
	public void pushShouldThrowExceptionWhenGivenNegativePosition() {
		new Update().push("foo").atPosition(-1).each("booh");
	}

	@Test // DATAMONGO-1346
	public void registersMultiplePullAllClauses() {

		Update update = new Update();
		update.pullAll("field1", new String[] { "foo" });
		update.pullAll("field2", new String[] { "bar" });

		Document updateObject = update.getUpdateObject();

		Document pullAll = DocumentTestUtils.getAsDocument(updateObject, "$pullAll");

		assertThat(pullAll.get("field1"), is(notNullValue()));
		assertThat(pullAll.get("field2"), is(notNullValue()));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1404
	public void maxShouldThrowExceptionForNullMultiplier() {
		new Update().max("key", null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1404
	public void minShouldThrowExceptionForNullMultiplier() {
		new Update().min("key", null);
	}

	@Test // DATAMONGO-1404
	public void getUpdateObjectShouldReturnCorrectRepresentationForMax() {

		Update update = new Update().max("key", 10);

		assertThat(update.getUpdateObject(), equalTo(new Document("$max", new Document("key", 10))));
	}

	@Test // DATAMONGO-1404
	public void getUpdateObjectShouldReturnCorrectRepresentationForMin() {

		Update update = new Update().min("key", 10);

		assertThat(update.getUpdateObject(), equalTo(new Document("$min", new Document("key", 10))));
	}

	@Test // DATAMONGO-1404
	public void shouldSuppressPreviousValueForMax() {

		Update update = new Update().max("key", 10);
		update.max("key", 99);

		assertThat(update.getUpdateObject(), equalTo(new Document("$max", new Document("key", 99))));
	}

	@Test // DATAMONGO-1404
	public void shouldSuppressPreviousValueForMin() {

		Update update = new Update().min("key", 10);
		update.min("key", 99);

		assertThat(update.getUpdateObject(), equalTo(new Document("$min", new Document("key", 99))));
	}

	@Test // DATAMONGO-1404
	public void getUpdateObjectShouldReturnCorrectDateRepresentationForMax() {

		Date date = new Date();
		Update update = new Update().max("key", date);

		assertThat(update.getUpdateObject(), equalTo(new Document("$max", new Document("key", date))));
	}

	@Test // DATAMONGO-1404
	public void getUpdateObjectShouldReturnCorrectDateRepresentationForMin() {

		Date date = new Date();
		Update update = new Update().min("key", date);

		assertThat(update.getUpdateObject(), equalTo(new Document("$min", new Document("key", date))));
	}

	@Test // DATAMONGO-1777
	public void toStringShouldPrettyPrintModifiers() {

		assertThat(new Update().push("key").atPosition(Position.FIRST).value("Arya").toString(), is(equalTo(
				"{ \"$push\" : { \"key\" : { \"$java\" : { \"$position\" : { \"$java\" : { \"$position\" : 0} }, \"$each\" : { \"$java\" : { \"$each\" : [ \"Arya\"]} } } } } }")));
	}

	@Test // DATAMONGO-1777
	public void toStringConsidersIsolated() {

		assertThat(new Update().set("key", "value").isolated().toString(),
				is(equalTo("{ \"$set\" : { \"key\" : \"value\" }, \"$isolated\" : 1 }")));
	}
}
