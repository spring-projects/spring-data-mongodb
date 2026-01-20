/*
 * Copyright 2010-present the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;
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
 * @author Mark Paluch
 */
class UpdateTests {

	@Test
	void testSet() {

		Update u = new Update().set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject())
				.isEqualTo(Document.parse("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	@Test // GH-5135
	void testSetViaPath() {

		Update u = new Update().set(TestFile::getDirectory, "/Users/Test/Desktop");
		assertThat(u.getUpdateObject())
				.isEqualTo(Document.parse("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	@Test
	void testSetSet() {

		Update u = new Update().set("directory", "/Users/Test/Desktop").set("size", 0);
		assertThat(u.getUpdateObject())
				.isEqualTo((Document.parse("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\" , \"size\" : 0}}")));
	}

	@Test
	void testInc() {

		Update u = new Update().inc("size", 1);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$inc\" : { \"size\" : 1}}"));
	}

	@Test // GH-5135
	void testIncViaPath() {

		Update u = new Update().inc(TestFile::getSize, 1);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$inc\" : { \"size\" : 1}}"));
	}

	@Test // GH-5135
	void testIncByOneViaPath() {

		Update u = new Update().inc(TestFile::getSize);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$inc\" : { \"size\" : NumberLong(1)}}"));
	}

	@Test
	void testIncInc() {

		Update u = new Update().inc("size").inc("count", 1);
		assertThat(u.getUpdateObject())
				.isEqualTo(Document.parse("{ \"$inc\" : { \"size\" : NumberLong(1) , \"count\" : 1}}"));
	}

	@Test
	void testIncAndSet() {

		Update u = new Update().inc("size", 1).set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject()).isEqualTo(
				Document.parse("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	@Test
	void testUnset() {

		Update u = new Update().unset("directory");
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$unset\" : { \"directory\" : 1}}"));
	}

	@Test // GH-5135
	void testUnsetViaPath() {

		Update u = new Update().unset(TestFile::getDirectory);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$unset\" : { \"directory\" : 1}}"));
	}

	@Test
	void testPush() {

		Update u = new Update().push("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$push\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test // GH-5135
	void testPushViaPath() {

		Update u = new Update().push(TestFile::getAuthors, Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$push\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test
	void testAddToSet() {

		Update u = new Update().addToSet("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject())
				.isEqualTo(Document.parse("{ \"$addToSet\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test // GH-5135
	void testAddToSetViaPath() {

		Update u = new Update().addToSet(TestFile::getAuthors, Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject())
				.isEqualTo(Document.parse("{ \"$addToSet\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test
	void testPop() {

		Update u = new Update().pop("authors", Update.Position.FIRST);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$pop\" : { \"authors\" : -1}}"));

		u = new Update().pop("authors", Update.Position.LAST);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$pop\" : { \"authors\" : 1}}"));
	}

	@Test // GH-5135
	void testPopViaPath() {

		Update u = new Update().pop(TestFile::getAuthors, Update.Position.FIRST);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$pop\" : { \"authors\" : -1}}"));

		u = new Update().pop(TestFile::getAuthors, Update.Position.LAST);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$pop\" : { \"authors\" : 1}}"));
	}

	@Test
	void testPull() {

		Update u = new Update().pull("authors", Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$pull\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test // GH-5135
	void testPullViaPath() {

		Update u = new Update().pull(TestFile::getAuthors, Collections.singletonMap("name", "Sven"));
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$pull\" : { \"authors\" : { \"name\" : \"Sven\"}}}"));
	}

	@Test
	void testPullAll() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pullAll("authors", new Object[] { m1, m2 });
		assertThat(u.getUpdateObject()).isEqualTo(
				Document.parse("{ \"$pullAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}"));
	}

	@Test // GH-5135
	void testPullAllViaPath() {

		Map<String, String> m1 = Collections.singletonMap("name", "Sven");
		Map<String, String> m2 = Collections.singletonMap("name", "Maria");

		Update u = new Update().pullAll(TestFile::getAuthors, new Object[] { m1, m2 });
		assertThat(u.getUpdateObject()).isEqualTo(
				Document.parse("{ \"$pullAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}"));
	}

	@Test
	void testRename() {

		Update u = new Update().rename("directory", "folder");
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$rename\" : { \"directory\" : \"folder\"}}"));
	}

	@Test
	void testBasicUpdateInc() {

		Update u = new Update().inc("size", 1);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$inc\" : { \"size\" : 1}}"));
	}

	@Test
	void testBasicUpdateIncAndSet() {

		Update u = new BasicUpdate("{ \"$inc\" : { \"size\" : 1}}").set("directory", "/Users/Test/Desktop");
		assertThat(u.getUpdateObject()).isEqualTo(
				Document.parse("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}"));
	}

	@Test // DATAMONGO-630
	void testSetOnInsert() {

		Update u = new Update().setOnInsert("size", 1);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$setOnInsert\" : { \"size\" : 1}}"));
	}

	@Test // GH-5135
	void testSetOnInsertViaPath() {

		Update u = new Update().setOnInsert(TestFile::getSize, 1);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$setOnInsert\" : { \"size\" : 1}}"));
	}

	@Test // DATAMONGO-630
	void testSetOnInsertSetOnInsert() {

		Update u = new Update().setOnInsert("size", 1).setOnInsert("count", 1);
		assertThat(u.getUpdateObject()).isEqualTo(Document.parse("{ \"$setOnInsert\" : { \"size\" : 1 , \"count\" : 1}}"));
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnTrueWhenMultiFieldOperationAddedForField() {

		Update update = new Update().set("foo", "bar");
		assertThat(update.modifies("foo")).isTrue();
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnFalseWhenMultiFieldOperationAddedForField() {

		Update update = new Update().set("foo", "bar");
		assertThat(update.modifies("oof")).isFalse();
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnTrueWhenSingleFieldOperationAddedForField() {

		Update update = new Update().pullAll("foo", new Object[] { "bar" });
		assertThat(update.modifies("foo")).isTrue();
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnFalseWhenSingleFieldOperationAddedForField() {

		Update update = new Update().pullAll("foo", new Object[] { "bar" });
		assertThat(update.modifies("oof")).isFalse();
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnFalseWhenCalledOnEmptyUpdate() {
		assertThat(new Update().modifies("foo")).isFalse();
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnTrueWhenUpdateWithKeyCreatedFromDocument() {

		Update update = new Update().set("foo", "bar");
		Update clone = Update.fromDocument(update.getUpdateObject());

		assertThat(clone.modifies("foo")).isTrue();
	}

	@Test // DATAMONGO-852
	void testUpdateAffectsFieldShouldReturnFalseWhenUpdateWithoutKeyCreatedFromDocument() {

		Update update = new Update().set("foo", "bar");
		Update clone = Update.fromDocument(update.getUpdateObject());

		assertThat(clone.modifies("oof")).isFalse();
	}

	@Test // DATAMONGO-853
	void testAddingMultiFieldOperationThrowsExceptionWhenCalledWithNullKey() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> new Update().addMultiFieldOperation("$op", null, "exprected to throw IllegalArgumentException."));
	}

	@Test // DATAMONGO-853
	void testAddingSingleFieldOperationThrowsExceptionWhenCalledWithNullKey() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> new Update().addMultiFieldOperation("$op", null, "exprected to throw IllegalArgumentException."));
	}

	@Test // DATAMONGO-853
	void testCreatingUpdateWithNullKeyThrowsException() {
		assertThatIllegalArgumentException().isThrownBy(() -> Update.update((String) null, "value"));
	}

	@Test // DATAMONGO-953
	void testEquality() {
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

		assertThat(actualUpdate).isEqualTo(actualUpdate);
		assertThat(actualUpdate.hashCode()).isEqualTo(actualUpdate.hashCode());
		assertThat(actualUpdate).isEqualTo(expectedUpdate);
		assertThat(actualUpdate.hashCode()).isEqualTo(expectedUpdate.hashCode());
	}

	@Test // DATAMONGO-953
	void testToString() {

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

		assertThat(actualUpdate.toString()).isEqualTo(expectedUpdate.toString());
		assertThat(actualUpdate.getUpdateObject()).isEqualTo(Document.parse("{ \"$inc\" : { \"size\" : 1} ," //
				+ " \"$set\" : { \"nl\" :  null  , \"directory\" : \"/Users/Test/Desktop\" , \"foo\" : \"bar\"} , " //
				+ "\"$push\" : { \"authors\" : { \"name\" : \"Sven\"}} " //
				+ ", \"$pop\" : { \"authors\" : -1}}")); //
	}

	@Test // DATAMONGO-944
	void getUpdateObjectShouldReturnCurrentDateCorrectlyForSingleFieldWhenUsingDate() {

		Update update = new Update().currentDate("foo");
		assertThat(update.getUpdateObject()).isEqualTo(new Document().append("$currentDate", new Document("foo", true)));
	}

	@Test // DATAMONGO-944
	void getUpdateObjectShouldReturnCurrentDateCorrectlyForMultipleFieldsWhenUsingDate() {

		Update update = new Update().currentDate("foo").currentDate("bar");
		assertThat(update.getUpdateObject())
				.isEqualTo(new Document().append("$currentDate", new Document("foo", true).append("bar", true)));
	}

	@Test // DATAMONGO-944
	void getUpdateObjectShouldReturnCurrentDateCorrectlyForSingleFieldWhenUsingTimestamp() {

		Update update = new Update().currentTimestamp("foo");
		assertThat(update.getUpdateObject())
				.isEqualTo(new Document().append("$currentDate", new Document("foo", new Document("$type", "timestamp"))));
	}

	@Test // DATAMONGO-944
	void getUpdateObjectShouldReturnCurrentDateCorrectlyForMultipleFieldsWhenUsingTimestamp() {

		Update update = new Update().currentTimestamp("foo").currentTimestamp("bar");
		assertThat(update.getUpdateObject()).isEqualTo(new Document().append("$currentDate",
				new Document("foo", new Document("$type", "timestamp")).append("bar", new Document("$type", "timestamp"))));
	}

	@Test // DATAMONGO-944
	void getUpdateObjectShouldReturnCurrentDateCorrectlyWhenUsingMixedDateAndTimestamp() {

		Update update = new Update().currentDate("foo").currentTimestamp("bar");
		assertThat(update.getUpdateObject()).isEqualTo(new Document().append("$currentDate",
				new Document("foo", true).append("bar", new Document("$type", "timestamp"))));
	}

	@Test // DATAMONGO-1002
	void toStringWorksForUpdateWithComplexObject() {

		Update update = new Update().addToSet("key", new Date());
		assertThat(update.toString()).isNotNull();
	}

	@Test // DATAMONGO-1097
	void multiplyShouldThrowExceptionForNullMultiplier() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Update().multiply("key", null));
	}

	@Test // DATAMONGO-1097
	void multiplyShouldAddMultiplierAsItsDoubleValue() {

		Update update = new Update().multiply("key", 10);

		assertThat(update.getUpdateObject()).isEqualTo(new Document().append("$mul", new Document("key", 10D)));
	}

	@Test // GH-5135
	void multiplyShouldAddMultiplierAsItsDoubleValueViaPath() {

		Update update = new Update().multiply(TestFile::getKey, 10);

		assertThat(update.getUpdateObject()).isEqualTo(new Document().append("$mul", new Document("key", 10D)));
	}

	@Test // DATAMONGO-1101
	void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseAnd() {

		Update update = new Update().bitwise("key").and(10L);

		assertThat(update.getUpdateObject())
				.isEqualTo(new Document().append("$bit", new Document("key", new Document("and", 10L))));
	}

	@Test // GH-5135
	void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseAndViaPath() {

		Update update = new Update().bitwise(TestFile::getKey).and(10L);

		assertThat(update.getUpdateObject())
				.isEqualTo(new Document().append("$bit", new Document("key", new Document("and", 10L))));
	}

	@Test // DATAMONGO-1101
	void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseOr() {

		Update update = new Update().bitwise("key").or(10L);

		assertThat(update.getUpdateObject())
				.isEqualTo(new Document().append("$bit", new Document("key", new Document("or", 10L))));
	}

	@Test // DATAMONGO-1101
	void getUpdateObjectShouldReturnCorrectRepresentationForBitwiseXor() {

		Update update = new Update().bitwise("key").xor(10L);

		assertThat(update.getUpdateObject())
				.isEqualTo(new Document().append("$bit", new Document("key", new Document("xor", 10L))));
	}

	@Test // DATAMONGO-1346
	void registersMultiplePullAllClauses() {

		Update update = new Update();
		update.pullAll("field1", new String[] { "foo" });
		update.pullAll("field2", new String[] { "bar" });

		Document updateObject = update.getUpdateObject();

		Document pullAll = DocumentTestUtils.getAsDocument(updateObject, "$pullAll");

		assertThat(pullAll.get("field1")).isNotNull();
		assertThat(pullAll.get("field2")).isNotNull();
	}

	@Test // DATAMONGO-1404
	void maxShouldThrowExceptionForNullMultiplier() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Update().max("key", null));
	}

	@Test // DATAMONGO-1404
	void minShouldThrowExceptionForNullMultiplier() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Update().min("key", null));
	}

	@Test // DATAMONGO-1404
	void getUpdateObjectShouldReturnCorrectRepresentationForMax() {

		Update update = new Update().max("key", 10);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$max", new Document("key", 10)));
	}

	@Test // GH-5135
	void getUpdateObjectShouldReturnCorrectRepresentationForMaxViaPath() {

		Update update = new Update().max(TestFile::getKey, 10);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$max", new Document("key", 10)));
	}

	@Test // DATAMONGO-1404
	void getUpdateObjectShouldReturnCorrectRepresentationForMin() {

		Update update = new Update().min("key", 10);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$min", new Document("key", 10)));
	}

	@Test // GH-5135
	void getUpdateObjectShouldReturnCorrectRepresentationForMinViaPath() {

		Update update = new Update().min(TestFile::getKey, 10);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$min", new Document("key", 10)));
	}

	@Test // DATAMONGO-1404
	void shouldSuppressPreviousValueForMax() {

		Update update = new Update().max("key", 10);
		update.max("key", 99);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$max", new Document("key", 99)));
	}

	@Test // DATAMONGO-1404
	void shouldSuppressPreviousValueForMin() {

		Update update = new Update().min("key", 10);
		update.min("key", 99);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$min", new Document("key", 99)));
	}

	@Test // DATAMONGO-1404
	void getUpdateObjectShouldReturnCorrectDateRepresentationForMax() {

		Date date = new Date();
		Update update = new Update().max("key", date);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$max", new Document("key", date)));
	}

	@Test // DATAMONGO-1404
	void getUpdateObjectShouldReturnCorrectDateRepresentationForMin() {

		Date date = new Date();
		Update update = new Update().min("key", date);

		assertThat(update.getUpdateObject()).isEqualTo(new Document("$min", new Document("key", date)));
	}

	@Test // DATAMONGO-1777, DATAMONGO-2199
	void toStringShouldPrettyPrintModifiers() {

		assertThat(new Update().push("key").atPosition(Position.FIRST).value("Arya").toString()).isEqualTo(
				"{ \"$push\" : { \"key\" : { \"$java\" : { \"$position\" : { \"$java\" : { \"$position\" : 0} }, \"$each\" : { \"$java\" : { \"$each\" : [ \"Arya\" ] } } } } } }");
	}

	@Test // DATAMONGO-1777, DATAMONGO-2198
	void toStringConsidersIsolated() {

		assertThat(new Update().set("key", "value").isolated().toString()).contains("\"$isolated\"");
	}

	@Test // DATAMONGO-1778
	void equalsShouldConsiderModifiers() {

		Update update1 = new Update().inc("version", 1).push("someField").slice(-10).each("test");
		Update update2 = new Update().inc("version", 1).push("someField").slice(-10).each("test");
		Update update3 = new Update().inc("version", 1).push("someField").slice(10).each("test");

		assertThat(update1).isEqualTo(update2);
		assertThat(update1).isNotEqualTo(update3);
	}

	@Test // DATAMONGO-1778
	void equalsShouldConsiderIsolated() {

		Update update1 = new Update().inc("version", 1).isolated();
		Update update2 = new Update().inc("version", 1).isolated();

		assertThat(update1).isEqualTo(update2);
	}

	@Test // DATAMONGO-1778
	void hashCodeShouldConsiderModifiers() {

		Update update1 = new Update().inc("version", 1).push("someField").slice(-10).each("test");
		Update update2 = new Update().inc("version", 1).push("someField").slice(-10).each("test");
		Update update3 = new Update().inc("version", 1).push("someField").slice(10).each("test");

		assertThat(update1.hashCode()).isEqualTo(update2.hashCode());
		assertThat(update1.hashCode()).isNotEqualTo(update3.hashCode());
	}

	@Test // DATAMONGO-1778
	void hashCodeShouldConsiderIsolated() {

		Update update1 = new Update().inc("version", 1).isolated();
		Update update2 = new Update().inc("version", 1).isolated();
		Update update3 = new Update().inc("version", 1);

		assertThat(update1.hashCode()).isEqualTo(update2.hashCode());
		assertThat(update1.hashCode()).isNotEqualTo(update3.hashCode());
	}

	static class TestFile {

		String directory;
		String fileName;
		int version;
		long size;
		List<String> authors;
		List<String> key;

		public String getDirectory() {
			return directory;
		}

		public String getFileName() {
			return fileName;
		}

		public int getVersion() {
			return version;
		}

		public List<String> getKey() {
			return key;
		}

		public List<String> getAuthors() {
			return authors;
		}

		public long getSize() {
			return size;
		}
	}
}
