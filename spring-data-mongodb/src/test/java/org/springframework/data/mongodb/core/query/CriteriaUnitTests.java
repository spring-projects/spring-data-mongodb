/*
 * Copyright 2010-2025 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.geo.GeoJsonLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;

/**
 * Unit tests for {@link Criteria}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Andreas Zink
 * @author Ziemowit Stolarczyk
 * @author Clément Petit
 * @author Mark Paluch
 * @author James McNee
 */
class CriteriaUnitTests {

	@Test
	void testSimpleCriteria() {
		Criteria c = new Criteria("name").is("Bubba");
		assertThat(c.getCriteriaObject()).isEqualTo("{ \"name\" : \"Bubba\"}");
	}

	@Test // GH-4850
	void testCombiningSimpleCriteria() {

		Document expected = Document.parse("{ name : { $eq : 123, $type : ['long'] } }");

		Criteria c = Criteria.where("name") //
				.is(123) //
				.type(Type.INT_64);

		assertThat(c.getCriteriaObject()).isEqualTo(expected);

		c = Criteria.where("name") //
				.type(Type.INT_64).is(123);

		assertThat(c.getCriteriaObject()).isEqualTo(expected);
	}

	@Test // GH-4850
	void testCombiningBsonRegexCriteria() {

		Criteria c = Criteria.where("name").regex(new BsonRegularExpression("^spring$")).type(Type.INT_64);

		assertThat(c.getCriteriaObject())
				.isEqualTo(Document.parse("{ name : { $regex : RegExp('^spring$'), $type : ['long'] } }"));
	}

	@Test // GH-4850
	void testCombiningRegexCriteria() {

		Criteria c = Criteria.where("name").regex("^spring$").type(Type.INT_64);

		assertThat(c.getCriteriaObject()).hasEntrySatisfying("name.$regex",
				it -> assertThat(it).isInstanceOf(Pattern.class));
	}

	@Test
	void testNotEqualCriteria() {
		Criteria c = new Criteria("name").ne("Bubba");
		assertThat(c.getCriteriaObject()).isEqualTo("{ \"name\" : { \"$ne\" : \"Bubba\"}}");
	}

	@Test
	void buildsIsNullCriteriaCorrectly() {

		Document reference = new Document("name", null);

		Criteria criteria = new Criteria("name").is(null);
		assertThat(criteria.getCriteriaObject()).isEqualTo(reference);
	}

	@Test
	void testChainedCriteria() {
		Criteria c = new Criteria("name").is("Bubba").and("age").lt(21);
		assertThat(c.getCriteriaObject()).isEqualTo("{ \"name\" : \"Bubba\" , \"age\" : { \"$lt\" : 21}}");
	}

	@Test
	void testCriteriaWithMultipleConditionsForSameKey() {
		Criteria c = new Criteria("name").gte("M").and("name").ne("A");

		assertThatExceptionOfType(InvalidMongoDbApiUsageException.class).isThrownBy(c::getCriteriaObject);
	}

	@Test
	void equalIfCriteriaMatches() {

		Criteria left = new Criteria("name").is("Foo").and("lastname").is("Bar");
		Criteria right = new Criteria("name").is("Bar").and("lastname").is("Bar");

		assertThat(left).isNotEqualTo(right);
		assertThat(right).isNotEqualTo(left);
	}

	@Test // GH-3286
	void shouldBuildCorrectAndOperator() {

		Collection<Criteria> operatorCriteria = Arrays.asList(Criteria.where("x").is(true), Criteria.where("y").is(42),
				Criteria.where("z").is("value"));

		Criteria criteria = Criteria.where("foo").is("bar").andOperator(operatorCriteria);

		assertThat(criteria.getCriteriaObject())
				.isEqualTo("{\"$and\":[{\"x\":true}, {\"y\":42}, {\"z\":\"value\"}], \"foo\":\"bar\"}");
	}

	@Test // GH-3286
	void shouldBuildCorrectOrOperator() {

		Collection<Criteria> operatorCriteria = Arrays.asList(Criteria.where("x").is(true), Criteria.where("y").is(42),
				Criteria.where("z").is("value"));

		Criteria criteria = Criteria.where("foo").is("bar").orOperator(operatorCriteria);

		assertThat(criteria.getCriteriaObject())
				.isEqualTo("{\"$or\":[{\"x\":true}, {\"y\":42}, {\"z\":\"value\"}], \"foo\":\"bar\"}");
	}

	@Test // GH-3286
	void shouldBuildCorrectNorOperator() {

		Collection<Criteria> operatorCriteria = Arrays.asList(Criteria.where("x").is(true), Criteria.where("y").is(42),
				Criteria.where("z").is("value"));

		Criteria criteria = Criteria.where("foo").is("bar").norOperator(operatorCriteria);

		assertThat(criteria.getCriteriaObject())
				.isEqualTo("{\"$nor\":[{\"x\":true}, {\"y\":42}, {\"z\":\"value\"}], \"foo\":\"bar\"}");
	}

	@Test // DATAMONGO-507
	void shouldThrowExceptionWhenTryingToNegateAndOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Criteria() //
				.not() //
				.andOperator(Criteria.where("delete").is(true).and("_id").is(42)));
	}

	@Test // DATAMONGO-507
	void shouldThrowExceptionWhenTryingToNegateOrOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Criteria() //
				.not() //
				.orOperator(Criteria.where("delete").is(true).and("_id").is(42)));
	}

	@Test // DATAMONGO-507
	void shouldThrowExceptionWhenTryingToNegateNorOperation() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Criteria() //
				.not() //
				.norOperator(Criteria.where("delete").is(true).and("_id").is(42)));
	}

	@Test // DATAMONGO-507
	void shouldNegateFollowingSimpleExpression() {

		Criteria c = Criteria.where("age").not().gt(18).and("status").is("student");
		Document co = c.getCriteriaObject();

		assertThat(co).isNotNull();
		assertThat(co).isEqualTo("{ \"age\" : { \"$not\" : { \"$gt\" : 18}} , \"status\" : \"student\"}");
	}

	@Test // GH-3726
	void shouldBuildCorrectSampleRateOperation() {
		Criteria c = new Criteria().sampleRate(0.4);
		assertThat(c.getCriteriaObject()).isEqualTo("{ \"$sampleRate\" : 0.4 }");
	}

	@Test // GH-3726
	void shouldThrowExceptionWhenSampleRateIsNegative() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Criteria().sampleRate(-1));
	}

	@Test // GH-3726
	void shouldThrowExceptionWhenSampleRateIsGreatedThanOne() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Criteria().sampleRate(1.01));
	}

	@Test // DATAMONGO-1068
	void getCriteriaObjectShouldReturnEmptyDocumentWhenNoCriteriaSpecified() {

		Document document = new Criteria().getCriteriaObject();

		assertThat(document).isEqualTo(new Document());
	}

	@Test // DATAMONGO-1068
	void getCriteriaObjectShouldUseCritieraValuesWhenNoKeyIsPresent() {

		Document document = new Criteria().lt("foo").getCriteriaObject();

		assertThat(document).isEqualTo(new Document().append("$lt", "foo"));
	}

	@Test // DATAMONGO-1068
	void getCriteriaObjectShouldUseCritieraValuesWhenNoKeyIsPresentButMultipleCriteriasPresent() {

		Document document = new Criteria().lt("foo").gt("bar").getCriteriaObject();

		assertThat(document).isEqualTo(new Document().append("$lt", "foo").append("$gt", "bar"));
	}

	@Test // DATAMONGO-1068
	void getCriteriaObjectShouldRespectNotWhenNoKeyPresent() {

		Document document = new Criteria().lt("foo").not().getCriteriaObject();

		assertThat(document).isEqualTo(new Document().append("$not", new Document("$lt", "foo")));
	}

	@Test // GH-4220
	void usesCorrectBsonType() {

		Document document = new Criteria("foo").type(Type.BOOLEAN).getCriteriaObject();

		assertThat(document).containsEntry("foo.$type", Collections.singletonList("bool"));
	}

	@Test // DATAMONGO-1135
	void geoJsonTypesShouldBeWrappedInGeometry() {

		Document document = new Criteria("foo").near(new GeoJsonPoint(100, 200)).getCriteriaObject();

		assertThat(document).containsEntry("foo.$near.$geometry", new GeoJsonPoint(100, 200));
	}

	@Test // DATAMONGO-1135
	void legacyCoordinateTypesShouldNotBeWrappedInGeometry() {

		Document document = new Criteria("foo").near(new Point(100, 200)).getCriteriaObject();

		assertThat(document).doesNotContainKey("foo.$near.$geometry");
	}

	@Test // DATAMONGO-1135
	void maxDistanceShouldBeMappedInsideNearWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").near(new GeoJsonPoint(100, 200)).maxDistance(50D).getCriteriaObject();

		assertThat(document).containsEntry("foo.$near.$maxDistance", 50D);
	}

	@Test // DATAMONGO-1135
	void maxDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).maxDistance(50D).getCriteriaObject();

		assertThat(document).containsEntry("foo.$nearSphere.$maxDistance", 50D);
	}

	@Test // DATAMONGO-1110
	void minDistanceShouldBeMappedInsideNearWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").near(new GeoJsonPoint(100, 200)).minDistance(50D).getCriteriaObject();

		assertThat(document).containsEntry("foo.$near.$minDistance", 50D);
	}

	@Test // DATAMONGO-1110
	void minDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).minDistance(50D).getCriteriaObject();

		assertThat(document).containsEntry("foo.$nearSphere.$minDistance", 50D);
	}

	@Test // DATAMONGO-1110
	void minAndMaxDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).minDistance(50D).maxDistance(100D)
				.getCriteriaObject();

		assertThat(document).containsEntry("foo.$nearSphere.$minDistance", 50D);
		assertThat(document).containsEntry("foo.$nearSphere.$maxDistance", 100D);
	}

	@Test // DATAMONGO-1134
	void intersectsShouldThrowExceptionWhenCalledWihtNullValue() {
		assertThatIllegalArgumentException().isThrownBy(() -> new Criteria("foo").intersects(null));
	}

	@Test // DATAMONGO-1134
	void intersectsShouldWrapGeoJsonTypeInGeometryCorrectly() {

		GeoJsonLineString lineString = new GeoJsonLineString(new Point(0, 0), new Point(10, 10));
		Document document = new Criteria("foo").intersects(lineString).getCriteriaObject();

		assertThat(document).containsEntry("foo.$geoIntersects.$geometry", lineString);
	}

	@Test // DATAMONGO-1835
	void extractsJsonSchemaInChainCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("name").build();
		Criteria criteria = Criteria.where("foo").is("bar").andDocumentStructureMatches(schema);

		assertThat(criteria.getCriteriaObject()).isEqualTo(new Document("foo", "bar").append("$jsonSchema",
				new Document("type", "object").append("required", Collections.singletonList("name"))));
	}

	@Test // DATAMONGO-1835
	void extractsJsonSchemaFromFactoryMethodCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("name").build();
		Criteria criteria = Criteria.matchingDocumentStructure(schema);

		assertThat(criteria.getCriteriaObject()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("required", Collections.singletonList("name"))));
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAllClearWithIntBitmaskCorrectly() {

		Criteria numericBitmaskCriteria = new Criteria("field").bits().allClear(0b101);

		assertThat(numericBitmaskCriteria.getCriteriaObject()).isEqualTo("{ \"field\" : { \"$bitsAllClear\" : 5} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAllClearWithPositionListCorrectly() {

		Criteria bitPositionsBitmaskCriteria = new Criteria("field").bits().allClear(Arrays.asList(0, 2));

		assertThat(bitPositionsBitmaskCriteria.getCriteriaObject())
				.isEqualTo("{ \"field\" : { \"$bitsAllClear\" : [ 0, 2 ]} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAllSetWithIntBitmaskCorrectly() {

		Criteria numericBitmaskCriteria = new Criteria("field").bits().allSet(0b101);

		assertThat(numericBitmaskCriteria.getCriteriaObject()).isEqualTo("{ \"field\" : { \"$bitsAllSet\" : 5} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAllSetWithPositionListCorrectly() {

		Criteria bitPositionsBitmaskCriteria = new Criteria("field").bits().allSet(Arrays.asList(0, 2));

		assertThat(bitPositionsBitmaskCriteria.getCriteriaObject())
				.isEqualTo("{ \"field\" : { \"$bitsAllSet\" : [ 0, 2 ]} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAnyClearWithIntBitmaskCorrectly() {

		Criteria numericBitmaskCriteria = new Criteria("field").bits().anyClear(0b101);

		assertThat(numericBitmaskCriteria.getCriteriaObject()).isEqualTo("{ \"field\" : { \"$bitsAnyClear\" : 5} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAnyClearWithPositionListCorrectly() {

		Criteria bitPositionsBitmaskCriteria = new Criteria("field").bits().anyClear(Arrays.asList(0, 2));

		assertThat(bitPositionsBitmaskCriteria.getCriteriaObject())
				.isEqualTo("{ \"field\" : { \"$bitsAnyClear\" : [ 0, 2 ]} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAnySetWithIntBitmaskCorrectly() {

		Criteria numericBitmaskCriteria = new Criteria("field").bits().anySet(0b101);

		assertThat(numericBitmaskCriteria.getCriteriaObject()).isEqualTo("{ \"field\" : { \"$bitsAnySet\" : 5} }");
	}

	@Test // DATAMONGO-1808
	void shouldAppendBitsAnySetWithPositionListCorrectly() {

		Criteria bitPositionsBitmaskCriteria = new Criteria("field").bits().anySet(Arrays.asList(0, 2));

		assertThat(bitPositionsBitmaskCriteria.getCriteriaObject())
				.isEqualTo("{ \"field\" : { \"$bitsAnySet\" : [ 0, 2 ]} }");
	}

	@Test // DATAMONGO-2002
	void shouldEqualForSamePattern() {

		Criteria left = new Criteria("field").regex("foo");
		Criteria right = new Criteria("field").regex("foo");

		assertThat(left).isEqualTo(right);
	}

	@Test // DATAMONGO-2002
	void shouldEqualForDocument() {

		assertThat(new Criteria("field").is(new Document("one", 1).append("two", "two").append("null", null)))
				.isEqualTo(new Criteria("field").is(new Document("one", 1).append("two", "two").append("null", null)));

		assertThat(new Criteria("field").is(new Document("one", 1).append("two", "two").append("null", null)))
				.isNotEqualTo(new Criteria("field").is(new Document("one", 1).append("two", "two")));

		assertThat(new Criteria("field").is(new Document("one", 1).append("two", "two")))
				.isNotEqualTo(new Criteria("field").is(new Document("one", 1).append("two", "two").append("null", null)));

		assertThat(new Criteria("field").is(new Document("one", 1).append("null", null).append("two", "two")))
				.isNotEqualTo(new Criteria("field").is(new Document("one", 1).append("two", "two").append("null", null)));

		assertThat(new Criteria("field").is(new Document())).isNotEqualTo(new Criteria("field").is("foo"));
		assertThat(new Criteria("field").is("foo")).isNotEqualTo(new Criteria("field").is(new Document()));
	}

	@Test // DATAMONGO-2002
	void shouldEqualForCollection() {

		assertThat(new Criteria("field").is(Arrays.asList("foo", "bar")))
				.isEqualTo(new Criteria("field").is(Arrays.asList("foo", "bar")));

		assertThat(new Criteria("field").is(Arrays.asList("foo", 1)))
				.isNotEqualTo(new Criteria("field").is(Arrays.asList("foo", "bar")));

		assertThat(new Criteria("field").is(Collections.singletonList("foo")))
				.isNotEqualTo(new Criteria("field").is(Arrays.asList("foo", "bar")));

		assertThat(new Criteria("field").is(Arrays.asList("foo", "bar")))
				.isNotEqualTo(new Criteria("field").is(Collections.singletonList("foo")));

		assertThat(new Criteria("field").is(Arrays.asList("foo", "bar"))).isNotEqualTo(new Criteria("field").is("foo"));

		assertThat(new Criteria("field").is("foo")).isNotEqualTo(new Criteria("field").is(Arrays.asList("foo", "bar")));
	}

	@Test // GH-3414
	void shouldEqualForSamePatternAndFlags() {

		Criteria left = new Criteria("field").regex("foo", "iu");
		Criteria right = new Criteria("field").regex("foo");

		assertThat(left).isNotEqualTo(right);
	}

	@Test // GH-3414
	void shouldEqualForNestedPattern() {

		Criteria left = new Criteria("a").orOperator(new Criteria("foo").regex("value", "i"),
				new Criteria("bar").regex("value"));
		Criteria right = new Criteria("a").orOperator(new Criteria("foo").regex("value", "i"),
				new Criteria("bar").regex("value"));

		assertThat(left).isEqualTo(right);
	}
}
