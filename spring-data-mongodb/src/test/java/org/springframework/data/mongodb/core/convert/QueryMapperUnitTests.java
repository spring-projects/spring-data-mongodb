/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.TextScore;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * Unit tests for {@link QueryMapper}.
 * 
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryMapperUnitTests {

	QueryMapper mapper;
	MongoMappingContext context;
	MappingMongoConverter converter;

	@Mock MongoDbFactory factory;

	@Before
	public void setUp() {

		this.context = new MongoMappingContext();

		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), context);
		this.converter.afterPropertiesSet();

		this.mapper = new QueryMapper(converter);
	}

	@Test
	public void translatesIdPropertyIntoIdKey() {

		DBObject query = new BasicDBObject("foo", "value");
		MongoPersistentEntity<?> entity = context.getPersistentEntity(Sample.class);

		DBObject result = mapper.getMappedObject(query, entity);
		assertThat(result.get("_id"), is(notNullValue()));
		assertThat(result.get("foo"), is(nullValue()));
	}

	@Test
	public void convertsStringIntoObjectId() {

		DBObject query = new BasicDBObject("_id", new ObjectId().toString());
		DBObject result = mapper.getMappedObject(query, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id"), is(instanceOf(ObjectId.class)));
	}

	@Test
	public void handlesBigIntegerIdsCorrectly() {

		DBObject dbObject = new BasicDBObject("id", new BigInteger("1"));
		DBObject result = mapper.getMappedObject(dbObject, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id"), is((Object) "1"));
	}

	@Test
	public void handlesObjectIdCapableBigIntegerIdsCorrectly() {

		ObjectId id = new ObjectId();
		DBObject dbObject = new BasicDBObject("id", new BigInteger(id.toString(), 16));
		DBObject result = mapper.getMappedObject(dbObject, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id"), is((Object) id));
	}

	/**
	 * @see DATAMONGO-278
	 */
	@Test
	public void translates$NeCorrectly() {

		Criteria criteria = where("foo").ne(new ObjectId().toString());

		DBObject result = mapper.getMappedObject(criteria.getCriteriaObject(), context.getPersistentEntity(Sample.class));
		Object object = result.get("_id");
		assertThat(object, is(instanceOf(DBObject.class)));
		DBObject dbObject = (DBObject) object;
		assertThat(dbObject.get("$ne"), is(instanceOf(ObjectId.class)));
	}

	/**
	 * @see DATAMONGO-326
	 */
	@Test
	public void handlesEnumsCorrectly() {
		Query query = query(where("foo").is(Enum.INSTANCE));
		DBObject result = mapper.getMappedObject(query.getQueryObject(), null);

		Object object = result.get("foo");
		assertThat(object, is(instanceOf(String.class)));
	}

	@Test
	public void handlesEnumsInNotEqualCorrectly() {
		Query query = query(where("foo").ne(Enum.INSTANCE));
		DBObject result = mapper.getMappedObject(query.getQueryObject(), null);

		Object object = result.get("foo");
		assertThat(object, is(instanceOf(DBObject.class)));

		Object ne = ((DBObject) object).get("$ne");
		assertThat(ne, is(instanceOf(String.class)));
		assertThat(ne.toString(), is(Enum.INSTANCE.name()));
	}

	@Test
	public void handlesEnumsIn$InCorrectly() {

		Query query = query(where("foo").in(Enum.INSTANCE));
		DBObject result = mapper.getMappedObject(query.getQueryObject(), null);

		Object object = result.get("foo");
		assertThat(object, is(instanceOf(DBObject.class)));

		Object in = ((DBObject) object).get("$in");
		assertThat(in, is(instanceOf(BasicDBList.class)));

		BasicDBList list = (BasicDBList) in;
		assertThat(list.size(), is(1));
		assertThat(list.get(0), is(instanceOf(String.class)));
		assertThat(list.get(0).toString(), is(Enum.INSTANCE.name()));
	}

	/**
	 * @see DATAMONGO-373
	 */
	@Test
	public void handlesNativelyBuiltQueryCorrectly() {

		DBObject query = new QueryBuilder().or(new BasicDBObject("foo", "bar")).get();
		mapper.getMappedObject(query, null);
	}

	/**
	 * @see DATAMONGO-369
	 */
	@Test
	public void handlesAllPropertiesIfDBObject() {

		DBObject query = new BasicDBObject();
		query.put("foo", new BasicDBObject("$in", Arrays.asList(1, 2)));
		query.put("bar", new Person());

		DBObject result = mapper.getMappedObject(query, null);
		assertThat(result.get("bar"), is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-429
	 */
	@Test
	public void transformsArraysCorrectly() {

		Query query = new BasicQuery("{ 'tags' : { '$all' : [ 'green', 'orange']}}");

		DBObject result = mapper.getMappedObject(query.getQueryObject(), null);
		assertThat(result, is(query.getQueryObject()));
	}

	@Test
	public void doesHandleNestedFieldsWithDefaultIdNames() {

		BasicDBObject dbObject = new BasicDBObject("id", new ObjectId().toString());
		dbObject.put("nested", new BasicDBObject("id", new ObjectId().toString()));

		MongoPersistentEntity<?> entity = context.getPersistentEntity(ClassWithDefaultId.class);

		DBObject result = mapper.getMappedObject(dbObject, entity);
		assertThat(result.get("_id"), is(instanceOf(ObjectId.class)));
		assertThat(((DBObject) result.get("nested")).get("_id"), is(instanceOf(ObjectId.class)));
	}

	/**
	 * @see DATAMONGO-493
	 */
	@Test
	public void doesNotTranslateNonIdPropertiesFor$NeCriteria() {

		ObjectId accidentallyAnObjectId = new ObjectId();

		Query query = Query.query(Criteria.where("id").is("id_value").and("publishers")
				.ne(accidentallyAnObjectId.toString()));

		DBObject dbObject = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(UserEntity.class));
		assertThat(dbObject.get("publishers"), is(instanceOf(DBObject.class)));

		DBObject publishers = (DBObject) dbObject.get("publishers");
		assertThat(publishers.containsField("$ne"), is(true));
		assertThat(publishers.get("$ne"), is(instanceOf(String.class)));
	}

	/**
	 * @see DATAMONGO-494
	 */
	@Test
	public void usesEntityMetadataInOr() {

		Query query = query(new Criteria().orOperator(where("foo").is("bar")));
		DBObject result = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Sample.class));

		assertThat(result.keySet(), hasSize(1));
		assertThat(result.keySet(), hasItem("$or"));

		BasicDBList ors = getAsDBList(result, "$or");
		assertThat(ors, hasSize(1));
		DBObject criterias = getAsDBObject(ors, 0);
		assertThat(criterias.keySet(), hasSize(1));
		assertThat(criterias.get("_id"), is(notNullValue()));
		assertThat(criterias.get("foo"), is(nullValue()));
	}

	@Test
	public void translatesPropertyReferenceCorrectly() {

		Query query = query(where("field").is(new CustomizedField()));
		DBObject result = mapper
				.getMappedObject(query.getQueryObject(), context.getPersistentEntity(CustomizedField.class));

		assertThat(result.containsField("foo"), is(true));
		assertThat(result.keySet().size(), is(1));
	}

	@Test
	public void translatesNestedPropertyReferenceCorrectly() {

		Query query = query(where("field.field").is(new CustomizedField()));
		DBObject result = mapper
				.getMappedObject(query.getQueryObject(), context.getPersistentEntity(CustomizedField.class));

		assertThat(result.containsField("foo.foo"), is(true));
		assertThat(result.keySet().size(), is(1));
	}

	@Test
	public void returnsOriginalKeyIfNoPropertyReference() {

		Query query = query(where("bar").is(new CustomizedField()));
		DBObject result = mapper
				.getMappedObject(query.getQueryObject(), context.getPersistentEntity(CustomizedField.class));

		assertThat(result.containsField("bar"), is(true));
		assertThat(result.keySet().size(), is(1));
	}

	@Test
	public void convertsAssociationCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("reference").is(reference));
		DBObject object = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(WithDBRef.class));

		Object referenceObject = object.get("reference");

		assertThat(referenceObject, is(instanceOf(com.mongodb.DBRef.class)));
	}

	@Test
	public void convertsNestedAssociationCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("withDbRef.reference").is(reference));
		DBObject object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRefWrapper.class));

		Object referenceObject = object.get("withDbRef.reference");

		assertThat(referenceObject, is(instanceOf(com.mongodb.DBRef.class)));
	}

	@Test
	public void convertsInKeywordCorrectly() {

		Reference first = new Reference();
		first.id = 5L;

		Reference second = new Reference();
		second.id = 6L;

		Query query = query(where("reference").in(first, second));
		DBObject result = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(WithDBRef.class));

		DBObject reference = DBObjectTestUtils.getAsDBObject(result, "reference");

		BasicDBList inClause = getAsDBList(reference, "$in");
		assertThat(inClause, hasSize(2));
		assertThat(inClause.get(0), is(instanceOf(com.mongodb.DBRef.class)));
		assertThat(inClause.get(1), is(instanceOf(com.mongodb.DBRef.class)));
	}

	/**
	 * @see DATAMONGO-570
	 */
	@Test
	public void correctlyConvertsNullReference() {

		Query query = query(where("reference").is(null));
		DBObject object = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(WithDBRef.class));

		assertThat(object.get("reference"), is(nullValue()));
	}

	/**
	 * @see DATAMONGO-629
	 */
	@Test
	public void doesNotMapIdIfNoEntityMetadataAvailable() {

		String id = new ObjectId().toString();
		Query query = query(where("id").is(id));

		DBObject object = mapper.getMappedObject(query.getQueryObject(), null);

		assertThat(object.containsField("id"), is(true));
		assertThat(object.get("id"), is((Object) id));
		assertThat(object.containsField("_id"), is(false));
	}

	/**
	 * @see DATAMONGO-677
	 */
	@Test
	public void handleMapWithDBRefCorrectly() {

		DBObject mapDbObject = new BasicDBObject();
		mapDbObject.put("test", new com.mongodb.DBRef("test", "test"));
		DBObject dbObject = new BasicDBObject();
		dbObject.put("mapWithDBRef", mapDbObject);

		DBObject mapped = mapper.getMappedObject(dbObject, context.getPersistentEntity(WithMapDBRef.class));

		assertThat(mapped.containsField("mapWithDBRef"), is(true));
		assertThat(mapped.get("mapWithDBRef"), instanceOf(BasicDBObject.class));
		assertThat(((BasicDBObject) mapped.get("mapWithDBRef")).containsField("test"), is(true));
		assertThat(((BasicDBObject) mapped.get("mapWithDBRef")).get("test"), instanceOf(com.mongodb.DBRef.class));
	}

	@Test
	public void convertsUnderscoreIdValueWithoutMetadata() {

		DBObject dbObject = new BasicDBObject().append("_id", new ObjectId().toString());

		DBObject mapped = mapper.getMappedObject(dbObject, null);
		assertThat(mapped.containsField("_id"), is(true));
		assertThat(mapped.get("_id"), is(instanceOf(ObjectId.class)));
	}

	/**
	 * @see DATAMONGO-705
	 */
	@Test
	public void convertsDBRefWithExistsQuery() {

		Query query = query(where("reference").exists(false));

		BasicMongoPersistentEntity<?> entity = context.getPersistentEntity(WithDBRef.class);
		DBObject mappedObject = mapper.getMappedObject(query.getQueryObject(), entity);

		DBObject reference = getAsDBObject(mappedObject, "reference");
		assertThat(reference.containsField("$exists"), is(true));
		assertThat(reference.get("$exists"), is((Object) false));
	}

	/**
	 * @see DATAMONGO-706
	 */
	@Test
	public void convertsNestedDBRefsCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("someString").is("foo").andOperator(where("reference").in(reference)));

		BasicMongoPersistentEntity<?> entity = context.getPersistentEntity(WithDBRef.class);
		DBObject mappedObject = mapper.getMappedObject(query.getQueryObject(), entity);

		assertThat(mappedObject.get("someString"), is((Object) "foo"));

		BasicDBList andClause = getAsDBList(mappedObject, "$and");
		assertThat(andClause, hasSize(1));

		BasicDBList inClause = getAsDBList(getAsDBObject(getAsDBObject(andClause, 0), "reference"), "$in");
		assertThat(inClause, hasSize(1));
		assertThat(inClause.get(0), is(instanceOf(com.mongodb.DBRef.class)));
	}

	/**
	 * @see DATAMONGO-752
	 */
	@Test
	public void mapsSimpleValuesStartingWith$Correctly() {

		Query query = query(where("myvalue").is("$334"));

		DBObject result = mapper.getMappedObject(query.getQueryObject(), null);

		assertThat(result.keySet(), hasSize(1));
		assertThat(result.get("myvalue"), is((Object) "$334"));
	}

	/**
	 * @see DATAMONGO-752
	 */
	@Test
	public void mapsKeywordAsSimpleValuesCorrectly() {

		Query query = query(where("myvalue").is("$center"));

		DBObject result = mapper.getMappedObject(query.getQueryObject(), null);

		assertThat(result.keySet(), hasSize(1));
		assertThat(result.get("myvalue"), is((Object) "$center"));
	}

	/**
	 * @DATAMONGO-805
	 */
	@Test
	public void shouldExcludeDBRefAssociation() {

		Query query = query(where("someString").is("foo"));
		query.fields().exclude("reference");

		BasicMongoPersistentEntity<?> entity = context.getPersistentEntity(WithDBRef.class);
		DBObject queryResult = mapper.getMappedObject(query.getQueryObject(), entity);
		DBObject fieldsResult = mapper.getMappedObject(query.getFieldsObject(), entity);

		assertThat(queryResult.get("someString"), is((Object) "foo"));
		assertThat(fieldsResult.get("reference"), is((Object) 0));
	}

	/**
	 * @see DATAMONGO-686
	 */
	@Test
	public void queryMapperShouldNotChangeStateInGivenQueryObjectWhenIdConstrainedByInList() {

		BasicMongoPersistentEntity<?> persistentEntity = context.getPersistentEntity(Sample.class);
		String idPropertyName = persistentEntity.getIdProperty().getName();
		DBObject queryObject = query(where(idPropertyName).in("42")).getQueryObject();

		Object idValuesBefore = getAsDBObject(queryObject, idPropertyName).get("$in");
		mapper.getMappedObject(queryObject, persistentEntity);
		Object idValuesAfter = getAsDBObject(queryObject, idPropertyName).get("$in");

		assertThat(idValuesAfter, is(idValuesBefore));
	}

	/**
	 * @see DATAMONGO-821
	 */
	@Test
	public void queryMapperShouldNotTryToMapDBRefListPropertyIfNestedInsideDBObjectWithinDBObject() {

		DBObject queryObject = query(
				where("referenceList").is(new BasicDBObject("$nested", new BasicDBObject("$keys", 0L)))).getQueryObject();

		DBObject mappedObject = mapper.getMappedObject(queryObject, context.getPersistentEntity(WithDBRefList.class));
		DBObject referenceObject = getAsDBObject(mappedObject, "referenceList");
		DBObject nestedObject = getAsDBObject(referenceObject, "$nested");

		assertThat(nestedObject, is((DBObject) new BasicDBObject("$keys", 0L)));
	}

	/**
	 * @see DATAMONGO-821
	 */
	@Test
	public void queryMapperShouldNotTryToMapDBRefPropertyIfNestedInsideDBObjectWithinDBObject() {

		DBObject queryObject = query(where("reference").is(new BasicDBObject("$nested", new BasicDBObject("$keys", 0L))))
				.getQueryObject();

		DBObject mappedObject = mapper.getMappedObject(queryObject, context.getPersistentEntity(WithDBRef.class));
		DBObject referenceObject = getAsDBObject(mappedObject, "reference");
		DBObject nestedObject = getAsDBObject(referenceObject, "$nested");

		assertThat(nestedObject, is((DBObject) new BasicDBObject("$keys", 0L)));
	}

	/**
	 * @see DATAMONGO-821
	 */
	@Test
	public void queryMapperShouldMapDBRefPropertyIfNestedInDBObject() {

		Reference sample = new Reference();
		sample.id = 321L;
		DBObject queryObject = query(where("reference").is(new BasicDBObject("$in", Arrays.asList(sample))))
				.getQueryObject();

		DBObject mappedObject = mapper.getMappedObject(queryObject, context.getPersistentEntity(WithDBRef.class));

		DBObject referenceObject = getAsDBObject(mappedObject, "reference");
		BasicDBList inObject = getAsDBList(referenceObject, "$in");

		assertThat(inObject.get(0), is(instanceOf(com.mongodb.DBRef.class)));
	}

	/**
	 * @see DATAMONGO-773
	 */
	@Test
	public void queryMapperShouldBeAbleToProcessQueriesThatIncludeDbRefFields() {

		BasicMongoPersistentEntity<?> persistentEntity = context.getPersistentEntity(WithDBRef.class);

		Query qry = query(where("someString").is("abc"));
		qry.fields().include("reference");

		DBObject mappedFields = mapper.getMappedObject(qry.getFieldsObject(), persistentEntity);
		assertThat(mappedFields, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-893
	 */
	@Test
	public void classInformationShouldNotBePresentInDBObjectUsedInFinderMethods() {

		EmbeddedClass embedded = new EmbeddedClass();
		embedded.id = "1";

		EmbeddedClass embedded2 = new EmbeddedClass();
		embedded2.id = "2";
		Query query = query(where("embedded").in(Arrays.asList(embedded, embedded2)));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));
		assertThat(dbo.toString(), equalTo("{ \"embedded\" : { \"$in\" : [ { \"_id\" : \"1\"} , { \"_id\" : \"2\"}]}}"));
	}

	/**
	 * @see DATAMONGO-647
	 */
	@Test
	public void customizedFieldNameShouldBeMappedCorrectlyWhenApplyingSort() {

		Query query = query(where("field").is("bar")).with(new Sort(Direction.DESC, "field"));
		DBObject dbo = mapper.getMappedObject(query.getSortObject(), context.getPersistentEntity(CustomizedField.class));
		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("foo", -1).get()));
	}

	/**
	 * @see DATAMONGO-973
	 */
	@Test
	public void getMappedFieldsAppendsTextScoreFieldProperlyCorrectlyWhenNotPresent() {

		Query query = new Query();

		DBObject dbo = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("score", new BasicDBObject("$meta", "textScore")).get()));
	}

	/**
	 * @see DATAMONGO-973
	 */
	@Test
	public void getMappedFieldsReplacesTextScoreFieldProperlyCorrectlyWhenPresent() {

		Query query = new Query();
		query.fields().include("textScore");

		DBObject dbo = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("score", new BasicDBObject("$meta", "textScore")).get()));
	}

	/**
	 * @see DATAMONGO-973
	 */
	@Test
	public void getMappedSortAppendsTextScoreProperlyWhenSortedByScore() {

		Query query = new Query().with(new Sort("textScore"));

		DBObject dbo = mapper
				.getMappedSort(query.getSortObject(), context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("score", new BasicDBObject("$meta", "textScore")).get()));
	}

	/**
	 * @see DATAMONGO-973
	 */
	@Test
	public void getMappedSortIgnoresTextScoreWhenNotSortedByScore() {

		Query query = new Query().with(new Sort("id"));

		DBObject dbo = mapper
				.getMappedSort(query.getSortObject(), context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("_id", 1).get()));
	}

	/**
	 * @see DATAMONGO-1070
	 */
	@Test
	public void mapsIdReferenceToDBRefCorrectly() {

		ObjectId id = new ObjectId();

		DBObject query = new BasicDBObject("reference.id", new com.mongodb.DBRef("reference", id.toString()));
		DBObject result = mapper.getMappedObject(query, context.getPersistentEntity(WithDBRef.class));

		assertThat(result.containsField("reference"), is(true));
		com.mongodb.DBRef reference = getTypedValue(result, "reference", com.mongodb.DBRef.class);
		assertThat(reference.getId(), is(instanceOf(ObjectId.class)));
	}

	/**
	 * @see DATAMONGO-1050
	 */
	@Test
	public void shouldUseExplicitlySetFieldnameForIdPropertyCandidates() {

		Query query = query(where("nested.id").is("bar"));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("nested.id", "bar").get()));
	}

	/**
	 * @see DATAMONGO-1050
	 */
	@Test
	public void shouldUseExplicitlySetFieldnameForIdPropertyCandidatesUsedInSortClause() {

		Query query = new Query().with(new Sort("nested.id"));

		DBObject dbo = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("nested.id", 1).get()));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void nearShouldUseGeoJsonRepresentationOnUnmappedProperty() {

		Query query = query(where("foo").near(new GeoJsonPoint(100, 50)));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(dbo, isBsonObject().containing("foo.$near.$geometry.type", "Point"));
		assertThat(dbo, isBsonObject().containing("foo.$near.$geometry.coordinates.[0]", 100D));
		assertThat(dbo, isBsonObject().containing("foo.$near.$geometry.coordinates.[1]", 50D));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void nearShouldUseGeoJsonRepresentationWhenMappingToGoJsonType() {

		Query query = query(where("geoJsonPoint").near(new GeoJsonPoint(100, 50)));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(dbo, isBsonObject().containing("geoJsonPoint.$near.$geometry.type", "Point"));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void nearSphereShouldUseGeoJsonRepresentationWhenMappingToGoJsonType() {

		Query query = query(where("geoJsonPoint").nearSphere(new GeoJsonPoint(100, 50)));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(dbo, isBsonObject().containing("geoJsonPoint.$nearSphere.$geometry.type", "Point"));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void shouldMapNameCorrectlyForGeoJsonType() {

		Query query = query(where("namedGeoJsonPoint").nearSphere(new GeoJsonPoint(100, 50)));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(dbo,
				isBsonObject().containing("geoJsonPointWithNameViaFieldAnnotation.$nearSphere.$geometry.type", "Point"));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void withinShouldUseGeoJsonPolygonWhenMappingPolygonOn2DSphereIndex() {

		Query query = query(where("geoJsonPoint").within(
				new GeoJsonPolygon(new Point(0, 0), new Point(100, 100), new Point(100, 0), new Point(0, 0))));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(dbo, isBsonObject().containing("geoJsonPoint.$geoWithin.$geometry.type", "Polygon"));
	}

	/**
	 * @see DATAMONGO-1269
	 */
	@Test
	public void mappingShouldRetainNumericMapKey() {

		Query query = query(where("map.1.stringProperty").is("ba'alzamon"));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithComplexValueTypeMap.class));

		assertThat(dbo.containsField("map.1.stringProperty"), is(true));
	}

	/**
	 * @see DATAMONGO-1269
	 */
	@Test
	public void mappingShouldRetainNumericPositionInList() {

		Query query = query(where("list.1.stringProperty").is("ba'alzamon"));

		DBObject dbo = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithComplexValueTypeList.class));

		assertThat(dbo.containsField("list.1.stringProperty"), is(true));
	}

	@Document
	public class Foo {
		@Id private ObjectId id;
		EmbeddedClass embedded;
	}

	public class EmbeddedClass {
		public String id;
	}

	class IdWrapper {
		Object id;
	}

	class ClassWithEmbedded {
		@Id String id;
		Sample sample;
	}

	class ClassWithDefaultId {

		String id;
		ClassWithDefaultId nested;
	}

	class Sample {

		@Id private String foo;
	}

	class BigIntegerId {

		@Id private BigInteger id;
	}

	enum Enum {
		INSTANCE;
	}

	class UserEntity {
		String id;
		List<String> publishers = new ArrayList<String>();
	}

	class CustomizedField {

		@Field("foo") CustomizedField field;
	}

	class WithDBRef {

		String someString;
		@DBRef Reference reference;
	}

	class WithDBRefList {

		String someString;
		@DBRef List<Reference> referenceList;
	}

	class Reference {

		Long id;
	}

	class WithDBRefWrapper {

		WithDBRef withDbRef;
	}

	class WithMapDBRef {

		@DBRef Map<String, Sample> mapWithDBRef;
	}

	class WithTextScoreProperty {

		@Id String id;
		@TextScore @Field("score") Float textScore;
	}

	static class RootForClassWithExplicitlyRenamedIdField {

		@Id String id;
		ClassWithExplicitlyRenamedField nested;
	}

	static class ClassWithExplicitlyRenamedField {

		@Field("id") String id;
	}

	static class ClassWithGeoTypes {

		double[] justAnArray;
		Point legacyPoint;
		GeoJsonPoint geoJsonPoint;
		@Field("geoJsonPointWithNameViaFieldAnnotation") GeoJsonPoint namedGeoJsonPoint;
	}

	static class SimpeEntityWithoutId {

		String stringProperty;
		Integer integerProperty;
	}

	static class EntityWithComplexValueTypeMap {
		Map<Integer, SimpeEntityWithoutId> map;
	}

	static class EntityWithComplexValueTypeList {
		List<SimpeEntityWithoutId> list;
	}
}
