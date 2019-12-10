/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.springframework.data.mongodb.core.DocumentTestUtils.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.bson.types.Code;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.TextScore;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class QueryMapperUnitTests {

	QueryMapper mapper;
	MongoMappingContext context;
	MappingMongoConverter converter;

	@Mock MongoDbFactory factory;

	@BeforeEach
	public void beforeEach() {

		this.context = new MongoMappingContext();

		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), context);
		this.converter.afterPropertiesSet();

		this.mapper = new QueryMapper(converter);
	}

	@Test
	public void translatesIdPropertyIntoIdKey() {

		org.bson.Document query = new org.bson.Document("foo", "value");
		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(Sample.class);

		org.bson.Document result = mapper.getMappedObject(query, entity);
		assertThat(result.get("_id")).isNotNull();
		assertThat(result.get("foo")).isNull();
	}

	@Test
	public void convertsStringIntoObjectId() {

		org.bson.Document query = new org.bson.Document("_id", new ObjectId().toString());
		org.bson.Document result = mapper.getMappedObject(query, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test
	public void handlesBigIntegerIdsCorrectly() {

		org.bson.Document document = new org.bson.Document("id", new BigInteger("1"));
		org.bson.Document result = mapper.getMappedObject(document, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id")).isEqualTo("1");
	}

	@Test
	public void handlesObjectIdCapableBigIntegerIdsCorrectly() {

		ObjectId id = new ObjectId();
		org.bson.Document document = new org.bson.Document("id", new BigInteger(id.toString(), 16));
		org.bson.Document result = mapper.getMappedObject(document, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id")).isEqualTo(id);
	}

	@Test // DATAMONGO-278
	public void translates$NeCorrectly() {

		Criteria criteria = where("foo").ne(new ObjectId().toString());

		org.bson.Document result = mapper.getMappedObject(criteria.getCriteriaObject(),
				context.getPersistentEntity(Sample.class));
		Object object = result.get("_id");
		assertThat(object).isInstanceOf(org.bson.Document.class);
		org.bson.Document document = (org.bson.Document) object;
		assertThat(document.get("$ne")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-326
	public void handlesEnumsCorrectly() {
		Query query = query(where("foo").is(Enum.INSTANCE));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		Object object = result.get("foo");
		assertThat(object).isInstanceOf(String.class);
	}

	@Test
	public void handlesEnumsInNotEqualCorrectly() {
		Query query = query(where("foo").ne(Enum.INSTANCE));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		Object object = result.get("foo");
		assertThat(object).isInstanceOf(org.bson.Document.class);

		Object ne = ((org.bson.Document) object).get("$ne");
		assertThat(ne).isInstanceOf(String.class);
		assertThat(ne.toString()).isEqualTo(Enum.INSTANCE.name());
	}

	@Test
	public void handlesEnumsIn$InCorrectly() {

		Query query = query(where("foo").in(Enum.INSTANCE));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		Object object = result.get("foo");
		assertThat(object).isInstanceOf(org.bson.Document.class);

		Object in = ((org.bson.Document) object).get("$in");
		assertThat(in).isInstanceOf(List.class);

		List list = (List) in;
		assertThat(list).hasSize(1);
		assertThat(list.get(0)).isInstanceOf(String.class);
		assertThat(list.get(0).toString()).isEqualTo(Enum.INSTANCE.name());
	}

	@Test // DATAMONGO-373
	public void handlesNativelyBuiltQueryCorrectly() {

		Bson query = new BasicDBObject(Filters.or(new BasicDBObject("foo", "bar")).toBsonDocument(org.bson.Document.class, MongoClientSettings.getDefaultCodecRegistry()));
		mapper.getMappedObject(query, Optional.empty());
	}

	@Test // DATAMONGO-369
	public void handlesAllPropertiesIfDocument() {

		org.bson.Document query = new org.bson.Document();
		query.put("foo", new org.bson.Document("$in", Arrays.asList(1, 2)));
		query.put("bar", new Person());

		org.bson.Document result = mapper.getMappedObject(query, Optional.empty());
		assertThat(result.get("bar")).isNotNull();
	}

	@Test // DATAMONGO-429
	public void transformsArraysCorrectly() {

		Query query = new BasicQuery("{ 'tags' : { '$all' : [ 'green', 'orange']}}");

		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());
		assertThat(result.toJson()).isEqualTo(query.getQueryObject().toJson());
	}

	@Test
	public void doesHandleNestedFieldsWithDefaultIdNames() {

		org.bson.Document document = new org.bson.Document("id", new ObjectId().toString());
		document.put("nested", new org.bson.Document("id", new ObjectId().toString()));

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(ClassWithDefaultId.class);

		org.bson.Document result = mapper.getMappedObject(document, entity);
		assertThat(result.get("_id")).isInstanceOf(ObjectId.class);
		assertThat(((org.bson.Document) result.get("nested")).get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-493
	public void doesNotTranslateNonIdPropertiesFor$NeCriteria() {

		ObjectId accidentallyAnObjectId = new ObjectId();

		Query query = Query
				.query(Criteria.where("id").is("id_value").and("publishers").ne(accidentallyAnObjectId.toString()));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(UserEntity.class));
		assertThat(document.get("publishers")).isInstanceOf(org.bson.Document.class);

		org.bson.Document publishers = (org.bson.Document) document.get("publishers");
		assertThat(publishers.containsKey("$ne")).isTrue();
		assertThat(publishers.get("$ne")).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-494
	public void usesEntityMetadataInOr() {

		Query query = query(new Criteria().orOperator(where("foo").is("bar")));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(Sample.class));

		assertThat(result.keySet()).hasSize(1).containsOnly("$or");

		List<Object> ors = getAsDBList(result, "$or");
		assertThat(ors).hasSize(1);
		org.bson.Document criterias = getAsDocument(ors, 0);
		assertThat(criterias.keySet()).hasSize(1).doesNotContain("foo");
		assertThat(criterias.get("_id")).isNotNull();
	}

	@Test
	public void translatesPropertyReferenceCorrectly() {

		Query query = query(where("field").is(new CustomizedField()));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));

		assertThat(result.containsKey("foo")).isTrue();
		assertThat(result.keySet()).hasSize(1);
	}

	@Test
	public void translatesNestedPropertyReferenceCorrectly() {

		Query query = query(where("field.field").is(new CustomizedField()));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));

		assertThat(result.containsKey("foo.foo")).isTrue();
		assertThat(result.keySet()).hasSize(1);
	}

	@Test
	public void returnsOriginalKeyIfNoPropertyReference() {

		Query query = query(where("bar").is(new CustomizedField()));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));

		assertThat(result.containsKey("bar")).isTrue();
		assertThat(result.keySet()).hasSize(1);
	}

	@Test
	public void convertsAssociationCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("reference").is(reference));
		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		Object referenceObject = object.get("reference");

		assertThat(referenceObject).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test
	public void convertsNestedAssociationCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("withDbRef.reference").is(reference));
		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRefWrapper.class));

		Object referenceObject = object.get("withDbRef.reference");

		assertThat(referenceObject).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test
	public void convertsInKeywordCorrectly() {

		Reference first = new Reference();
		first.id = 5L;

		Reference second = new Reference();
		second.id = 6L;

		Query query = query(where("reference").in(first, second));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		org.bson.Document reference = DocumentTestUtils.getAsDocument(result, "reference");

		List<Object> inClause = getAsDBList(reference, "$in");
		assertThat(inClause).hasSize(2);
		assertThat(inClause.get(0)).isInstanceOf(com.mongodb.DBRef.class);
		assertThat(inClause.get(1)).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test // DATAMONGO-570
	public void correctlyConvertsNullReference() {

		Query query = query(where("reference").is(null));
		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		assertThat(object.get("reference")).isNull();
	}

	@Test // DATAMONGO-629
	public void doesNotMapIdIfNoEntityMetadataAvailable() {

		String id = new ObjectId().toString();
		Query query = query(where("id").is(id));

		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		assertThat(object.containsKey("id")).isTrue();
		assertThat(object.get("id")).isEqualTo(id);
		assertThat(object.containsKey("_id")).isFalse();
	}

	@Test // DATAMONGO-677
	public void handleMapWithDBRefCorrectly() {

		org.bson.Document mapDocument = new org.bson.Document();
		mapDocument.put("test", new com.mongodb.DBRef("test", "test"));
		org.bson.Document document = new org.bson.Document();
		document.put("mapWithDBRef", mapDocument);

		org.bson.Document mapped = mapper.getMappedObject(document, context.getPersistentEntity(WithMapDBRef.class));

		assertThat(mapped.containsKey("mapWithDBRef")).isTrue();
		assertThat(mapped.get("mapWithDBRef")).isInstanceOf(org.bson.Document.class);
		assertThat(((org.bson.Document) mapped.get("mapWithDBRef")).containsKey("test")).isTrue();
		assertThat(((org.bson.Document) mapped.get("mapWithDBRef")).get("test")).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test
	public void convertsUnderscoreIdValueWithoutMetadata() {

		org.bson.Document document = new org.bson.Document().append("_id", new ObjectId().toString());

		org.bson.Document mapped = mapper.getMappedObject(document, Optional.empty());
		assertThat(mapped.containsKey("_id")).isTrue();
		assertThat(mapped.get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-705
	public void convertsDBRefWithExistsQuery() {

		Query query = query(where("reference").exists(false));

		BasicMongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithDBRef.class);
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(), entity);

		org.bson.Document reference = getAsDocument(mappedObject, "reference");
		assertThat(reference.containsKey("$exists")).isTrue();
		assertThat(reference.get("$exists")).isEqualTo(false);
	}

	@Test // DATAMONGO-706
	public void convertsNestedDBRefsCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("someString").is("foo").andOperator(where("reference").in(reference)));

		BasicMongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithDBRef.class);
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(), entity);

		assertThat(mappedObject.get("someString")).isEqualTo("foo");

		List<Object> andClause = getAsDBList(mappedObject, "$and");
		assertThat(andClause).hasSize(1);

		List<Object> inClause = getAsDBList(getAsDocument(getAsDocument(andClause, 0), "reference"), "$in");
		assertThat(inClause).hasSize(1);
		assertThat(inClause.get(0)).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test // DATAMONGO-752
	public void mapsSimpleValuesStartingWith$Correctly() {

		Query query = query(where("myvalue").is("$334"));

		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		assertThat(result.keySet()).hasSize(1);
		assertThat(result.get("myvalue")).isEqualTo("$334");
	}

	@Test // DATAMONGO-752
	public void mapsKeywordAsSimpleValuesCorrectly() {

		Query query = query(where("myvalue").is("$center"));

		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		assertThat(result.keySet()).hasSize(1);
		assertThat(result.get("myvalue")).isEqualTo("$center");
	}

	@Test // DATAMONGO-805
	public void shouldExcludeDBRefAssociation() {

		Query query = query(where("someString").is("foo"));
		query.fields().exclude("reference");

		BasicMongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithDBRef.class);
		org.bson.Document queryResult = mapper.getMappedObject(query.getQueryObject(), entity);
		org.bson.Document fieldsResult = mapper.getMappedObject(query.getFieldsObject(), entity);

		assertThat(queryResult.get("someString")).isEqualTo("foo");
		assertThat(fieldsResult.get("reference")).isEqualTo(0);
	}

	@Test // DATAMONGO-686
	public void queryMapperShouldNotChangeStateInGivenQueryObjectWhenIdConstrainedByInList() {

		BasicMongoPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(Sample.class);
		String idPropertyName = persistentEntity.getIdProperty().getName();
		org.bson.Document queryObject = query(where(idPropertyName).in("42")).getQueryObject();

		Object idValuesBefore = getAsDocument(queryObject, idPropertyName).get("$in");
		mapper.getMappedObject(queryObject, persistentEntity);
		Object idValuesAfter = getAsDocument(queryObject, idPropertyName).get("$in");

		assertThat(idValuesAfter).isEqualTo(idValuesBefore);
	}

	@Test // DATAMONGO-821
	public void queryMapperShouldNotTryToMapDBRefListPropertyIfNestedInsideDocumentWithinDocument() {

		org.bson.Document queryObject = query(
				where("referenceList").is(new org.bson.Document("$nested", new org.bson.Document("$keys", 0L))))
						.getQueryObject();

		org.bson.Document mappedObject = mapper.getMappedObject(queryObject,
				context.getPersistentEntity(WithDBRefList.class));
		org.bson.Document referenceObject = getAsDocument(mappedObject, "referenceList");
		org.bson.Document nestedObject = getAsDocument(referenceObject, "$nested");

		assertThat(nestedObject).isEqualTo(new org.bson.Document("$keys", 0L));
	}

	@Test // DATAMONGO-821
	public void queryMapperShouldNotTryToMapDBRefPropertyIfNestedInsideDocumentWithinDocument() {

		org.bson.Document queryObject = query(
				where("reference").is(new org.bson.Document("$nested", new org.bson.Document("$keys", 0L)))).getQueryObject();

		org.bson.Document mappedObject = mapper.getMappedObject(queryObject, context.getPersistentEntity(WithDBRef.class));
		org.bson.Document referenceObject = getAsDocument(mappedObject, "reference");
		org.bson.Document nestedObject = getAsDocument(referenceObject, "$nested");

		assertThat(nestedObject).isEqualTo(new org.bson.Document("$keys", 0L));
	}

	@Test // DATAMONGO-821
	public void queryMapperShouldMapDBRefPropertyIfNestedInDocument() {

		Reference sample = new Reference();
		sample.id = 321L;
		org.bson.Document queryObject = query(
				where("reference").is(new org.bson.Document("$in", Collections.singletonList(sample)))).getQueryObject();

		org.bson.Document mappedObject = mapper.getMappedObject(queryObject, context.getPersistentEntity(WithDBRef.class));

		org.bson.Document referenceObject = getAsDocument(mappedObject, "reference");
		List<Object> inObject = getAsDBList(referenceObject, "$in");

		assertThat(inObject.get(0)).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test // DATAMONGO-773
	public void queryMapperShouldBeAbleToProcessQueriesThatIncludeDbRefFields() {

		BasicMongoPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(WithDBRef.class);

		Query qry = query(where("someString").is("abc"));
		qry.fields().include("reference");

		org.bson.Document mappedFields = mapper.getMappedObject(qry.getFieldsObject(), persistentEntity);
		assertThat(mappedFields).isNotNull();
	}

	@Test // DATAMONGO-893
	public void classInformationShouldNotBePresentInDocumentUsedInFinderMethods() {

		EmbeddedClass embedded = new EmbeddedClass();
		embedded.id = "1";

		EmbeddedClass embedded2 = new EmbeddedClass();
		embedded2.id = "2";
		Query query = query(where("embedded").in(Arrays.asList(embedded, embedded2)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));
		assertThat(document).isEqualTo("{ \"embedded\" : { \"$in\" : [ { \"_id\" : \"1\"} , { \"_id\" : \"2\"}]}}");
	}

	@Test // DATAMONGO-1406
	public void shouldMapQueryForNestedCustomizedPropertiesUsingConfiguredFieldNames() {

		EmbeddedClass embeddedClass = new EmbeddedClass();
		embeddedClass.customizedField = "hello";

		Foo foo = new Foo();
		foo.listOfItems = Collections.singletonList(embeddedClass);

		Query query = new Query(Criteria.where("listOfItems") //
				.elemMatch(new Criteria(). //
						andOperator(Criteria.where("customizedField").is(embeddedClass.customizedField))));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));

		assertThat(document).containsEntry("my_items.$elemMatch.$and",
				Collections.singletonList(new org.bson.Document("fancy_custom_name", embeddedClass.customizedField)));
	}

	@Test // DATAMONGO-647
	public void customizedFieldNameShouldBeMappedCorrectlyWhenApplyingSort() {

		Query query = query(where("field").is("bar")).with(Sort.by(Direction.DESC, "field"));
		org.bson.Document document = mapper.getMappedObject(query.getSortObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(document).isEqualTo(new org.bson.Document().append("foo", -1));
	}

	@Test // DATAMONGO-973
	public void getMappedFieldsAppendsTextScoreFieldProperlyCorrectlyWhenNotPresent() {

		Query query = new Query();

		org.bson.Document document = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document)
				.isEqualTo(new org.bson.Document().append("score", new org.bson.Document("$meta", "textScore")));
	}

	@Test // DATAMONGO-973
	public void getMappedFieldsReplacesTextScoreFieldProperlyCorrectlyWhenPresent() {

		Query query = new Query();
		query.fields().include("textScore");

		org.bson.Document document = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document)
				.isEqualTo(new org.bson.Document().append("score", new org.bson.Document("$meta", "textScore")));
	}

	@Test // DATAMONGO-973
	public void getMappedSortAppendsTextScoreProperlyWhenSortedByScore() {

		Query query = new Query().with(Sort.by("textScore"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document)
				.isEqualTo(new org.bson.Document().append("score", new org.bson.Document("$meta", "textScore")));
	}

	@Test // DATAMONGO-973
	public void getMappedSortIgnoresTextScoreWhenNotSortedByScore() {

		Query query = new Query().with(Sort.by("id"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document).isEqualTo(new org.bson.Document().append("_id", 1));
	}

	@Test // DATAMONGO-1070, DATAMONGO-1798
	public void mapsIdReferenceToDBRefCorrectly() {

		ObjectId id = new ObjectId();

		org.bson.Document query = new org.bson.Document("reference.id", new com.mongodb.DBRef("reference", id));
		org.bson.Document result = mapper.getMappedObject(query, context.getPersistentEntity(WithDBRef.class));

		assertThat(result.containsKey("reference")).isTrue();
		com.mongodb.DBRef reference = getTypedValue(result, "reference", com.mongodb.DBRef.class);
		assertThat(reference.getId()).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-1050
	public void shouldUseExplicitlySetFieldnameForIdPropertyCandidates() {

		Query query = query(where("nested.id").is("bar"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document().append("nested.id", "bar"));
	}

	@Test // DATAMONGO-1050
	public void shouldUseExplicitlySetFieldnameForIdPropertyCandidatesUsedInSortClause() {

		Query query = new Query().with(Sort.by("nested.id"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document().append("nested.id", 1));
	}

	@Test // DATAMONGO-1135
	public void nearShouldUseGeoJsonRepresentationOnUnmappedProperty() {

		Query query = query(where("foo").near(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("foo.$near.$geometry.type", "Point");
		assertThat(document).containsEntry("foo.$near.$geometry.coordinates.[0]", 100D);
		assertThat(document).containsEntry("foo.$near.$geometry.coordinates.[1]", 50D);
	}

	@Test // DATAMONGO-1135
	public void nearShouldUseGeoJsonRepresentationWhenMappingToGoJsonType() {

		Query query = query(where("geoJsonPoint").near(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$near.$geometry.type", "Point");
	}

	@Test // DATAMONGO-1135
	public void nearSphereShouldUseGeoJsonRepresentationWhenMappingToGoJsonType() {

		Query query = query(where("geoJsonPoint").nearSphere(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$nearSphere.$geometry.type", "Point");
	}

	@Test // DATAMONGO-1135
	public void shouldMapNameCorrectlyForGeoJsonType() {

		Query query = query(where("namedGeoJsonPoint").nearSphere(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPointWithNameViaFieldAnnotation.$nearSphere.$geometry.type", "Point");
	}

	@Test // DATAMONGO-1135
	public void withinShouldUseGeoJsonPolygonWhenMappingPolygonOn2DSphereIndex() {

		Query query = query(where("geoJsonPoint")
				.within(new GeoJsonPolygon(new Point(0, 0), new Point(100, 100), new Point(100, 0), new Point(0, 0))));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$geoWithin.$geometry.type", "Polygon");
	}

	@Test // DATAMONGO-1134
	public void intersectsShouldUseGeoJsonRepresentationCorrectly() {

		Query query = query(where("geoJsonPoint")
				.intersects(new GeoJsonPolygon(new Point(0, 0), new Point(100, 100), new Point(100, 0), new Point(0, 0))));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$geoIntersects.$geometry.type", "Polygon");
		assertThat(document).containsKey("geoJsonPoint.$geoIntersects.$geometry.coordinates");
	}

	@Test // DATAMONGO-1269
	public void mappingShouldRetainNumericMapKey() {

		Query query = query(where("map.1.stringProperty").is("ba'alzamon"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithComplexValueTypeMap.class));

		assertThat(document.containsKey("map.1.stringProperty")).isTrue();
	}

	@Test // DATAMONGO-1269
	public void mappingShouldRetainNumericPositionInList() {

		Query query = query(where("list.1.stringProperty").is("ba'alzamon"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithComplexValueTypeList.class));

		assertThat(document.containsKey("list.1.stringProperty")).isTrue();
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectly() {

		Foo probe = new Foo();
		probe.embedded = new EmbeddedClass();
		probe.embedded.id = "conflux";

		Query query = query(byExample(probe));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));

		assertThat(document).containsEntry("embedded\\._id", "conflux");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyWhenContainingLegacyPoint() {

		ClassWithGeoTypes probe = new ClassWithGeoTypes();
		probe.legacyPoint = new Point(10D, 20D);

		Query query = query(byExample(probe));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		assertThat(document.get("legacyPoint.x")).isEqualTo(10D);
		assertThat(document.get("legacyPoint.y")).isEqualTo(20D);
	}

	@Test // DATAMONGO-1988
	public void mapsStringObjectIdRepresentationToObjectIdWhenReferencingIdProperty() {

		Query query = query(where("sample.foo").is(new ObjectId().toHexString()));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithEmbedded.class));

		assertThat(document.get("sample._id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-1988
	public void matchesExactFieldNameToIdProperty() {

		Query query = query(where("sample.iid").is(new ObjectId().toHexString()));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithEmbedded.class));

		assertThat(document.get("sample.iid")).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-1988
	public void leavesNonObjectIdStringIdRepresentationUntouchedWhenReferencingIdProperty() {

		Query query = query(where("sample.foo").is("id-1"));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithEmbedded.class));

		assertThat(document.get("sample._id")).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-2168
	public void getMappedObjectShouldNotMapTypeHint() {

		converter.setTypeMapper(new DefaultMongoTypeMapper("className"));

		org.bson.Document update = new org.bson.Document("className", "foo");
		org.bson.Document mappedObject = mapper.getMappedObject(update, context.getPersistentEntity(UserEntity.class));

		assertThat(mappedObject).containsEntry("className", "foo");
	}

	@Test // DATAMONGO-2168
	public void getMappedObjectShouldIgnorePathsLeadingToJavaLangClassProperties/* like Class#getName() */() {

		org.bson.Document update = new org.bson.Document("className", "foo");
		org.bson.Document mappedObject = mapper.getMappedObject(update, context.getPersistentEntity(UserEntity.class));

		assertThat(mappedObject).containsEntry("className", "foo");
	}

	@Test // DATAMONGO-2193
	public void shouldNotConvertHexStringToObjectIdForRenamedNestedIdField() {

		String idHex = new ObjectId().toHexString();
		Query query = new Query(where("nested.id").is(idHex));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document("nested.id", idHex));
	}

	@Test // DATAMONGO-2221
	public void shouldNotConvertHexStringToObjectIdForRenamedDeeplyNestedIdField() {

		String idHex = new ObjectId().toHexString();
		Query query = new Query(where("nested.deeplyNested.id").is(idHex));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document("nested.deeplyNested.id", idHex));
	}

	@Test // DATAMONGO-2221
	public void shouldNotConvertHexStringToObjectIdForUnresolvablePath() {

		String idHex = new ObjectId().toHexString();
		Query query = new Query(where("nested.unresolvablePath.id").is(idHex));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document("nested.unresolvablePath.id", idHex));
	}

	@Test // DATAMONGO-1849
	public void shouldConvertPropertyWithExplicitTargetType() {

		String script = "if (a > b) a else b";
		Query query = new Query(where("script").is(script));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithExplicitTargetTypes.class));

		assertThat(document).isEqualTo(new org.bson.Document("script", new Code(script)));
	}

	@Test // DATAMONGO-1849
	public void shouldConvertCollectionPropertyWithExplicitTargetType() {

		String script = "if (a > b) a else b";
		Query query = new Query(where("scripts").is(script));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithExplicitTargetTypes.class));

		assertThat(document).isEqualTo(new org.bson.Document("scripts", new Code(script)));
	}

	@Test // DATAMONGO-2339
	public void findByIdUsesMappedIdFieldNameWithUnderscoreCorrectly() {

		org.bson.Document target = mapper.getMappedObject(new org.bson.Document("with_underscore", "id-1"),
				context.getPersistentEntity(WithIdPropertyContainingUnderscore.class));

		assertThat(target).isEqualTo(new org.bson.Document("_id", "id-1"));
	}

	@Test // DATAMONGO-2394
	public void leavesDistanceUntouchedWhenUsingGeoJson() {

		Query query = query(where("geoJsonPoint").near(new GeoJsonPoint(27.987901, 86.9165379)).maxDistance(1000));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));
		assertThat(document).containsEntry("geoJsonPoint.$near.$geometry.type", "Point");
		assertThat(document).containsEntry("geoJsonPoint.$near.$maxDistance", 1000.0D);
	}

	@Document
	public class Foo {
		@Id private ObjectId id;
		EmbeddedClass embedded;

		@Field("my_items") List<EmbeddedClass> listOfItems;
	}

	public class EmbeddedClass {
		public String id;

		@Field("fancy_custom_name") public String customizedField;
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
		List<String> publishers = new ArrayList<>();
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
		DeeplyNestedClassWithExplicitlyRenamedField deeplyNested;
	}

	static class DeeplyNestedClassWithExplicitlyRenamedField {
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

	static class WithExplicitTargetTypes {

		@Field(targetType = FieldType.SCRIPT) //
		String script;

		@Field(targetType = FieldType.SCRIPT) //
		List<String> scripts;
	}

	static class WithIdPropertyContainingUnderscore {
		@Id String with_underscore;
	}
}
