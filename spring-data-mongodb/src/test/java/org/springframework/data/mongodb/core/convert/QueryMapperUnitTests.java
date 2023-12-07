/*
 * Copyright 2011-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.AggregationExpressionCriteria.*;
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
import java.util.TreeMap;

import org.bson.conversions.Bson;
import org.bson.types.Code;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.EvaluationOperators;
import org.springframework.data.mongodb.core.aggregation.EvaluationOperators.Expr;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.mapping.*;
import org.springframework.data.mongodb.core.mapping.FieldName.Type;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.TextQuery;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author David Julia
 */
public class QueryMapperUnitTests {

	private QueryMapper mapper;
	private MongoMappingContext context;
	private MappingMongoConverter converter;

	@BeforeEach
	void beforeEach() {

		MongoCustomConversions conversions = new MongoCustomConversions();
		this.context = new MongoMappingContext();
		this.context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();

		this.mapper = new QueryMapper(converter);
	}

	@Test
	void translatesIdPropertyIntoIdKey() {

		org.bson.Document query = new org.bson.Document("foo", "value");
		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(Sample.class);

		org.bson.Document result = mapper.getMappedObject(query, entity);
		assertThat(result).containsKey("_id");
		assertThat(result).doesNotContainKey("foo");
	}

	@Test
	void convertsStringIntoObjectId() {

		org.bson.Document query = new org.bson.Document("_id", new ObjectId().toString());
		org.bson.Document result = mapper.getMappedObject(query, context.getPersistentEntity(IdWrapper.class));
		assertThat(result.get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test
	void handlesBigIntegerIdsCorrectly() {

		org.bson.Document document = new org.bson.Document("id", new BigInteger("1"));
		org.bson.Document result = mapper.getMappedObject(document, context.getPersistentEntity(IdWrapper.class));
		assertThat(result).containsEntry("_id", "1");
	}

	@Test
	void handlesObjectIdCapableBigIntegerIdsCorrectly() {

		ObjectId id = new ObjectId();
		org.bson.Document document = new org.bson.Document("id", new BigInteger(id.toString(), 16));
		org.bson.Document result = mapper.getMappedObject(document, context.getPersistentEntity(IdWrapper.class));
		assertThat(result).containsEntry("_id", id);
	}

	@Test // DATAMONGO-278
	void translates$NeCorrectly() {

		Criteria criteria = where("foo").ne(new ObjectId().toString());

		org.bson.Document result = mapper.getMappedObject(criteria.getCriteriaObject(),
				context.getPersistentEntity(Sample.class));
		Object object = result.get("_id");
		assertThat(object).isInstanceOf(org.bson.Document.class);
		org.bson.Document document = (org.bson.Document) object;
		assertThat(document.get("$ne")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-326
	void handlesEnumsCorrectly() {
		Query query = query(where("foo").is(Enum.INSTANCE));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		Object object = result.get("foo");
		assertThat(object).isInstanceOf(String.class);
	}

	@Test
	void handlesEnumsInNotEqualCorrectly() {
		Query query = query(where("foo").ne(Enum.INSTANCE));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		Object object = result.get("foo");
		assertThat(object).isInstanceOf(org.bson.Document.class);

		Object ne = ((org.bson.Document) object).get("$ne");
		assertThat(ne).isInstanceOf(String.class).hasToString(Enum.INSTANCE.name());
	}

	@Test
	void handlesEnumsIn$InCorrectly() {

		Query query = query(where("foo").in(Enum.INSTANCE));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		Object object = result.get("foo");
		assertThat(object).isInstanceOf(org.bson.Document.class);

		Object in = ((org.bson.Document) object).get("$in");
		assertThat(in).isInstanceOf(List.class);

		List list = (List) in;
		assertThat(list).hasSize(1);
		assertThat(list.get(0)).isInstanceOf(String.class).hasToString(Enum.INSTANCE.name());
	}

	@Test // DATAMONGO-373
	void handlesNativelyBuiltQueryCorrectly() {

		Bson query = new BasicDBObject(Filters.or(new BasicDBObject("foo", "bar")).toBsonDocument(org.bson.Document.class,
				MongoClientSettings.getDefaultCodecRegistry()));
		mapper.getMappedObject(query, Optional.empty());
	}

	@Test // DATAMONGO-369
	void handlesAllPropertiesIfDocument() {

		org.bson.Document query = new org.bson.Document();
		query.put("foo", new org.bson.Document("$in", Arrays.asList(1, 2)));
		query.put("bar", new Person());

		org.bson.Document result = mapper.getMappedObject(query, Optional.empty());
		assertThat(result).containsKey("bar");
	}

	@Test // DATAMONGO-429
	void transformsArraysCorrectly() {

		Query query = new BasicQuery("{ 'tags' : { '$all' : [ 'green', 'orange']}}");

		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());
		assertThat(result.toJson()).isEqualTo(query.getQueryObject().toJson());
	}

	@Test
	void doesHandleNestedFieldsWithDefaultIdNames() {

		org.bson.Document document = new org.bson.Document("id", new ObjectId().toString());
		document.put("nested", new org.bson.Document("id", new ObjectId().toString()));

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(ClassWithDefaultId.class);

		org.bson.Document result = mapper.getMappedObject(document, entity);
		assertThat(result.get("_id")).isInstanceOf(ObjectId.class);
		assertThat(((org.bson.Document) result.get("nested")).get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-493
	void doesNotTranslateNonIdPropertiesFor$NeCriteria() {

		ObjectId accidentallyAnObjectId = new ObjectId();

		Query query = Query
				.query(Criteria.where("id").is("id_value").and("publishers").ne(accidentallyAnObjectId.toString()));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(UserEntity.class));
		assertThat(document.get("publishers")).isInstanceOf(org.bson.Document.class);

		org.bson.Document publishers = (org.bson.Document) document.get("publishers");
		assertThat(publishers).containsKey("$ne");
		assertThat(publishers.get("$ne")).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-494
	void usesEntityMetadataInOr() {

		Query query = query(new Criteria().orOperator(where("foo").is("bar")));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(Sample.class));

		assertThat(result.keySet()).hasSize(1).containsOnly("$or");

		List<Object> ors = getAsDBList(result, "$or");
		assertThat(ors).hasSize(1);
		org.bson.Document criterias = getAsDocument(ors, 0);
		assertThat(criterias.keySet()).hasSize(1).doesNotContain("foo");
		assertThat(criterias).containsKey("_id");
	}

	@Test
	void translatesPropertyReferenceCorrectly() {

		Query query = query(where("field").is(new CustomizedField()));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));

		assertThat(result).containsKey("foo").hasSize(1);
	}

	@Test
	void translatesNestedPropertyReferenceCorrectly() {

		Query query = query(where("field.field").is(new CustomizedField()));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));

		assertThat(result).containsKey("foo.foo");
		assertThat(result.keySet()).hasSize(1);
	}

	@Test
	void returnsOriginalKeyIfNoPropertyReference() {

		Query query = query(where("bar").is(new CustomizedField()));
		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));

		assertThat(result).containsKey("bar");
		assertThat(result.keySet()).hasSize(1);
	}

	@Test
	void convertsAssociationCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("reference").is(reference));
		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		Object referenceObject = object.get("reference");

		assertThat(referenceObject).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test
	void convertsNestedAssociationCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("withDbRef.reference").is(reference));
		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRefWrapper.class));

		Object referenceObject = object.get("withDbRef.reference");

		assertThat(referenceObject).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test
	void convertsInKeywordCorrectly() {

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
	void correctlyConvertsNullReference() {

		Query query = query(where("reference").is(null));
		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		assertThat(object.get("reference")).isNull();
	}

	@Test // DATAMONGO-629
	void doesNotMapIdIfNoEntityMetadataAvailable() {

		String id = new ObjectId().toString();
		Query query = query(where("id").is(id));

		org.bson.Document object = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		assertThat(object).containsKey("id");
		assertThat(object).containsEntry("id", id);
		assertThat(object).doesNotContainKey("_id");
	}

	@Test // DATAMONGO-677
	void handleMapWithDBRefCorrectly() {

		org.bson.Document mapDocument = new org.bson.Document();
		mapDocument.put("test", new com.mongodb.DBRef("test", "test"));
		org.bson.Document document = new org.bson.Document();
		document.put("mapWithDBRef", mapDocument);

		org.bson.Document mapped = mapper.getMappedObject(document, context.getPersistentEntity(WithMapDBRef.class));

		assertThat(mapped).containsKey("mapWithDBRef");
		assertThat(mapped.get("mapWithDBRef")).isInstanceOf(org.bson.Document.class);
		assertThat(((org.bson.Document) mapped.get("mapWithDBRef"))).containsKey("test");
		assertThat(((org.bson.Document) mapped.get("mapWithDBRef")).get("test")).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test
	void convertsUnderscoreIdValueWithoutMetadata() {

		org.bson.Document document = new org.bson.Document().append("_id", new ObjectId().toString());

		org.bson.Document mapped = mapper.getMappedObject(document, Optional.empty());
		assertThat(mapped).containsKey("_id");
		assertThat(mapped.get("_id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-705
	void convertsDBRefWithExistsQuery() {

		Query query = query(where("reference").exists(false));

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithDBRef.class);
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(), entity);

		org.bson.Document reference = getAsDocument(mappedObject, "reference");
		assertThat(reference).containsKey("$exists");
		assertThat(reference).containsEntry("$exists", false);
	}

	@Test // DATAMONGO-706
	void convertsNestedDBRefsCorrectly() {

		Reference reference = new Reference();
		reference.id = 5L;

		Query query = query(where("someString").is("foo").andOperator(where("reference").in(reference)));

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithDBRef.class);
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(), entity);

		assertThat(mappedObject).containsEntry("someString", "foo");

		List<Object> andClause = getAsDBList(mappedObject, "$and");
		assertThat(andClause).hasSize(1);

		List<Object> inClause = getAsDBList(getAsDocument(getAsDocument(andClause, 0), "reference"), "$in");
		assertThat(inClause).hasSize(1);
		assertThat(inClause.get(0)).isInstanceOf(com.mongodb.DBRef.class);
	}

	@Test // GH-3853
	void convertsDocumentReferenceOnIdPropertyCorrectly() {

		Sample reference = new Sample();
		reference.foo = "s1";

		Query query = query(where("sample").is(reference));
		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDocumentReference.class));

		assertThat(mappedQuery).containsEntry("sample", "s1");
	}

	@Test // GH-4033
	void convertsNestedPathToIdPropertyOfDocumentReferenceCorrectly() {

		Query query = query(where("sample.foo").is("s1"));
		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDocumentReference.class));

		assertThat(mappedQuery).containsEntry("sample", "s1");
	}

	@Test // GH-4033
	void convertsNestedPathToIdPropertyOfDocumentReferenceCorrectlyWhenItShouldBeConvertedToObjectId() {

		ObjectId id = new ObjectId();
		Query query = query(where("sample.foo").is(id.toHexString()));
		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDocumentReference.class));

		assertThat(mappedQuery.get("sample")).satisfies(it -> {

			assertThat(it).isInstanceOf(ObjectId.class);
			assertThat(((ObjectId) it).toHexString()).isEqualTo(id.toHexString());
		});
	}

	@Test // GH-3853
	void convertsListDocumentReferenceOnIdPropertyCorrectly() {

		Sample reference = new Sample();
		reference.foo = "s1";

		Query query = query(where("samples").is(Arrays.asList(reference)));
		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDocumentReference.class));

		assertThat(mappedQuery).containsEntry("samples", Arrays.asList("s1"));
	}

	@Test // GH-3853
	void convertsDocumentReferenceOnNonIdPropertyCorrectly() {

		Customer reference = new Customer();
		reference.id = new ObjectId();
		reference.name = "c1";

		Query query = query(where("customer").is(reference));
		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDocumentReference.class));

		assertThat(mappedQuery).containsEntry("customer", "c1");
	}

	@Test // GH-3853
	void convertsListDocumentReferenceOnNonIdPropertyCorrectly() {

		Customer reference = new Customer();
		reference.id = new ObjectId();
		reference.name = "c1";

		Query query = query(where("customers").is(Arrays.asList(reference)));
		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDocumentReference.class));

		assertThat(mappedQuery).containsEntry("customers", Arrays.asList("c1"));
	}

	@Test // DATAMONGO-752
	void mapsSimpleValuesStartingWith$Correctly() {

		Query query = query(where("myvalue").is("$334"));

		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		assertThat(result.keySet()).hasSize(1);
		assertThat(result).containsEntry("myvalue", "$334");
	}

	@Test // DATAMONGO-752
	void mapsKeywordAsSimpleValuesCorrectly() {

		Query query = query(where("myvalue").is("$center"));

		org.bson.Document result = mapper.getMappedObject(query.getQueryObject(), Optional.empty());

		assertThat(result.keySet()).hasSize(1);
		assertThat(result).containsEntry("myvalue", "$center");
	}

	@Test // DATAMONGO-805
	void shouldExcludeDBRefAssociation() {

		Query query = query(where("someString").is("foo"));
		query.fields().exclude("reference");

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithDBRef.class);
		org.bson.Document queryResult = mapper.getMappedObject(query.getQueryObject(), entity);
		org.bson.Document fieldsResult = mapper.getMappedObject(query.getFieldsObject(), entity);

		assertThat(queryResult).containsEntry("someString", "foo");
		assertThat(fieldsResult).containsEntry("reference", 0);
	}

	@Test // DATAMONGO-686
	void queryMapperShouldNotChangeStateInGivenQueryObjectWhenIdConstrainedByInList() {

		MongoPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(Sample.class);
		String idPropertyName = persistentEntity.getIdProperty().getName();
		org.bson.Document queryObject = query(where(idPropertyName).in("42")).getQueryObject();

		Object idValuesBefore = getAsDocument(queryObject, idPropertyName).get("$in");
		mapper.getMappedObject(queryObject, persistentEntity);
		Object idValuesAfter = getAsDocument(queryObject, idPropertyName).get("$in");

		assertThat(idValuesAfter).isEqualTo(idValuesBefore);
	}

	@Test // DATAMONGO-821
	void queryMapperShouldNotTryToMapDBRefListPropertyIfNestedInsideDocumentWithinDocument() {

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
	void queryMapperShouldNotTryToMapDBRefPropertyIfNestedInsideDocumentWithinDocument() {

		org.bson.Document queryObject = query(
				where("reference").is(new org.bson.Document("$nested", new org.bson.Document("$keys", 0L)))).getQueryObject();

		org.bson.Document mappedObject = mapper.getMappedObject(queryObject, context.getPersistentEntity(WithDBRef.class));
		org.bson.Document referenceObject = getAsDocument(mappedObject, "reference");
		org.bson.Document nestedObject = getAsDocument(referenceObject, "$nested");

		assertThat(nestedObject).isEqualTo(new org.bson.Document("$keys", 0L));
	}

	@Test // DATAMONGO-821
	void queryMapperShouldMapDBRefPropertyIfNestedInDocument() {

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
	void queryMapperShouldBeAbleToProcessQueriesThatIncludeDbRefFields() {

		MongoPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(WithDBRef.class);

		Query qry = query(where("someString").is("abc"));
		qry.fields().include("reference");

		org.bson.Document mappedFields = mapper.getMappedObject(qry.getFieldsObject(), persistentEntity);
		assertThat(mappedFields).isNotNull();
	}

	@Test // DATAMONGO-893
	void classInformationShouldNotBePresentInDocumentUsedInFinderMethods() {

		EmbeddedClass embedded = new EmbeddedClass();
		embedded.id = "1";

		EmbeddedClass embedded2 = new EmbeddedClass();
		embedded2.id = "2";
		Query query = query(where("embedded").in(Arrays.asList(embedded, embedded2)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));
		assertThat(document).isEqualTo("{ \"embedded\" : { \"$in\" : [ { \"_id\" : \"1\"} , { \"_id\" : \"2\"}]}}");
	}

	@Test // DATAMONGO-1406
	void shouldMapQueryForNestedCustomizedPropertiesUsingConfiguredFieldNames() {

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
	void customizedFieldNameShouldBeMappedCorrectlyWhenApplyingSort() {

		Query query = query(where("field").is("bar")).with(Sort.by(Direction.DESC, "field"));
		org.bson.Document document = mapper.getMappedObject(query.getSortObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(document).isEqualTo(new org.bson.Document().append("foo", -1));
	}

	@Test // DATAMONGO-973
	void getMappedFieldsAppendsTextScoreFieldProperlyCorrectlyWhenNotPresent() {

		Query query = new Query();

		org.bson.Document document = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document)
				.isEqualTo(new org.bson.Document().append("score", new org.bson.Document("$meta", "textScore")));
	}

	@Test // DATAMONGO-973
	void getMappedFieldsReplacesTextScoreFieldProperlyCorrectlyWhenPresent() {

		Query query = new Query();
		query.fields().include("textScore");

		org.bson.Document document = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document)
				.isEqualTo(new org.bson.Document().append("score", new org.bson.Document("$meta", "textScore")));
	}

	@Test // DATAMONGO-973
	void getMappedSortAppendsTextScoreProperlyWhenSortedByScore() {

		Query query = new Query().with(Sort.by("textScore"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document)
				.isEqualTo(new org.bson.Document().append("score", new org.bson.Document("$meta", "textScore")));
	}

	@Test // DATAMONGO-973
	void getMappedSortIgnoresTextScoreWhenNotSortedByScore() {

		Query query = new Query().with(Sort.by("id"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithTextScoreProperty.class));

		assertThat(document).isEqualTo(new org.bson.Document().append("_id", 1));
	}

	@Test // DATAMONGO-1070, DATAMONGO-1798
	void mapsIdReferenceToDBRefCorrectly() {

		ObjectId id = new ObjectId();

		org.bson.Document query = new org.bson.Document("reference.id", new com.mongodb.DBRef("reference", id));
		org.bson.Document result = mapper.getMappedObject(query, context.getPersistentEntity(WithDBRef.class));

		assertThat(result).containsKey("reference");
		com.mongodb.DBRef reference = getTypedValue(result, "reference", com.mongodb.DBRef.class);
		assertThat(reference.getId()).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-1050
	void shouldUseExplicitlySetFieldnameForIdPropertyCandidates() {

		Query query = query(where("nested.id").is("bar"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document().append("nested.id", "bar"));
	}

	@Test // DATAMONGO-1050
	void shouldUseExplicitlySetFieldnameForIdPropertyCandidatesUsedInSortClause() {

		Query query = new Query().with(Sort.by("nested.id"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document().append("nested.id", 1));
	}

	@Test // DATAMONGO-1135
	void nearShouldUseGeoJsonRepresentationOnUnmappedProperty() {

		Query query = query(where("foo").near(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("foo.$near.$geometry.type", "Point");
		assertThat(document).containsEntry("foo.$near.$geometry.coordinates.[0]", 100D);
		assertThat(document).containsEntry("foo.$near.$geometry.coordinates.[1]", 50D);
	}

	@Test // DATAMONGO-1135
	void nearShouldUseGeoJsonRepresentationWhenMappingToGoJsonType() {

		Query query = query(where("geoJsonPoint").near(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$near.$geometry.type", "Point");
	}

	@Test // DATAMONGO-1135
	void nearSphereShouldUseGeoJsonRepresentationWhenMappingToGoJsonType() {

		Query query = query(where("geoJsonPoint").nearSphere(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$nearSphere.$geometry.type", "Point");
	}

	@Test // DATAMONGO-1135
	void shouldMapNameCorrectlyForGeoJsonType() {

		Query query = query(where("namedGeoJsonPoint").nearSphere(new GeoJsonPoint(100, 50)));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPointWithNameViaFieldAnnotation.$nearSphere.$geometry.type", "Point");
	}

	@Test // DATAMONGO-1135
	void withinShouldUseGeoJsonPolygonWhenMappingPolygonOn2DSphereIndex() {

		Query query = query(where("geoJsonPoint")
				.within(new GeoJsonPolygon(new Point(0, 0), new Point(100, 100), new Point(100, 0), new Point(0, 0))));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$geoWithin.$geometry.type", "Polygon");
	}

	@Test // DATAMONGO-1134
	void intersectsShouldUseGeoJsonRepresentationCorrectly() {

		Query query = query(where("geoJsonPoint")
				.intersects(new GeoJsonPolygon(new Point(0, 0), new Point(100, 100), new Point(100, 0), new Point(0, 0))));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));

		assertThat(document).containsEntry("geoJsonPoint.$geoIntersects.$geometry.type", "Polygon");
		assertThat(document).containsKey("geoJsonPoint.$geoIntersects.$geometry.coordinates");
	}

	@Test // DATAMONGO-1269
	void mappingShouldRetainNumericMapKey() {

		Query query = query(where("map.1.stringProperty").is("ba'alzamon"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithComplexValueTypeMap.class));

		assertThat(document).containsKey("map.1.stringProperty");
	}

	@Test // GH-3688
	void mappingShouldRetainNestedNumericMapKeys() {

		Query query = query(where("outerMap.1.map.2.stringProperty").is("ba'alzamon"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithIntKeyedMapOfMap.class));

		assertThat(document).containsKey("outerMap.1.map.2.stringProperty");
	}

	@Test // GH-3688
	void mappingShouldAllowSettingEntireNestedNumericKeyedMapValue() {

		Query query = query(where("outerMap.1.map").is(null)); //newEntityWithComplexValueTypeMap()

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithIntKeyedMapOfMap.class));

		assertThat(document).containsKey("outerMap.1.map");
	}

	@Test // DATAMONGO-1269
	void mappingShouldRetainNumericPositionInList() {

		Query query = query(where("list.1.stringProperty").is("ba'alzamon"));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(EntityWithComplexValueTypeList.class));

		assertThat(document).containsKey("list.1.stringProperty");
	}

	@Test // DATAMONGO-1245
	void exampleShouldBeMappedCorrectly() {

		Foo probe = new Foo();
		probe.embedded = new EmbeddedClass();
		probe.embedded.id = "conflux";

		Query query = query(byExample(probe));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));

		assertThat(document).containsEntry("embedded\\._id", "conflux");
	}

	@Test // DATAMONGO-1245
	void exampleShouldBeMappedCorrectlyWhenContainingLegacyPoint() {

		ClassWithGeoTypes probe = new ClassWithGeoTypes();
		probe.legacyPoint = new Point(10D, 20D);

		Query query = query(byExample(probe));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithDBRef.class));

		assertThat(document).containsEntry("legacyPoint.x", 10D);
		assertThat(document).containsEntry("legacyPoint.y", 20D);
	}

	@Test // GH-3544
	void exampleWithCombinedCriteriaShouldBeMappedCorrectly() {

		Foo probe = new Foo();
		probe.embedded = new EmbeddedClass();
		probe.embedded.id = "conflux";

		Query query = query(byExample(probe).and("listOfItems").exists(true));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class));

		assertThat(document).containsEntry("embedded\\._id", "conflux").containsEntry("my_items",
				new org.bson.Document("$exists", true));
	}

	@Test // DATAMONGO-1988
	void mapsStringObjectIdRepresentationToObjectIdWhenReferencingIdProperty() {

		Query query = query(where("sample.foo").is(new ObjectId().toHexString()));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithEmbedded.class));

		assertThat(document.get("sample._id")).isInstanceOf(ObjectId.class);
	}

	@Test // DATAMONGO-1988
	void matchesExactFieldNameToIdProperty() {

		Query query = query(where("sample.iid").is(new ObjectId().toHexString()));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithEmbedded.class));

		assertThat(document.get("sample.iid")).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-1988
	void leavesNonObjectIdStringIdRepresentationUntouchedWhenReferencingIdProperty() {

		Query query = query(where("sample.foo").is("id-1"));
		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithEmbedded.class));

		assertThat(document.get("sample._id")).isInstanceOf(String.class);
	}

	@Test // DATAMONGO-2168
	void getMappedObjectShouldNotMapTypeHint() {

		converter.setTypeMapper(new DefaultMongoTypeMapper("className"));

		org.bson.Document update = new org.bson.Document("className", "foo");
		org.bson.Document mappedObject = mapper.getMappedObject(update, context.getPersistentEntity(UserEntity.class));

		assertThat(mappedObject).containsEntry("className", "foo");
	}

	@Test // DATAMONGO-2168
	void getMappedObjectShouldIgnorePathsLeadingToJavaLangClassProperties/* like Class#getName() */() {

		org.bson.Document update = new org.bson.Document("className", "foo");
		org.bson.Document mappedObject = mapper.getMappedObject(update, context.getPersistentEntity(UserEntity.class));

		assertThat(mappedObject).containsEntry("className", "foo");
	}

	@Test // DATAMONGO-2193
	void shouldNotConvertHexStringToObjectIdForRenamedNestedIdField() {

		String idHex = new ObjectId().toHexString();
		Query query = new Query(where("nested.id").is(idHex));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document("nested.id", idHex));
	}

	@Test // DATAMONGO-2221
	void shouldNotConvertHexStringToObjectIdForRenamedDeeplyNestedIdField() {

		String idHex = new ObjectId().toHexString();
		Query query = new Query(where("nested.deeplyNested.id").is(idHex));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document("nested.deeplyNested.id", idHex));
	}

	@Test // DATAMONGO-2221
	void shouldNotConvertHexStringToObjectIdForUnresolvablePath() {

		String idHex = new ObjectId().toHexString();
		Query query = new Query(where("nested.unresolvablePath.id").is(idHex));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(RootForClassWithExplicitlyRenamedIdField.class));

		assertThat(document).isEqualTo(new org.bson.Document("nested.unresolvablePath.id", idHex));
	}

	@Test // DATAMONGO-1849
	void shouldConvertPropertyWithExplicitTargetType() {

		String script = "if (a > b) a else b";
		Query query = new Query(where("script").is(script));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithExplicitTargetTypes.class));

		assertThat(document).isEqualTo(new org.bson.Document("script", new Code(script)));
	}

	@Test // DATAMONGO-1849
	void shouldConvertCollectionPropertyWithExplicitTargetType() {

		String script = "if (a > b) a else b";
		Query query = new Query(where("scripts").is(script));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithExplicitTargetTypes.class));

		assertThat(document).isEqualTo(new org.bson.Document("scripts", new Code(script)));
	}

	@Test // DATAMONGO-2339
	void findByIdUsesMappedIdFieldNameWithUnderscoreCorrectly() {

		org.bson.Document target = mapper.getMappedObject(new org.bson.Document("with_underscore", "id-1"),
				context.getPersistentEntity(WithIdPropertyContainingUnderscore.class));

		assertThat(target).isEqualTo(new org.bson.Document("_id", "id-1"));
	}

	@Test // DATAMONGO-2394
	void leavesDistanceUntouchedWhenUsingGeoJson() {

		Query query = query(where("geoJsonPoint").near(new GeoJsonPoint(27.987901, 86.9165379)).maxDistance(1000));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(ClassWithGeoTypes.class));
		assertThat(document).containsEntry("geoJsonPoint.$near.$geometry.type", "Point");
		assertThat(document).containsEntry("geoJsonPoint.$near.$maxDistance", 1000.0D);
	}

	@Test // DATAMONGO-2440
	void convertsInWithNonIdFieldAndObjectIdTypeHintCorrectly() {

		String id = new ObjectId().toHexString();
		NonIdFieldWithObjectIdTargetType source = new NonIdFieldWithObjectIdTargetType();

		source.stringAsOid = id;

		org.bson.Document target = mapper.getMappedObject(query(where("stringAsOid").in(id)).getQueryObject(),
				context.getPersistentEntity(NonIdFieldWithObjectIdTargetType.class));
		assertThat(target).isEqualTo(org.bson.Document.parse("{\"stringAsOid\": {\"$in\": [{\"$oid\": \"" + id + "\"}]}}"));
	}

	@Test // DATAMONGO-2440
	void convertsInWithIdFieldAndObjectIdTypeHintCorrectly() {

		String id = new ObjectId().toHexString();
		NonIdFieldWithObjectIdTargetType source = new NonIdFieldWithObjectIdTargetType();

		source.id = id;

		org.bson.Document target = mapper.getMappedObject(query(where("id").in(id)).getQueryObject(),
				context.getPersistentEntity(NonIdFieldWithObjectIdTargetType.class));
		assertThat(target).isEqualTo(org.bson.Document.parse("{\"_id\": {\"$in\": [{\"$oid\": \"" + id + "\"}]}}"));
	}

	@Test // DATAMONGO-2488
	void mapsNestedArrayPathCorrectlyForNonMatchingPath() {

		org.bson.Document target = mapper.getMappedObject(
				query(where("array.$[some_item].nested.$[other_item]").is("value")).getQueryObject(),
				context.getPersistentEntity(Foo.class));

		assertThat(target).isEqualTo(new org.bson.Document("array.$[some_item].nested.$[other_item]", "value"));
	}

	@Test // DATAMONGO-2488
	void mapsNestedArrayPathCorrectlyForObjectTargetArray() {

		org.bson.Document target = mapper.getMappedObject(
				query(where("arrayObj.$[some_item].nested.$[other_item]").is("value")).getQueryObject(),
				context.getPersistentEntity(WithNestedArray.class));

		assertThat(target).isEqualTo(new org.bson.Document("arrayObj.$[some_item].nested.$[other_item]", "value"));
	}

	@Test // DATAMONGO-2488
	void mapsNestedArrayPathCorrectlyForStringTargetArray() {

		org.bson.Document target = mapper.getMappedObject(
				query(where("arrayString.$[some_item].nested.$[other_item]").is("value")).getQueryObject(),
				context.getPersistentEntity(WithNestedArray.class));

		assertThat(target).isEqualTo(new org.bson.Document("arrayString.$[some_item].nested.$[other_item]", "value"));
	}

	@Test // DATAMONGO-2488
	void mapsCustomFieldNamesForNestedArrayPathCorrectly() {

		org.bson.Document target = mapper.getMappedObject(
				query(where("arrayCustomName.$[some_item].nested.$[other_item]").is("value")).getQueryObject(),
				context.getPersistentEntity(WithNestedArray.class));

		assertThat(target).isEqualTo(new org.bson.Document("arrayCustomName.$[some_item].nes-ted.$[other_item]", "value"));
	}

	@Test // DATAMONGO-2502
	void shouldAllowDeeplyNestedPlaceholders() {

		org.bson.Document target = mapper.getMappedObject(
				query(where("level0.$[some_item].arrayObj.$[other_item].nested").is("value")).getQueryObject(),
				context.getPersistentEntity(WithDeepArrayNesting.class));

		assertThat(target).isEqualTo(new org.bson.Document("level0.$[some_item].arrayObj.$[other_item].nested", "value"));
	}

	@Test // DATAMONGO-2502
	void shouldAllowDeeplyNestedPlaceholdersWithCustomName() {

		org.bson.Document target = mapper.getMappedObject(
				query(where("level0.$[some_item].arrayCustomName.$[other_item].nested").is("value")).getQueryObject(),
				context.getPersistentEntity(WithDeepArrayNesting.class));

		assertThat(target)
				.isEqualTo(new org.bson.Document("level0.$[some_item].arrayCustomName.$[other_item].nes-ted", "value"));
	}

	@Test // DATAMONGO-2517
	void shouldParseNestedKeywordWithArgumentMatchingTheSourceEntitiesConstructorCorrectly() {

		TextQuery source = new TextQuery("test");

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WithSingleStringArgConstructor.class));
		assertThat(target).isEqualTo(org.bson.Document.parse("{\"$text\" : { \"$search\" : \"test\" }}"));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnUnwrappedObjectCorrectly() {

		UnwrappableType unwrappableType = new UnwrappableType();
		unwrappableType.stringValue = "test";

		Query source = query(Criteria.where("unwrappedValue").is(unwrappableType));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(target).isEqualTo(new org.bson.Document("stringValue", "test"));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnUnwrappedCorrectly() {

		Query source = query(Criteria.where("unwrappedValue.stringValue").is("test"));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(target).isEqualTo(new org.bson.Document("stringValue", "test"));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnPrefixedUnwrappedCorrectly() {

		Query source = query(Criteria.where("unwrappedValue.stringValue").is("test"));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WithPrefixedUnwrapped.class));

		assertThat(target).isEqualTo(new org.bson.Document("prefix-stringValue", "test"));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnNestedUnwrappedObjectCorrectly() {

		UnwrappableType unwrappableType = new UnwrappableType();
		unwrappableType.stringValue = "test";
		Query source = query(Criteria.where("withUnwrapped.unwrappedValue").is(unwrappableType));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithUnwrapped.class));

		assertThat(target).isEqualTo(new org.bson.Document("withUnwrapped", new org.bson.Document("stringValue", "test")));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnNestedPrefixedUnwrappedObjectCorrectly() {

		UnwrappableType unwrappableType = new UnwrappableType();
		unwrappableType.stringValue = "test";
		Query source = query(Criteria.where("withPrefixedUnwrapped.unwrappedValue").is(unwrappableType));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithUnwrapped.class));

		assertThat(target)
				.isEqualTo(new org.bson.Document("withPrefixedUnwrapped", new org.bson.Document("prefix-stringValue", "test")));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnNestedUnwrappedCorrectly() {

		Query source = query(Criteria.where("withUnwrapped.unwrappedValue.stringValue").is("test"));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithUnwrapped.class));

		assertThat(target).isEqualTo(new org.bson.Document("withUnwrapped.stringValue", "test"));
	}

	@Test // DATAMONGO-1902
	void rendersQueryOnNestedPrefixedUnwrappedCorrectly() {

		Query source = query(Criteria.where("withPrefixedUnwrapped.unwrappedValue.stringValue").is("test"));

		org.bson.Document target = mapper.getMappedObject(source.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithUnwrapped.class));

		assertThat(target).isEqualTo(new org.bson.Document("withPrefixedUnwrapped.prefix-stringValue", "test"));
	}

	@Test // DATAMONGO-1902
	void sortByUnwrappedIsEmpty() {

		Query query = new Query().with(Sort.by("unwrappedValue"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(document).isEqualTo(
				new org.bson.Document("stringValue", 1).append("listValue", 1).append("with-at-field-annotation", 1));
	}

	@Test // DATAMONGO-1902
	void sortByUnwrappedValue() {

		// atFieldAnnotatedValue
		Query query = new Query().with(Sort.by("unwrappedValue.stringValue"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(document).isEqualTo(new org.bson.Document("stringValue", 1));
	}

	@Test // DATAMONGO-1902
	void sortByUnwrappedValueWithFieldAnnotation() {

		Query query = new Query().with(Sort.by("unwrappedValue.atFieldAnnotatedValue"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(document).isEqualTo(new org.bson.Document("with-at-field-annotation", 1));
	}

	@Test // DATAMONGO-1902
	void sortByPrefixedUnwrappedValueWithFieldAnnotation() {

		Query query = new Query().with(Sort.by("unwrappedValue.atFieldAnnotatedValue"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WithPrefixedUnwrapped.class));

		assertThat(document).isEqualTo(new org.bson.Document("prefix-with-at-field-annotation", 1));
	}

	@Test // DATAMONGO-1902
	void sortByNestedUnwrappedValueWithFieldAnnotation() {

		Query query = new Query().with(Sort.by("withUnwrapped.unwrappedValue.atFieldAnnotatedValue"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WrapperAroundWithUnwrapped.class));

		assertThat(document).isEqualTo(new org.bson.Document("withUnwrapped.with-at-field-annotation", 1));
	}

	@Test // DATAMONGO-1902
	void sortByNestedPrefixedUnwrappedValueWithFieldAnnotation() {

		Query query = new Query().with(Sort.by("withPrefixedUnwrapped.unwrappedValue.atFieldAnnotatedValue"));

		org.bson.Document document = mapper.getMappedSort(query.getSortObject(),
				context.getPersistentEntity(WrapperAroundWithUnwrapped.class));

		assertThat(document).isEqualTo(new org.bson.Document("withPrefixedUnwrapped.prefix-with-at-field-annotation", 1));
	}

	@Test // DATAMONGO-1902
	void projectOnUnwrappedUsesFields() {

		Query query = new Query();
		query.fields().include("unwrappedValue");

		org.bson.Document document = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(document).isEqualTo(
				new org.bson.Document("stringValue", 1).append("listValue", 1).append("with-at-field-annotation", 1));
	}

	@Test // DATAMONGO-1902
	void projectOnUnwrappedValue() {

		Query query = new Query();
		query.fields().include("unwrappedValue.stringValue");

		org.bson.Document document = mapper.getMappedFields(query.getFieldsObject(),
				context.getPersistentEntity(WithUnwrapped.class));

		assertThat(document).isEqualTo(new org.bson.Document("stringValue", 1));
	}

	@Test // GH-3601
	void resolvesFieldnameWithUnderscoresCorrectly() {

		Query query = query(where("fieldname_with_underscores").exists(true));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithPropertyUsingUnderscoreInName.class));

		assertThat(document).isEqualTo(new org.bson.Document("fieldname_with_underscores", new org.bson.Document("$exists", true)));
	}

	@Test // GH-3601
	void resolvesMappedFieldnameWithUnderscoresCorrectly() {

		Query query = query(where("renamed_fieldname_with_underscores").exists(true));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WithPropertyUsingUnderscoreInName.class));

		assertThat(document).isEqualTo(new org.bson.Document("renamed", new org.bson.Document("$exists", true)));
	}

	@Test // GH-3601
	void resolvesSimpleNestedFieldnameWithUnderscoresCorrectly() {

		Query query = query(where("simple.fieldname_with_underscores").exists(true));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithPropertyUsingUnderscoreInName.class));

		assertThat(document).isEqualTo(new org.bson.Document("simple.fieldname_with_underscores", new org.bson.Document("$exists", true)));
	}

	@Test // GH-3601
	void resolvesSimpleNestedMappedFieldnameWithUnderscoresCorrectly() {

		Query query = query(where("simple.renamed_fieldname_with_underscores").exists(true));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithPropertyUsingUnderscoreInName.class));

		assertThat(document).isEqualTo(new org.bson.Document("simple.renamed", new org.bson.Document("$exists", true)));
	}

	@Test // GH-3601
	void resolvesFieldNameWithUnderscoreOnNestedFieldnameWithUnderscoresCorrectly() {

		Query query = query(where("double_underscore.fieldname_with_underscores").exists(true));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithPropertyUsingUnderscoreInName.class));

		assertThat(document).isEqualTo(new org.bson.Document("double_underscore.fieldname_with_underscores", new org.bson.Document("$exists", true)));
	}

	@Test // GH-3601
	void resolvesFieldNameWithUnderscoreOnNestedMappedFieldnameWithUnderscoresCorrectly() {

		Query query = query(where("double_underscore.renamed_fieldname_with_underscores").exists(true));

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(WrapperAroundWithPropertyUsingUnderscoreInName.class));

		assertThat(document).isEqualTo(new org.bson.Document("double_underscore.renamed", new org.bson.Document("$exists", true)));
	}

	@Test // GH-3633
	void mapsNullValueForFieldWithCustomTargetType() {

		Query query = query(where("stringAsOid").isNull());

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(NonIdFieldWithObjectIdTargetType.class));

		assertThat(document).isEqualTo(new org.bson.Document("stringAsOid", null));
	}

	@Test // GH-3633
	void mapsNullBsonTypeForFieldWithCustomTargetType() {

		Query query = query(where("stringAsOid").isNullValue());

		org.bson.Document document = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(NonIdFieldWithObjectIdTargetType.class));

		assertThat(document).isEqualTo(new org.bson.Document("stringAsOid", new org.bson.Document("$type", 10)));
	}

	@Test // GH-3635
	void $floorKeywordDoesNotMatch$or$norPattern() {

		Query query = new BasicQuery(" { $expr: { $gt: [ \"$spent\" , { $floor : \"$budget\" } ] } }");
		assertThatNoException()
				.isThrownBy(() -> mapper.getMappedObject(query.getQueryObject(), context.getPersistentEntity(Foo.class)));
	}

	@Test // GH-3659
	void allowsUsingFieldPathsForPropertiesHavingCustomConversionRegistered() {

		Query query = query(where("address.street").is("1007 Mountain Drive"));

		MongoCustomConversions mongoCustomConversions = new MongoCustomConversions(Collections.singletonList(new MyAddressToDocumentConverter()));

		this.context = new MongoMappingContext();
		this.context.setSimpleTypeHolder(mongoCustomConversions.getSimpleTypeHolder());
		this.context.afterPropertiesSet();

		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		this.converter.setCustomConversions(mongoCustomConversions);
		this.converter.afterPropertiesSet();

		this.mapper = new QueryMapper(converter);

		assertThat(mapper.getMappedSort(query.getQueryObject(), context.getPersistentEntity(Customer.class))).isEqualTo(new org.bson.Document("address.street", "1007 Mountain Drive"));
	}

	@Test // GH-3790
	void shouldAcceptExprAsCriteriaDefinition() {

		EvaluationOperators.Expr expr = EvaluationOperators
				.valueOf(ConditionalOperators.ifNull("customizedField").then(true)).expr();

		Query query = query(
				expr.toCriteriaDefinition(new TypeBasedAggregationOperationContext(EmbeddedClass.class, context, mapper)));

		org.bson.Document mappedQuery = mapper.getMappedObject(query.getQueryObject(),
				context.getRequiredPersistentEntity(EmbeddedClass.class));

		assertThat(mappedQuery).isEqualTo("{ $expr : { $ifNull : [\"$fancy_custom_name\", true] } }");
	}

	@Test // GH-3668
	void mapStringIdFieldProjection() {

		org.bson.Document mappedFields = mapper.getMappedFields(new org.bson.Document("id", 1), context.getPersistentEntity(WithStringId.class));
		assertThat(mappedFields).containsEntry("_id", 1);
	}

	@Test // GH-3783
	void retainsId$InWithStringArray() {

		org.bson.Document mappedQuery = mapper.getMappedObject(
				org.bson.Document.parse("{ _id : { $in: [\"5b8bedceb1e0bfc07b008828\"]}}"),
				context.getPersistentEntity(WithExplicitStringId.class));
		assertThat(mappedQuery.get("_id")).isEqualTo(org.bson.Document.parse("{ $in: [\"5b8bedceb1e0bfc07b008828\"]}"));
	}

	@Test // GH-3783
	void mapsId$InInToObjectIds() {

		org.bson.Document mappedQuery = mapper.getMappedObject(
				org.bson.Document.parse("{ _id : { $in: [\"5b8bedceb1e0bfc07b008828\"]}}"),
				context.getPersistentEntity(ClassWithDefaultId.class));
		assertThat(mappedQuery.get("_id"))
				.isEqualTo(org.bson.Document.parse("{ $in: [ {$oid: \"5b8bedceb1e0bfc07b008828\" } ]}"));
	}

	@Test // GH-3596
	void considersValueConverterWhenPresent() {

		org.bson.Document mappedObject = mapper.getMappedObject(new org.bson.Document("text", "value"), context.getPersistentEntity(WithPropertyValueConverter.class));
		assertThat(mappedObject).isEqualTo(new org.bson.Document("text", "eulav"));
	}

	@Test // GH-2750
	void mapsAggregationExpression() {

		Query query = query(whereExpr(ComparisonOperators.valueOf("field").greaterThan("budget")));
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(mappedObject).isEqualTo("{ $expr : { $gt : [ '$foo', '$budget'] } }");
	}

	@Test // GH-2750
	void unwrapsAggregationExpressionExprObjectWrappedInExpressionCriteria() {

		Query query = query(whereExpr(Expr.valueOf(ComparisonOperators.valueOf("field").greaterThan("budget"))));
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(mappedObject).isEqualTo("{ $expr : { $gt : [ '$foo', '$budget'] } }");
	}

	@Test // GH-2750
	void mapsMongoExpressionToFieldsIfItsAnAggregationExpression() {

		Query query = query(expr(ComparisonOperators.valueOf("field").greaterThan("budget")));
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(mappedObject).isEqualTo("{ $expr : { $gt : [ '$foo', '$budget'] } }");
	}

	@Test // GH-2750
	void usageOfMongoExpressionOnCriteriaDoesNotUnwrapAnExprAggregationExpression() {

		Query query = query(expr(Expr.valueOf(ComparisonOperators.valueOf("field").greaterThan("budget"))));
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(mappedObject).isEqualTo("{ $expr : { $expr : { $gt : [ '$foo', '$budget'] } } }");
	}

	@Test // GH-2750
	void usesMongoExpressionDocumentAsIsIfItIsNotAnAggregationExpression() {

		Query query = query(expr(() -> org.bson.Document.parse("{ $gt : [ '$field', '$budget'] }")));
		org.bson.Document mappedObject = mapper.getMappedObject(query.getQueryObject(),
				context.getPersistentEntity(CustomizedField.class));
		assertThat(mappedObject).isEqualTo("{ $expr : { $gt : [ '$field', '$budget'] } }");
	}

	@Test // GH-4080
	void convertsListOfValuesForPropertyThatHasValueConverterButIsNotCollectionLikeOneByOne() {

		org.bson.Document mappedObject = mapper.getMappedObject(query(where("text").in("spring", "data")).getQueryObject(),
				context.getPersistentEntity(WithPropertyValueConverter.class));

		assertThat(mappedObject).isEqualTo("{ 'text' : { $in : ['gnirps', 'atad'] } }");
	}

	@Test // GH-4464
	void usesKeyNameWithDotsIfFieldNameTypeIsKey() {

		org.bson.Document mappedObject = mapper.getMappedObject(query(where("value").is("A")).getQueryObject(), context.getPersistentEntity(WithPropertyHavingDotsInFieldName.class));
		assertThat(mappedObject).isEqualTo("{ 'field.name.with.dots' : 'A' }");
	}

	@Test // GH-4577
	void mappingShouldRetainMapKeyOrder() {

		TreeMap<String, String> sourceMap = new TreeMap<>(Map.of("test1", "123", "test2", "456"));

		org.bson.Document target = mapper.getMappedObject(query(where("simpleMap").is(sourceMap)).getQueryObject(),
				context.getPersistentEntity(WithSimpleMap.class));
		assertThat(target.get("simpleMap", Map.class)).containsExactlyEntriesOf(sourceMap);
	}

	class WithSimpleMap {
		Map<String, String> simpleMap;
	}

	class WithDeepArrayNesting {

		List<WithNestedArray> level0;
	}

	class WithNestedArray {

		List<NestedArrayOfObj> arrayObj;
		List<NestedArrayOfString> arrayString;
		List<NestedArrayOfObjCustomFieldName> arrayCustomName;
	}

	class NestedArrayOfObj {
		List<ArrayObj> nested;
	}

	class NestedArrayOfObjCustomFieldName {

		@Field("nes-ted") List<ArrayObj> nested;
	}

	class NestedArrayOfString {
		List<String> nested;
	}

	class ArrayObj {
		String foo;
	}

	@Document
	class Foo {
		@Id private ObjectId id;
		EmbeddedClass embedded;

		@Field("my_items") List<EmbeddedClass> listOfItems;
	}

	class EmbeddedClass {
		String id;

		@Field("fancy_custom_name") String customizedField;
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

	class WithStringId {

		@MongoId String id;
		String name;
	}

	class WithExplicitStringId {

		@MongoId(FieldType.STRING) String id;
		String name;
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

	static class WithDocumentReference {

		private ObjectId id;

		private String name;

		@DocumentReference(lookup = "{ 'name' : ?#{#target} }")
		private Customer customer;

		@DocumentReference(lookup = "{ 'name' : ?#{#target} }")
		private List<Customer> customers;

		@DocumentReference
		private Sample sample;

		@DocumentReference
		private List<Sample> samples;
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

	static class SimpleEntityWithoutId {

		String stringProperty;
		Integer integerProperty;
	}

	static class EntityWithComplexValueTypeMap {
		Map<Integer, SimpleEntityWithoutId> map;
	}

	static class EntityWithIntKeyedMapOfMap{
		Map<Integer, EntityWithComplexValueTypeMap> outerMap;
	}

	static class EntityWithComplexValueTypeList {
		List<SimpleEntityWithoutId> list;
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

	static class NonIdFieldWithObjectIdTargetType {

		String id;
		@Field(targetType = FieldType.OBJECT_ID) String stringAsOid;
	}

	@Document
	static class WithSingleStringArgConstructor {

		String value;

		public WithSingleStringArgConstructor() {}

		public WithSingleStringArgConstructor(String value) {
			this.value = value;
		}
	}

	static class WrapperAroundWithUnwrapped {

		String someValue;
		WithUnwrapped withUnwrapped;
		WithPrefixedUnwrapped withPrefixedUnwrapped;
	}

	static class WithUnwrapped {

		String id;

		@Unwrapped.Nullable UnwrappableType unwrappedValue;
	}

	static class WithPrefixedUnwrapped {

		String id;

		@Unwrapped.Nullable("prefix-") UnwrappableType unwrappedValue;
	}

	static class UnwrappableType {

		String stringValue;
		List<String> listValue;

		@Field("with-at-field-annotation") //
		String atFieldAnnotatedValue;

		@Transient //
		String transientValue;
	}

	static class WrapperAroundWithPropertyUsingUnderscoreInName {

		WithPropertyUsingUnderscoreInName simple;
		WithPropertyUsingUnderscoreInName double_underscore;
	}

	static class WithPropertyUsingUnderscoreInName {

		String fieldname_with_underscores;

		@Field("renamed")
		String renamed_fieldname_with_underscores;
	}

	@Document
	static class Customer {

		@Id
		private ObjectId id;
		private String name;
		private MyAddress address;
	}

	static class MyAddress {
		private String street;
	}

	static class WithPropertyValueConverter {

		@ValueConverter(ReversingValueConverter.class)
		String text;
	}

	@WritingConverter
	public static class MyAddressToDocumentConverter implements Converter<MyAddress, org.bson.Document> {

		@Override
		public org.bson.Document convert(MyAddress address) {
			org.bson.Document doc = new org.bson.Document();
			doc.put("street", address.street);
			return doc;
		}
	}

	static class WithPropertyHavingDotsInFieldName {

		@Field(name = "field.name.with.dots", nameType = Type.KEY)
		String value;

	}
}
