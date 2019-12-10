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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.core.query.Update.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Wither;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxy;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.AuditingEventListener;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.MongoVersion;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.CloseableIterator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * Integration test for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Thomas Risberg
 * @author Amol Nayak
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Komi Innocent
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Laszlo Csontos
 * @author duozhilin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoTemplateTests {

	@Autowired MongoTemplate template;
	@Autowired MongoDbFactory factory;

	ConfigurableApplicationContext context;
	MongoTemplate mappingTemplate;

	@Rule public MongoVersionRule mongoVersion = MongoVersionRule.any();

	@Autowired
	public void setApplicationContext(ConfigurableApplicationContext context) {

		this.context = context;

		context.addApplicationListener(new PersonWithIdPropertyOfTypeUUIDListener());

		PersistentEntities entities = PersistentEntities.of(template.getConverter().getMappingContext());

		context.addApplicationListener(new AuditingEventListener(() -> new IsNewAwareAuditingHandler(entities)));
	}

	@Autowired
	public void setMongoClient(MongoClient mongo) throws Exception {

		CustomConversions conversions = new MongoCustomConversions(
				Arrays.asList(DateToDateTimeConverter.INSTANCE, DateTimeToDateConverter.INSTANCE));

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(
				new HashSet<>(Arrays.asList(PersonWith_idPropertyOfTypeObjectId.class, PersonWith_idPropertyOfTypeString.class,
						PersonWithIdPropertyOfTypeObjectId.class, PersonWithIdPropertyOfTypeString.class,
						PersonWithIdPropertyOfTypeInteger.class, PersonWithIdPropertyOfTypeBigInteger.class,
						PersonWithIdPropertyOfPrimitiveInt.class, PersonWithIdPropertyOfTypeLong.class,
						PersonWithIdPropertyOfPrimitiveLong.class, PersonWithIdPropertyOfTypeUUID.class)));
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.initialize();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MappingMongoConverter mappingConverter = new MappingMongoConverter(dbRefResolver, mappingContext);
		mappingConverter.setCustomConversions(conversions);
		mappingConverter.afterPropertiesSet();

		this.mappingTemplate = new MongoTemplate(factory, mappingConverter);
	}

	@Before
	public void setUp() {

		cleanDb();

		this.mappingTemplate.setApplicationContext(context);
	}

	@After
	public void cleanUp() {
		cleanDb();
	}

	protected void cleanDb() {
		template.dropCollection(Person.class);
		template.dropCollection(PersonWithAList.class);
		template.dropCollection(PersonWith_idPropertyOfTypeObjectId.class);
		template.dropCollection(PersonWith_idPropertyOfTypeString.class);
		template.dropCollection(PersonWithIdPropertyOfTypeObjectId.class);
		template.dropCollection(PersonWithIdPropertyOfTypeString.class);
		template.dropCollection(PersonWithIdPropertyOfTypeInteger.class);
		template.dropCollection(PersonWithIdPropertyOfTypeBigInteger.class);
		template.dropCollection(PersonWithIdPropertyOfPrimitiveInt.class);
		template.dropCollection(PersonWithIdPropertyOfTypeLong.class);
		template.dropCollection(PersonWithIdPropertyOfPrimitiveLong.class);
		template.dropCollection(PersonWithIdPropertyOfTypeUUID.class);
		template.dropCollection(PersonWithVersionPropertyOfTypeInteger.class);
		template.dropCollection(TestClass.class);
		template.dropCollection(Sample.class);
		template.dropCollection(MyPerson.class);
		template.dropCollection(TypeWithFieldAnnotation.class);
		template.dropCollection(TypeWithDate.class);
		template.dropCollection("collection");
		template.dropCollection("personX");
		template.dropCollection("findandreplace");
		template.dropCollection(Document.class);
		template.dropCollection(ObjectWith3AliasedFields.class);
		template.dropCollection(ObjectWith3AliasedFieldsAndNestedAddress.class);
		template.dropCollection(BaseDoc.class);
		template.dropCollection(ObjectWithEnumValue.class);
		template.dropCollection(DocumentWithCollection.class);
		template.dropCollection(DocumentWithCollectionOfSimpleType.class);
		template.dropCollection(DocumentWithMultipleCollections.class);
		template.dropCollection(DocumentWithNestedCollection.class);
		template.dropCollection(DocumentWithEmbeddedDocumentWithCollection.class);
		template.dropCollection(DocumentWithNestedList.class);
		template.dropCollection(DocumentWithDBRefCollection.class);
		template.dropCollection(SomeContent.class);
		template.dropCollection(SomeTemplate.class);
		template.dropCollection(Address.class);
		template.dropCollection(DocumentWithCollectionOfSamples.class);
		template.dropCollection(WithGeoJson.class);
		template.dropCollection(DocumentWithNestedTypeHavingStringIdProperty.class);
		template.dropCollection(ImmutableAudited.class);
		template.dropCollection(RawStringId.class);
		template.dropCollection(Outer.class);
	}

	@Test
	public void insertsSimpleEntityCorrectly() throws Exception {

		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);

		List<Person> result = template.find(new Query(Criteria.where("_id").is(person.getId())), Person.class);
		assertThat(result.size()).isEqualTo(1);
		assertThat(result).contains(person);
	}

	@Test
	public void bogusUpdateDoesNotTriggerException() throws Exception {

		MongoTemplate mongoTemplate = new MongoTemplate(factory);
		mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person person = new Person("Oliver2");
		person.setAge(25);
		mongoTemplate.insert(person);

		Query q = new Query(Criteria.where("BOGUS").gt(22));
		Update u = new Update().set("firstName", "Sven");
		mongoTemplate.updateFirst(q, u, Person.class);
	}

	@Test // DATAMONGO-480
	public void throwsExceptionForDuplicateIds() {

		MongoTemplate template = new MongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		template.insert(person);

		try {
			template.insert(person);
			fail("Expected DataIntegrityViolationException!");
		} catch (DataIntegrityViolationException e) {
			assertThat(e.getMessage()).contains("E11000 duplicate key error");
		}
	}

	@Test // DATAMONGO-480, DATAMONGO-799
	public void throwsExceptionForUpdateWithInvalidPushOperator() {

		MongoTemplate template = new MongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		template.insert(person);

		Query query = new Query(Criteria.where("firstName").is("Amol"));
		Update upd = new Update().push("age", 29);

		assertThatExceptionOfType(DataIntegrityViolationException.class)
				.isThrownBy(() -> template.updateFirst(query, upd, Person.class)).withMessageContaining("array")
				.withMessageContaining("age");
	}

	@Test // DATAMONGO-480
	public void throwsExceptionForIndexViolationIfConfigured() {

		MongoTemplate template = new MongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
		template.indexOps(Person.class).ensureIndex(new Index().on("firstName", Direction.DESC).unique());

		Person person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		template.save(person);

		person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		try {
			template.save(person);
			fail("Expected DataIntegrityViolationException!");
		} catch (DataIntegrityViolationException e) {
			assertThat(e.getMessage()).contains("E11000 duplicate key error");
		}
	}

	@Test // DATAMONGO-480
	public void rejectsDuplicateIdInInsertAll() {

		MongoTemplate template = new MongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		List<Person> records = new ArrayList<>();
		records.add(person);
		records.add(person);

		assertThatExceptionOfType(DataIntegrityViolationException.class).isThrownBy(() -> template.insertAll(records))
				.withMessageContaining("E11000 duplicate key error");
	}

	@Test // DATAMONGO-1687
	public void createCappedCollection() {

		template.createCollection(Person.class, CollectionOptions.empty().capped().size(1000).maxDocuments(1000));

		org.bson.Document collectionOptions = getCollectionInfo(template.getCollectionName(Person.class)).get("options",
				org.bson.Document.class);
		assertThat(collectionOptions.get("capped")).isEqualTo(true);
	}

	private org.bson.Document getCollectionInfo(String collectionName) {

		return template.execute(db -> {

			org.bson.Document result = db.runCommand(new org.bson.Document().append("listCollections", 1).append("filter",
					new org.bson.Document("name", collectionName)));
			return (org.bson.Document) result.get("cursor", org.bson.Document.class).get("firstBatch", List.class).get(0);
		});
	}

	@Test
	public void testEnsureIndex() throws Exception {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1);
		Person p2 = new Person("Sven");
		p2.setAge(40);
		template.insert(p2);

		template.indexOps(Person.class).ensureIndex(new Index().on("age", Direction.DESC).unique());

		MongoCollection<org.bson.Document> coll = template.getCollection(template.getCollectionName(Person.class));
		List<org.bson.Document> indexInfo = new ArrayList<>();
		coll.listIndexes().into(indexInfo);

		assertThat(indexInfo.size()).isEqualTo(2);
		Object indexKey = null;
		boolean unique = false;
		for (org.bson.Document ix : indexInfo) {

			if ("age_-1".equals(ix.get("name"))) {
				indexKey = ix.get("key");
				unique = (Boolean) ix.get("unique");
				assertThat(ix.get("dropDups")).isNull();
			}
		}
		assertThat(((org.bson.Document) indexKey)).containsEntry("age", -1);
		assertThat(unique).isTrue();

		List<IndexInfo> indexInfoList = template.indexOps(Person.class).getIndexInfo();

		assertThat(indexInfoList.size()).isEqualTo(2);
		IndexInfo ii = indexInfoList.get(1);
		assertThat(ii.isUnique()).isTrue();
		assertThat(ii.isSparse()).isFalse();

		List<IndexField> indexFields = ii.getIndexFields();
		IndexField field = indexFields.get(0);

		assertThat(field).isEqualTo(IndexField.create("age", Direction.DESC));
	}

	@Test // DATAMONGO-746, DATAMONGO-2264
	public void testReadIndexInfoForIndicesCreatedViaMongoShellCommands() throws Exception {

		template.indexOps(Person.class).dropAllIndexes();

		assertThat(template.indexOps(Person.class).getIndexInfo().isEmpty()).isTrue();

		factory.getMongoDatabase().getCollection(template.getCollectionName(Person.class))
				.createIndex(new org.bson.Document("age", -1), new IndexOptions().name("age_-1").unique(true).sparse(true));

		ListIndexesIterable<org.bson.Document> indexInfo = template.getCollection(template.getCollectionName(Person.class))
				.listIndexes();
		org.bson.Document indexKey = null;
		boolean unique = false;

		MongoCursor<org.bson.Document> cursor = indexInfo.iterator();

		while (cursor.hasNext()) {

			org.bson.Document ix = cursor.next();

			if ("age_-1".equals(ix.get("name"))) {
				indexKey = (org.bson.Document) ix.get("key");
				unique = (Boolean) ix.get("unique");
			}
		}

		assertThat(indexKey).containsEntry("age", -1);
		assertThat(unique).isTrue();

		IndexInfo info = template.indexOps(Person.class).getIndexInfo().get(1);
		assertThat(info.isUnique()).isTrue();
		assertThat(info.isSparse()).isTrue();

		List<IndexField> indexFields = info.getIndexFields();
		IndexField field = indexFields.get(0);

		assertThat(field).isEqualTo(IndexField.create("age", Direction.DESC));
	}

	@Test
	public void testProperHandlingOfDifferentIdTypesWithMappingMongoConverter() throws Exception {
		testProperHandlingOfDifferentIdTypes(this.mappingTemplate);
	}

	private void testProperHandlingOfDifferentIdTypes(MongoTemplate mongoTemplate) throws Exception {

		// String id - generated
		PersonWithIdPropertyOfTypeString p1 = new PersonWithIdPropertyOfTypeString();
		p1.setFirstName("Sven_1");
		p1.setAge(22);
		// insert
		mongoTemplate.insert(p1);
		// also try save
		mongoTemplate.save(p1);
		assertThat(p1.getId()).isNotNull();
		PersonWithIdPropertyOfTypeString p1q = mongoTemplate.findOne(new Query(where("id").is(p1.getId())),
				PersonWithIdPropertyOfTypeString.class);
		assertThat(p1q).isNotNull();
		assertThat(p1q.getId()).isEqualTo(p1.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeString.class, 1);

		// String id - provided
		PersonWithIdPropertyOfTypeString p2 = new PersonWithIdPropertyOfTypeString();
		p2.setFirstName("Sven_2");
		p2.setAge(22);
		p2.setId("TWO");
		// insert
		mongoTemplate.insert(p2);
		// also try save
		mongoTemplate.save(p2);
		assertThat(p2.getId()).isNotNull();
		PersonWithIdPropertyOfTypeString p2q = mongoTemplate.findOne(new Query(where("id").is(p2.getId())),
				PersonWithIdPropertyOfTypeString.class);
		assertThat(p2q).isNotNull();
		assertThat(p2q.getId()).isEqualTo(p2.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeString.class, 2);

		// String _id - generated
		PersonWith_idPropertyOfTypeString p3 = new PersonWith_idPropertyOfTypeString();
		p3.setFirstName("Sven_3");
		p3.setAge(22);
		// insert
		mongoTemplate.insert(p3);
		// also try save
		mongoTemplate.save(p3);
		assertThat(p3.get_id()).isNotNull();
		PersonWith_idPropertyOfTypeString p3q = mongoTemplate.findOne(new Query(where("_id").is(p3.get_id())),
				PersonWith_idPropertyOfTypeString.class);
		assertThat(p3q).isNotNull();
		assertThat(p3q.get_id()).isEqualTo(p3.get_id());
		checkCollectionContents(PersonWith_idPropertyOfTypeString.class, 1);

		// String _id - provided
		PersonWith_idPropertyOfTypeString p4 = new PersonWith_idPropertyOfTypeString();
		p4.setFirstName("Sven_4");
		p4.setAge(22);
		p4.set_id("FOUR");
		// insert
		mongoTemplate.insert(p4);
		// also try save
		mongoTemplate.save(p4);
		assertThat(p4.get_id()).isNotNull();
		PersonWith_idPropertyOfTypeString p4q = mongoTemplate.findOne(new Query(where("_id").is(p4.get_id())),
				PersonWith_idPropertyOfTypeString.class);
		assertThat(p4q).isNotNull();
		assertThat(p4q.get_id()).isEqualTo(p4.get_id());
		checkCollectionContents(PersonWith_idPropertyOfTypeString.class, 2);

		// ObjectId id - generated
		PersonWithIdPropertyOfTypeObjectId p5 = new PersonWithIdPropertyOfTypeObjectId();
		p5.setFirstName("Sven_5");
		p5.setAge(22);
		// insert
		mongoTemplate.insert(p5);
		// also try save
		mongoTemplate.save(p5);
		assertThat(p5.getId()).isNotNull();
		PersonWithIdPropertyOfTypeObjectId p5q = mongoTemplate.findOne(new Query(where("id").is(p5.getId())),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p5q).isNotNull();
		assertThat(p5q.getId()).isEqualTo(p5.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeObjectId.class, 1);

		// ObjectId id - provided
		PersonWithIdPropertyOfTypeObjectId p6 = new PersonWithIdPropertyOfTypeObjectId();
		p6.setFirstName("Sven_6");
		p6.setAge(22);
		p6.setId(new ObjectId());
		// insert
		mongoTemplate.insert(p6);
		// also try save
		mongoTemplate.save(p6);
		assertThat(p6.getId()).isNotNull();
		PersonWithIdPropertyOfTypeObjectId p6q = mongoTemplate.findOne(new Query(where("id").is(p6.getId())),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p6q).isNotNull();
		assertThat(p6q.getId()).isEqualTo(p6.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeObjectId.class, 2);

		// ObjectId _id - generated
		PersonWith_idPropertyOfTypeObjectId p7 = new PersonWith_idPropertyOfTypeObjectId();
		p7.setFirstName("Sven_7");
		p7.setAge(22);
		// insert
		mongoTemplate.insert(p7);
		// also try save
		mongoTemplate.save(p7);
		assertThat(p7.get_id()).isNotNull();
		PersonWith_idPropertyOfTypeObjectId p7q = mongoTemplate.findOne(new Query(where("_id").is(p7.get_id())),
				PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p7q).isNotNull();
		assertThat(p7q.get_id()).isEqualTo(p7.get_id());
		checkCollectionContents(PersonWith_idPropertyOfTypeObjectId.class, 1);

		// ObjectId _id - provided
		PersonWith_idPropertyOfTypeObjectId p8 = new PersonWith_idPropertyOfTypeObjectId();
		p8.setFirstName("Sven_8");
		p8.setAge(22);
		p8.set_id(new ObjectId());
		// insert
		mongoTemplate.insert(p8);
		// also try save
		mongoTemplate.save(p8);
		assertThat(p8.get_id()).isNotNull();
		PersonWith_idPropertyOfTypeObjectId p8q = mongoTemplate.findOne(new Query(where("_id").is(p8.get_id())),
				PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p8q).isNotNull();
		assertThat(p8q.get_id()).isEqualTo(p8.get_id());
		checkCollectionContents(PersonWith_idPropertyOfTypeObjectId.class, 2);

		// Integer id - provided
		PersonWithIdPropertyOfTypeInteger p9 = new PersonWithIdPropertyOfTypeInteger();
		p9.setFirstName("Sven_9");
		p9.setAge(22);
		p9.setId(Integer.valueOf(12345));
		// insert
		mongoTemplate.insert(p9);
		// also try save
		mongoTemplate.save(p9);
		assertThat(p9.getId()).isNotNull();
		PersonWithIdPropertyOfTypeInteger p9q = mongoTemplate.findOne(new Query(where("id").in(p9.getId())),
				PersonWithIdPropertyOfTypeInteger.class);
		assertThat(p9q).isNotNull();
		assertThat(p9q.getId()).isEqualTo(p9.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeInteger.class, 1);

		// DATAMONGO-602
		// BigInteger id - provided
		PersonWithIdPropertyOfTypeBigInteger p9bi = new PersonWithIdPropertyOfTypeBigInteger();
		p9bi.setFirstName("Sven_9bi");
		p9bi.setAge(22);
		p9bi.setId(BigInteger.valueOf(12345));
		// insert
		mongoTemplate.insert(p9bi);
		// also try save
		mongoTemplate.save(p9bi);
		assertThat(p9bi.getId()).isNotNull();
		PersonWithIdPropertyOfTypeBigInteger p9qbi = mongoTemplate.findOne(new Query(where("id").in(p9bi.getId())),
				PersonWithIdPropertyOfTypeBigInteger.class);
		assertThat(p9qbi).isNotNull();
		assertThat(p9qbi.getId()).isEqualTo(p9bi.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeBigInteger.class, 1);

		// int id - provided
		PersonWithIdPropertyOfPrimitiveInt p10 = new PersonWithIdPropertyOfPrimitiveInt();
		p10.setFirstName("Sven_10");
		p10.setAge(22);
		p10.setId(12345);
		// insert
		mongoTemplate.insert(p10);
		// also try save
		mongoTemplate.save(p10);
		assertThat(p10.getId()).isNotNull();
		PersonWithIdPropertyOfPrimitiveInt p10q = mongoTemplate.findOne(new Query(where("id").in(p10.getId())),
				PersonWithIdPropertyOfPrimitiveInt.class);
		assertThat(p10q).isNotNull();
		assertThat(p10q.getId()).isEqualTo(p10.getId());
		checkCollectionContents(PersonWithIdPropertyOfPrimitiveInt.class, 1);

		// Long id - provided
		PersonWithIdPropertyOfTypeLong p11 = new PersonWithIdPropertyOfTypeLong();
		p11.setFirstName("Sven_9");
		p11.setAge(22);
		p11.setId(Long.valueOf(12345L));
		// insert
		mongoTemplate.insert(p11);
		// also try save
		mongoTemplate.save(p11);
		assertThat(p11.getId()).isNotNull();
		PersonWithIdPropertyOfTypeLong p11q = mongoTemplate.findOne(new Query(where("id").in(p11.getId())),
				PersonWithIdPropertyOfTypeLong.class);
		assertThat(p11q).isNotNull();
		assertThat(p11q.getId()).isEqualTo(p11.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeLong.class, 1);

		// long id - provided
		PersonWithIdPropertyOfPrimitiveLong p12 = new PersonWithIdPropertyOfPrimitiveLong();
		p12.setFirstName("Sven_10");
		p12.setAge(22);
		p12.setId(12345L);
		// insert
		mongoTemplate.insert(p12);
		// also try save
		mongoTemplate.save(p12);
		assertThat(p12.getId()).isNotNull();
		PersonWithIdPropertyOfPrimitiveLong p12q = mongoTemplate.findOne(new Query(where("id").in(p12.getId())),
				PersonWithIdPropertyOfPrimitiveLong.class);
		assertThat(p12q).isNotNull();
		assertThat(p12q.getId()).isEqualTo(p12.getId());
		checkCollectionContents(PersonWithIdPropertyOfPrimitiveLong.class, 1);

		// DATAMONGO-1617
		// UUID id - provided
		PersonWithIdPropertyOfTypeUUID p13 = new PersonWithIdPropertyOfTypeUUID();
		p13.setFirstName("Sven_10");
		p13.setAge(22);
		// insert
		mongoTemplate.insert(p13);
		// also try save
		mongoTemplate.save(p13);
		assertThat(p13.getId()).isNotNull();
		PersonWithIdPropertyOfTypeUUID p13q = mongoTemplate.findOne(new Query(where("id").in(p13.getId())),
				PersonWithIdPropertyOfTypeUUID.class);
		assertThat(p13q).isNotNull();
		assertThat(p13q.getId()).isEqualTo(p13.getId());
		checkCollectionContents(PersonWithIdPropertyOfTypeUUID.class, 1);
	}

	private void checkCollectionContents(Class<?> entityClass, int count) {
		assertThat(template.findAll(entityClass).size()).isEqualTo(count);
	}

	@Test // DATAMONGO-234
	public void testFindAndUpdate() {

		template.insert(new Person("Tom", 21));
		template.insert(new Person("Dick", 22));
		template.insert(new Person("Harry", 23));

		Query query = new Query(Criteria.where("firstName").is("Harry"));
		Update update = new Update().inc("age", 1);
		Person p = template.findAndModify(query, update, Person.class); // return old
		assertThat(p.getFirstName()).isEqualTo("Harry");
		assertThat(p.getAge()).isEqualTo(23);
		p = template.findOne(query, Person.class);
		assertThat(p.getAge()).isEqualTo(24);

		p = template.findAndModify(query, update, Person.class, "person");
		assertThat(p.getAge()).isEqualTo(24);
		p = template.findOne(query, Person.class);
		assertThat(p.getAge()).isEqualTo(25);

		p = template.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), Person.class);
		assertThat(p.getAge()).isEqualTo(26);

		p = template.findAndModify(query, update, new FindAndModifyOptions(), Person.class, "person");
		assertThat(p.getAge()).isEqualTo(26);
		p = template.findOne(query, Person.class);
		assertThat(p.getAge()).isEqualTo(27);

		Query query2 = new Query(Criteria.where("firstName").is("Mary"));
		p = template.findAndModify(query2, update, new FindAndModifyOptions().returnNew(true).upsert(true), Person.class);
		assertThat(p.getFirstName()).isEqualTo("Mary");
		assertThat(p.getAge()).isEqualTo(1);

	}

	@Test
	public void testFindAndUpdateUpsert() {
		template.insert(new Person("Tom", 21));
		template.insert(new Person("Dick", 22));
		Query query = new Query(Criteria.where("firstName").is("Harry"));
		Update update = new Update().set("age", 23);
		Person p = template.findAndModify(query, update, new FindAndModifyOptions().upsert(true).returnNew(true),
				Person.class);
		assertThat(p.getFirstName()).isEqualTo("Harry");
		assertThat(p.getAge()).isEqualTo(23);
	}

	@Test
	public void testFindAndRemove() throws Exception {

		Message m1 = new Message("Hello Spring");
		template.insert(m1);
		Message m2 = new Message("Hello Mongo");
		template.insert(m2);

		Query q = new Query(Criteria.where("text").regex("^Hello.*"));
		Message found1 = template.findAndRemove(q, Message.class);
		Message found2 = template.findAndRemove(q, Message.class);

		Message notFound = template.findAndRemove(q, Message.class);
		assertThat(found1).isNotNull();
		assertThat(found2).isNotNull();
		assertThat(notFound).isNull();
	}

	@Test // DATAMONGO-1761
	public void testDistinct() {

		Address address1 = new Address();
		address1.state = "PA";
		address1.city = "Philadelphia";

		Address address2 = new Address();
		address2.state = "PA";
		address2.city = " New York";

		MyPerson person1 = new MyPerson();
		person1.name = "Ben";
		person1.address = address1;

		MyPerson person2 = new MyPerson();
		person2.name = "Eric";
		person2.address = address2;

		template.save(person1);
		template.save(person2);

		assertThat(template.findDistinct("name", MyPerson.class, String.class)).containsExactlyInAnyOrder(person1.getName(),
				person2.getName());
		assertThat(template.findDistinct(new BasicQuery("{'address.state' : 'PA'}"), "name", MyPerson.class, String.class))
				.containsExactlyInAnyOrder(person1.getName(), person2.getName());
		assertThat(template.findDistinct(new BasicQuery("{'address.state' : 'PA'}"), "name",
				template.getCollectionName(MyPerson.class), MyPerson.class, String.class))
						.containsExactlyInAnyOrder(person1.getName(), person2.getName());
	}

	@Test // DATAMONGO-1761
	public void testDistinctCovertsResultToPropertyTargetTypeCorrectly() {

		template.insert(new Person("garvin"));

		assertThat(template.findDistinct("firstName", Person.class, Object.class)).allSatisfy(String.class::isInstance);
	}

	@Test // DATAMONGO-1761
	public void testDistinctResolvesDbRefsCorrectly() {

		SomeContent content1 = new SomeContent();
		content1.text = "content-1";

		SomeContent content2 = new SomeContent();
		content2.text = "content-2";

		template.save(content1);
		template.save(content2);

		SomeTemplate t1 = new SomeTemplate();
		t1.content = content1;

		SomeTemplate t2 = new SomeTemplate();
		t2.content = content2;

		SomeTemplate t3 = new SomeTemplate();
		t3.content = content2;

		template.insert(t1);
		template.insert(t2);
		template.insert(t3);

		assertThat(template.findDistinct("content", SomeTemplate.class, SomeContent.class))
				.containsExactlyInAnyOrder(content1, content2);
	}

	@Test
	public void testUsingAnInQueryWithObjectId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		Query q3 = new Query(Criteria.where("id").in(p3.getId()));
		List<PersonWithIdPropertyOfTypeObjectId> results3 = template.find(q3, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(2);
		assertThat(results3.size()).isEqualTo(1);
	}

	@Test
	public void testUsingAnInQueryWithStringId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeString.class);

		PersonWithIdPropertyOfTypeString p1 = new PersonWithIdPropertyOfTypeString();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeString p2 = new PersonWithIdPropertyOfTypeString();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeString p3 = new PersonWithIdPropertyOfTypeString();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeString p4 = new PersonWithIdPropertyOfTypeString();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeString> results1 = template.find(q1, PersonWithIdPropertyOfTypeString.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeString> results2 = template.find(q2, PersonWithIdPropertyOfTypeString.class);
		Query q3 = new Query(Criteria.where("id").in(p3.getId(), p4.getId()));
		List<PersonWithIdPropertyOfTypeString> results3 = template.find(q3, PersonWithIdPropertyOfTypeString.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(2);
		assertThat(results3.size()).isEqualTo(2);
	}

	@Test
	public void testUsingAnInQueryWithLongId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeLong.class);

		PersonWithIdPropertyOfTypeLong p1 = new PersonWithIdPropertyOfTypeLong();
		p1.setFirstName("Sven");
		p1.setAge(11);
		p1.setId(1001L);
		template.insert(p1);
		PersonWithIdPropertyOfTypeLong p2 = new PersonWithIdPropertyOfTypeLong();
		p2.setFirstName("Mary");
		p2.setAge(21);
		p2.setId(1002L);
		template.insert(p2);
		PersonWithIdPropertyOfTypeLong p3 = new PersonWithIdPropertyOfTypeLong();
		p3.setFirstName("Ann");
		p3.setAge(31);
		p3.setId(1003L);
		template.insert(p3);
		PersonWithIdPropertyOfTypeLong p4 = new PersonWithIdPropertyOfTypeLong();
		p4.setFirstName("John");
		p4.setAge(41);
		p4.setId(1004L);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeLong> results1 = template.find(q1, PersonWithIdPropertyOfTypeLong.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeLong> results2 = template.find(q2, PersonWithIdPropertyOfTypeLong.class);
		Query q3 = new Query(Criteria.where("id").in(1001L, 1004L));
		List<PersonWithIdPropertyOfTypeLong> results3 = template.find(q3, PersonWithIdPropertyOfTypeLong.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(2);
		assertThat(results3.size()).isEqualTo(2);
	}

	@Test // DATAMONGO-602
	public void testUsingAnInQueryWithBigIntegerId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeBigInteger.class);

		PersonWithIdPropertyOfTypeBigInteger p1 = new PersonWithIdPropertyOfTypeBigInteger();
		p1.setFirstName("Sven");
		p1.setAge(11);
		p1.setId(new BigInteger("2666666666666666665069473312490162649510603601"));
		template.insert(p1);
		PersonWithIdPropertyOfTypeBigInteger p2 = new PersonWithIdPropertyOfTypeBigInteger();
		p2.setFirstName("Mary");
		p2.setAge(21);
		p2.setId(new BigInteger("2666666666666666665069473312490162649510603602"));
		template.insert(p2);
		PersonWithIdPropertyOfTypeBigInteger p3 = new PersonWithIdPropertyOfTypeBigInteger();
		p3.setFirstName("Ann");
		p3.setAge(31);
		p3.setId(new BigInteger("2666666666666666665069473312490162649510603603"));
		template.insert(p3);
		PersonWithIdPropertyOfTypeBigInteger p4 = new PersonWithIdPropertyOfTypeBigInteger();
		p4.setFirstName("John");
		p4.setAge(41);
		p4.setId(new BigInteger("2666666666666666665069473312490162649510603604"));
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeBigInteger> results1 = template.find(q1, PersonWithIdPropertyOfTypeBigInteger.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeBigInteger> results2 = template.find(q2, PersonWithIdPropertyOfTypeBigInteger.class);
		Query q3 = new Query(Criteria.where("id").in(new BigInteger("2666666666666666665069473312490162649510603601"),
				new BigInteger("2666666666666666665069473312490162649510603604")));
		List<PersonWithIdPropertyOfTypeBigInteger> results3 = template.find(q3, PersonWithIdPropertyOfTypeBigInteger.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(2);
		assertThat(results3.size()).isEqualTo(2);
	}

	@Test
	public void testUsingAnInQueryWithPrimitiveIntId() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfPrimitiveInt.class);

		PersonWithIdPropertyOfPrimitiveInt p1 = new PersonWithIdPropertyOfPrimitiveInt();
		p1.setFirstName("Sven");
		p1.setAge(11);
		p1.setId(1001);
		template.insert(p1);
		PersonWithIdPropertyOfPrimitiveInt p2 = new PersonWithIdPropertyOfPrimitiveInt();
		p2.setFirstName("Mary");
		p2.setAge(21);
		p2.setId(1002);
		template.insert(p2);
		PersonWithIdPropertyOfPrimitiveInt p3 = new PersonWithIdPropertyOfPrimitiveInt();
		p3.setFirstName("Ann");
		p3.setAge(31);
		p3.setId(1003);
		template.insert(p3);
		PersonWithIdPropertyOfPrimitiveInt p4 = new PersonWithIdPropertyOfPrimitiveInt();
		p4.setFirstName("John");
		p4.setAge(41);
		p4.setId(1004);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfPrimitiveInt> results1 = template.find(q1, PersonWithIdPropertyOfPrimitiveInt.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfPrimitiveInt> results2 = template.find(q2, PersonWithIdPropertyOfPrimitiveInt.class);
		Query q3 = new Query(Criteria.where("id").in(1001, 1003));
		List<PersonWithIdPropertyOfPrimitiveInt> results3 = template.find(q3, PersonWithIdPropertyOfPrimitiveInt.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(2);
		assertThat(results3.size()).isEqualTo(2);
	}

	@Test
	public void testUsingInQueryWithList() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		List<Integer> l1 = new ArrayList<>();
		l1.add(11);
		l1.add(21);
		l1.add(41);
		Query q1 = new Query(Criteria.where("age").in(l1));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("age").in(l1.toArray()));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(3);
		try {
			List<Integer> l2 = new ArrayList<>();
			l2.add(31);
			Query q3 = new Query(Criteria.where("age").in(l1, l2));
			template.find(q3, PersonWithIdPropertyOfTypeObjectId.class);
			fail("Should have trown an InvalidDocumentStoreApiUsageException");
		} catch (InvalidMongoDbApiUsageException e) {}
	}

	@Test
	public void testUsingRegexQueryWithOptions() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("samantha");
		p4.setAge(41);
		template.insert(p4);

		Query q1 = new Query(Criteria.where("firstName").regex("S.*"));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("firstName").regex("S.*", "i"));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size()).isEqualTo(1);
		assertThat(results2.size()).isEqualTo(2);
	}

	@Test
	public void testUsingAnOrQuery() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);
		PersonWithIdPropertyOfTypeObjectId p3 = new PersonWithIdPropertyOfTypeObjectId();
		p3.setFirstName("Ann");
		p3.setAge(31);
		template.insert(p3);
		PersonWithIdPropertyOfTypeObjectId p4 = new PersonWithIdPropertyOfTypeObjectId();
		p4.setFirstName("John");
		p4.setAge(41);
		template.insert(p4);

		Query orQuery = new Query(new Criteria().orOperator(where("age").in(11, 21), where("age").is(31)));
		List<PersonWithIdPropertyOfTypeObjectId> results = template.find(orQuery, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results.size()).isEqualTo(3);
		for (PersonWithIdPropertyOfTypeObjectId p : results) {
			assertThat(p.getAge()).isIn(11, 21, 31);
		}
	}

	@Test
	public void testUsingUpdateWithMultipleSet() throws Exception {

		template.remove(new Query(), PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven");
		p1.setAge(11);
		template.insert(p1);
		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Mary");
		p2.setAge(21);
		template.insert(p2);

		Update u = new Update().set("firstName", "Bob").set("age", 10);

		UpdateResult wr = template.updateMulti(new Query(), u, PersonWithIdPropertyOfTypeObjectId.class);

		if (wr.wasAcknowledged()) {
			assertThat(wr.getModifiedCount()).isEqualTo(2L);
		}

		Query q1 = new Query(Criteria.where("age").in(11, 21));
		List<PersonWithIdPropertyOfTypeObjectId> r1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(r1.size()).isEqualTo(0);
		Query q2 = new Query(Criteria.where("age").is(10));
		List<PersonWithIdPropertyOfTypeObjectId> r2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(r2.size()).isEqualTo(2);
		for (PersonWithIdPropertyOfTypeObjectId p : r2) {
			assertThat(p.getAge()).isEqualTo(10);
			assertThat(p.getFirstName()).isEqualTo("Bob");
		}
	}

	@Test
	public void testRemovingDocument() throws Exception {

		PersonWithIdPropertyOfTypeObjectId p1 = new PersonWithIdPropertyOfTypeObjectId();
		p1.setFirstName("Sven_to_be_removed");
		p1.setAge(51);
		template.insert(p1);

		Query q1 = new Query(Criteria.where("id").is(p1.getId()));
		PersonWithIdPropertyOfTypeObjectId found1 = template.findOne(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(found1).isNotNull();
		Query _q = new Query(Criteria.where("_id").is(p1.getId()));
		template.remove(_q, PersonWithIdPropertyOfTypeObjectId.class);
		PersonWithIdPropertyOfTypeObjectId notFound1 = template.findOne(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(notFound1).isNull();

		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Bubba_to_be_removed");
		p2.setAge(51);
		template.insert(p2);

		Query q2 = new Query(Criteria.where("id").is(p2.getId()));
		PersonWithIdPropertyOfTypeObjectId found2 = template.findOne(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(found2).isNotNull();
		template.remove(q2, PersonWithIdPropertyOfTypeObjectId.class);
		PersonWithIdPropertyOfTypeObjectId notFound2 = template.findOne(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(notFound2).isNull();
	}

	@Test
	public void testAddingToList() {
		PersonWithAList p = new PersonWithAList();
		p.setFirstName("Sven");
		p.setAge(22);
		template.insert(p);

		Query q1 = new Query(Criteria.where("id").is(p.getId()));
		PersonWithAList p2 = template.findOne(q1, PersonWithAList.class);
		assertThat(p2).isNotNull();
		assertThat(p2.getWishList().size()).isEqualTo(0);

		p2.addToWishList("please work!");

		template.save(p2);

		PersonWithAList p3 = template.findOne(q1, PersonWithAList.class);
		assertThat(p3).isNotNull();
		assertThat(p3.getWishList().size()).isEqualTo(1);

		Friend f = new Friend();
		p.setFirstName("Erik");
		p.setAge(21);

		p3.addFriend(f);
		template.save(p3);

		PersonWithAList p4 = template.findOne(q1, PersonWithAList.class);
		assertThat(p4).isNotNull();
		assertThat(p4.getWishList().size()).isEqualTo(1);
		assertThat(p4.getFriends().size()).isEqualTo(1);

	}

	@Test
	public void testFindOneWithSort() {
		PersonWithAList p = new PersonWithAList();
		p.setFirstName("Sven");
		p.setAge(22);
		template.insert(p);

		PersonWithAList p2 = new PersonWithAList();
		p2.setFirstName("Erik");
		p2.setAge(21);
		template.insert(p2);

		PersonWithAList p3 = new PersonWithAList();
		p3.setFirstName("Mark");
		p3.setAge(40);
		template.insert(p3);

		// test query with a sort
		Query q2 = new Query(Criteria.where("age").gt(10));
		q2.with(Sort.by(Direction.DESC, "age"));
		PersonWithAList p5 = template.findOne(q2, PersonWithAList.class);
		assertThat(p5.getFirstName()).isEqualTo("Mark");
	}

	@Test
	public void testUsingReadPreference() throws Exception {
		this.template.execute("readPref", new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<org.bson.Document> collection)
					throws MongoException, DataAccessException {

				// assertThat(collection.getOptions(), is(0));
				// assertThat(collection.read.getDB().getOptions(), is(0));
				return null;
			}
		});
		MongoTemplate slaveTemplate = new MongoTemplate(factory);
		slaveTemplate.setReadPreference(ReadPreference.secondary());
		slaveTemplate.execute("readPref", new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<org.bson.Document> collection)
					throws MongoException, DataAccessException {
				assertThat(collection.getReadPreference()).isEqualTo(ReadPreference.secondary());
				// assertThat(collection.getDB().getOptions(), is(0));
				return null;
			}
		});
	}

	@Test // DATADOC-166, DATAMONGO-1762
	public void removingNullIsANoOp() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.remove((Object) null));
	}

	@Test // DATADOC-240, DATADOC-212
	public void updatesObjectIdsCorrectly() {

		PersonWithIdPropertyOfTypeObjectId person = new PersonWithIdPropertyOfTypeObjectId();
		person.setId(new ObjectId());
		person.setFirstName("Dave");

		template.save(person);
		template.updateFirst(query(where("id").is(person.getId())), update("firstName", "Carter"),
				PersonWithIdPropertyOfTypeObjectId.class);

		PersonWithIdPropertyOfTypeObjectId result = template.findById(person.getId(),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(result).isNotNull();
		assertThat(result.getId()).isEqualTo(person.getId());
		assertThat(result.getFirstName()).isEqualTo("Carter");
	}

	@Test
	public void testWriteConcernResolver() {

		PersonWithIdPropertyOfTypeObjectId person = new PersonWithIdPropertyOfTypeObjectId();
		person.setId(new ObjectId());
		person.setFirstName("Dave");

		template.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		template.save(person);
		template.updateFirst(query(where("id").is(person.getId())), update("firstName", "Carter"),
				PersonWithIdPropertyOfTypeObjectId.class);

		FsyncSafeWriteConcernResolver resolver = new FsyncSafeWriteConcernResolver();
		template.setWriteConcernResolver(resolver);
		Query q = query(where("_id").is(person.getId()));
		Update u = update("firstName", "Carter");
		template.updateFirst(q, u, PersonWithIdPropertyOfTypeObjectId.class);

		MongoAction lastMongoAction = resolver.getMongoAction();
		assertThat(lastMongoAction.getCollectionName()).isEqualTo("personWithIdPropertyOfTypeObjectId");
		assertThat(lastMongoAction.getDefaultWriteConcern()).isEqualTo(WriteConcern.UNACKNOWLEDGED);
		assertThat(lastMongoAction.getDocument()).isNotNull();
		assertThat(lastMongoAction.getEntityType().toString())
				.isEqualTo(PersonWithIdPropertyOfTypeObjectId.class.toString());
		assertThat(lastMongoAction.getMongoActionOperation()).isEqualTo(MongoActionOperation.UPDATE);
		assertThat(lastMongoAction.getQuery()).isEqualTo(q.getQueryObject());
	}

	private class FsyncSafeWriteConcernResolver implements WriteConcernResolver {

		private MongoAction mongoAction;

		public WriteConcern resolve(MongoAction action) {
			this.mongoAction = action;
			return WriteConcern.JOURNALED;
		}

		public MongoAction getMongoAction() {
			return mongoAction;
		}
	}

	@Test // DATADOC-246
	public void updatesDBRefsCorrectly() {

		DBRef first = new DBRef("foo", new ObjectId());
		DBRef second = new DBRef("bar", new ObjectId());

		template.updateFirst(new Query(), update("dbRefs", Arrays.asList(first, second)), ClassWithDBRefs.class);
	}

	class ClassWithDBRefs {
		List<DBRef> dbrefs;
	}

	@Test // DATADOC-202
	public void executeDocument() {
		template.insert(new Person("Tom"));
		template.insert(new Person("Dick"));
		template.insert(new Person("Harry"));
		final List<String> names = new ArrayList<>();
		template.executeQuery(new Query(), template.getCollectionName(Person.class), new DocumentCallbackHandler() {
			public void processDocument(org.bson.Document document) {
				String name = (String) document.get("firstName");
				if (name != null) {
					names.add(name);
				}
			}
		});
		assertThat(names.size()).isEqualTo(3);
		// template.remove(new Query(), Person.class);
	}

	@Test // DATADOC-202
	public void executeDocumentWithCursorPreparer() {
		template.insert(new Person("Tom"));
		template.insert(new Person("Dick"));
		template.insert(new Person("Harry"));
		final List<String> names = new ArrayList<>();
		template.executeQuery(new Query(), template.getCollectionName(Person.class), new DocumentCallbackHandler() {
			public void processDocument(org.bson.Document document) {
				String name = (String) document.get("firstName");
				if (name != null) {
					names.add(name);
				}
			}
		}, new CursorPreparer() {

			public FindIterable<org.bson.Document> prepare(FindIterable<org.bson.Document> cursor) {
				cursor.limit(1);
				return cursor;
			}

		});
		assertThat(names.size()).isEqualTo(1);
		template.remove(new Query(), Person.class);
	}

	@Test // DATADOC-183
	public void countsDocumentsCorrectly() {

		assertThat(template.count(new Query(), Person.class)).isEqualTo(0L);

		Person dave = new Person("Dave");
		Person carter = new Person("Carter");

		template.save(dave);
		template.save(carter);

		assertThat(template.count(new Query(), Person.class)).isEqualTo(2L);
		assertThat(template.count(query(where("firstName").is("Carter")), Person.class)).isEqualTo(1L);
	}

	@Test // DATADOC-183
	public void countRejectsNullEntityClass() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.count(null, (Class<?>) null));
	}

	@Test // DATADOC-183
	public void countRejectsEmptyCollectionName() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.count(null, ""));
	}

	@Test // DATADOC-183
	public void countRejectsNullCollectionName() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.count(null, (String) null));
	}

	@Test
	public void returnsEntityWhenQueryingForDateTime() {

		DateTime dateTime = new DateTime(2011, 3, 3, 12, 0, 0, 0);
		TestClass testClass = new TestClass(dateTime);
		mappingTemplate.save(testClass);

		List<TestClass> testClassList = mappingTemplate.find(new Query(Criteria.where("myDate").is(dateTime.toDate())),
				TestClass.class);
		assertThat(testClassList.size()).isEqualTo(1);
		assertThat(testClassList.get(0).myDate).isEqualTo(testClass.myDate);
	}

	@Test // DATADOC-230
	public void removesEntityFromCollection() {

		template.remove(new Query(), "mycollection");

		Person person = new Person("Dave");

		template.save(person, "mycollection");
		assertThat(template.findAll(TestClass.class, "mycollection").size()).isEqualTo(1);

		template.remove(person, "mycollection");
		assertThat(template.findAll(Person.class, "mycollection").isEmpty()).isTrue();
	}

	@Test // DATADOC-349
	public void removesEntityWithAnnotatedIdIfIdNeedsMassaging() {

		String id = new ObjectId().toString();

		Sample sample = new Sample();
		sample.id = id;

		template.save(sample);

		assertThat(template.findOne(query(where("id").is(id)), Sample.class).id).isEqualTo(id);

		template.remove(sample);
		assertThat(template.findOne(query(where("id").is(id)), Sample.class)).isNull();
	}

	@Test // DATAMONGO-423
	public void executesQueryWithNegatedRegexCorrectly() {

		Sample first = new Sample();
		first.field = "Matthews";

		Sample second = new Sample();
		second.field = "Beauford";

		template.save(first);
		template.save(second);

		Query query = query(where("field").not().regex("Matthews"));

		List<Sample> result = template.find(query, Sample.class);
		assertThat(result.size()).isEqualTo(1);
		assertThat(result.get(0).field).isEqualTo("Beauford");
	}

	@Test // DATAMONGO-447
	public void storesAndRemovesTypeWithComplexId() {

		MyId id = new MyId();
		id.first = "foo";
		id.second = "bar";

		TypeWithMyId source = new TypeWithMyId();
		source.id = id;

		template.save(source);
		template.remove(query(where("id").is(id)), TypeWithMyId.class);
	}

	@Test // DATAMONGO-506
	public void exceutesBasicQueryCorrectly() {

		Address address = new Address();
		address.state = "PA";
		address.city = "Philadelphia";

		MyPerson person = new MyPerson();
		person.name = "Oleg";
		person.address = address;

		template.save(person);

		Query query = new BasicQuery("{'name' : 'Oleg'}");
		List<MyPerson> result = template.find(query, MyPerson.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Oleg");

		query = new BasicQuery("{'address.state' : 'PA' }");
		result = template.find(query, MyPerson.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Oleg");
	}

	@Test(expected = OptimisticLockingFailureException.class) // DATAMONGO-279
	public void optimisticLockingHandling() {

		// Init version
		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.age = 29;
		person.firstName = "Patryk";
		template.save(person);

		List<PersonWithVersionPropertyOfTypeInteger> result = template
				.findAll(PersonWithVersionPropertyOfTypeInteger.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).version).isEqualTo(0);

		// Version change
		person = result.get(0);
		person.firstName = "Patryk2";

		template.save(person);

		assertThat(person.version).isEqualTo(1);

		result = mappingTemplate.findAll(PersonWithVersionPropertyOfTypeInteger.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).version).isEqualTo(1);

		// Optimistic lock exception
		person.version = 0;
		person.firstName = "Patryk3";

		template.save(person);
	}

	@Test // DATAMONGO-562
	public void optimisticLockingHandlingWithExistingId() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.id = new ObjectId().toString();
		person.age = 29;
		person.firstName = "Patryk";
		template.save(person);
	}

	@Test // DATAMONGO-617
	public void doesNotFailOnVersionInitForUnversionedEntity() {

		org.bson.Document document = new org.bson.Document();
		document.put("firstName", "Oliver");

		template.insert(document, template.getCollectionName(PersonWithVersionPropertyOfTypeInteger.class));
	}

	@Test // DATAMONGO-1617
	public void doesNotFailOnInsertForEntityWithNonAutogeneratableId() {

		PersonWithIdPropertyOfTypeUUID person = new PersonWithIdPropertyOfTypeUUID();
		person.setFirstName("Laszlo");
		person.setAge(33);

		template.insert(person);
		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-539
	public void removesObjectFromExplicitCollection() {

		String collectionName = "explicit";
		template.remove(new Query(), collectionName);

		PersonWithConvertedId person = new PersonWithConvertedId();
		person.name = "Dave";
		template.save(person, collectionName);
		assertThat(template.findAll(PersonWithConvertedId.class, collectionName).isEmpty()).isFalse();

		template.remove(person, collectionName);
		assertThat(template.findAll(PersonWithConvertedId.class, collectionName).isEmpty()).isTrue();
	}

	// DATAMONGO-549
	public void savesMapCorrectly() {

		Map<String, String> map = new HashMap<>();
		map.put("key", "value");

		template.save(map, "maps");
	}

	@Test // DATAMONGO-549, DATAMONGO-1730
	public void savesMongoPrimitiveObjectCorrectly() {
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> template.save(new Object(), "collection"));
	}

	@Test // DATAMONGO-549
	public void rejectsNullObjectToBeSaved() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.save(null));
	}

	@Test // DATAMONGO-550
	public void savesPlainDocumentCorrectly() {

		org.bson.Document document = new org.bson.Document("foo", "bar");
		template.save(document, "collection");

		assertThat(document.containsKey("_id")).isTrue();
	}

	@Test(expected = MappingException.class) // DATAMONGO-550, DATAMONGO-1730
	public void rejectsPlainObjectWithOutExplicitCollection() {

		org.bson.Document document = new org.bson.Document("foo", "bar");
		template.save(document, "collection");

		template.findById(document.get("_id"), org.bson.Document.class);
	}

	@Test // DATAMONGO-550
	public void readsPlainDocumentById() {

		org.bson.Document document = new org.bson.Document("foo", "bar");
		template.save(document, "collection");

		org.bson.Document result = template.findById(document.get("_id"), org.bson.Document.class, "collection");
		assertThat(result.get("foo")).isEqualTo(document.get("foo"));
		assertThat(result.get("_id")).isEqualTo(document.get("_id"));
	}

	@Test // DATAMONGO-551
	public void writesPlainString() {
		template.save("{ 'foo' : 'bar' }", "collection");
	}

	@Test // DATAMONGO-551
	public void rejectsNonJsonStringForSave() {
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> template.save("Foobar!", "collection"));
	}

	@Test // DATAMONGO-588
	public void initializesVersionOnInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insert(person);

		assertThat(person.version).isEqualTo(0);
	}

	@Test // DATAMONGO-2195
	public void removeVersionedEntityConsidersVersion() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insert(person);
		assertThat(person.version).isEqualTo(0);
		template.update(PersonWithVersionPropertyOfTypeInteger.class).matching(query(where("id").is(person.id)))
				.apply(new Update().set("firstName", "Walter")).first();

		DeleteResult deleteResult = template.remove(person);

		assertThat(deleteResult.wasAcknowledged()).isTrue();
		assertThat(deleteResult.getDeletedCount()).isZero();
		assertThat(template.count(new Query(), PersonWithVersionPropertyOfTypeInteger.class)).isOne();
	}

	@Test // DATAMONGO-588
	public void initializesVersionOnBatchInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insertAll(Arrays.asList(person));

		assertThat(person.version).isEqualTo(0);
	}

	@Test // DATAMONGO-1992
	public void initializesIdAndVersionAndOfImmutableObject() {

		ImmutableVersioned versioned = new ImmutableVersioned();

		ImmutableVersioned saved = template.insert(versioned);

		assertThat(saved).isNotSameAs(versioned);
		assertThat(versioned.id).isNull();
		assertThat(versioned.version).isNull();

		assertThat(saved.id).isNotNull();
		assertThat(saved.version).isEqualTo(0L);
	}

	@Test // DATAMONGO-568, DATAMONGO-1762
	public void queryCantBeNull() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> template.find(null, PersonWithIdPropertyOfTypeObjectId.class));
	}

	@Test // DATAMONGO-620
	public void versionsObjectIntoDedicatedCollection() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.save(person, "personX");
		assertThat(person.version).isEqualTo(0);

		template.save(person, "personX");
		assertThat(person.version).isEqualTo(1);
	}

	@Test // DATAMONGO-621
	public void correctlySetsLongVersionProperty() {

		PersonWithVersionPropertyOfTypeLong person = new PersonWithVersionPropertyOfTypeLong();
		person.firstName = "Dave";

		template.save(person);
		assertThat(person.version).isEqualTo(0L);
	}

	@Test(expected = DuplicateKeyException.class) // DATAMONGO-622
	public void preventsDuplicateInsert() {

		template.setWriteConcern(WriteConcern.ACKNOWLEDGED);

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.save(person);
		assertThat(person.version).isEqualTo(0);

		person.version = null;
		template.save(person);
	}

	@Test // DATAMONGO-629
	public void countAndFindWithoutTypeInformation() {

		Person person = new Person();
		template.save(person);

		Query query = query(where("_id").is(person.getId()));
		String collectionName = template.getCollectionName(Person.class);

		assertThat(template.find(query, HashMap.class, collectionName)).hasSize(1);
		assertThat(template.count(query, collectionName)).isEqualTo(1L);
	}

	@Test // DATAMONGO-571
	public void nullsPropertiesForVersionObjectUpdates() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";

		template.save(person);
		assertThat(person.id).isNotNull();

		person.lastname = null;
		template.save(person);

		person = template.findOne(query(where("id").is(person.id)), VersionedPerson.class);
		assertThat(person.lastname).isNull();
	}

	@Test // DATAMONGO-571
	public void nullsValuesForUpdatesOfUnversionedEntity() {

		Person person = new Person("Dave");
		template.save(person);

		person.setFirstName(null);
		template.save(person);

		person = template.findOne(query(where("id").is(person.getId())), Person.class);
		assertThat(person.getFirstName()).isNull();
	}

	@Test // DATAMONGO-679
	public void savesJsonStringCorrectly() {

		org.bson.Document document = new org.bson.Document().append("first", "first").append("second", "second");

		template.save(document, "collection");

		List<org.bson.Document> result = template.findAll(org.bson.Document.class, "collection");
		assertThat(result.size()).isEqualTo(1);
		assertThat(result.get(0).containsKey("first")).isTrue();
	}

	@Test
	public void executesExistsCorrectly() {

		Sample sample = new Sample();
		template.save(sample);

		Query query = query(where("id").is(sample.id));

		assertThat(template.exists(query, Sample.class)).isTrue();
		assertThat(template.exists(query(where("_id").is(sample.id)), template.getCollectionName(Sample.class))).isTrue();
		assertThat(template.exists(query, Sample.class, template.getCollectionName(Sample.class))).isTrue();
	}

	@Test // DATAMONGO-675
	public void updateConsidersMappingAnnotations() {

		TypeWithFieldAnnotation entity = new TypeWithFieldAnnotation();
		entity.emailAddress = "old";

		template.save(entity);

		Query query = query(where("_id").is(entity.id));
		Update update = Update.update("emailAddress", "new");

		FindAndModifyOptions options = new FindAndModifyOptions().returnNew(true);
		TypeWithFieldAnnotation result = template.findAndModify(query, update, options, TypeWithFieldAnnotation.class);
		assertThat(result.emailAddress).isEqualTo("new");
	}

	@Test // DATAMONGO-671
	public void findsEntityByDateReference() {

		TypeWithDate entity = new TypeWithDate();
		entity.date = new Date(System.currentTimeMillis() - 10);
		template.save(entity);

		Query query = query(where("date").lt(new Date()));
		List<TypeWithDate> result = template.find(query, TypeWithDate.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).date).isNotNull();
	}

	@Test // DATAMONGO-540
	public void findOneAfterUpsertForNonExistingObjectReturnsTheInsertedObject() {

		String idValue = "4711";
		Query query = new Query(Criteria.where("id").is(idValue));

		String fieldValue = "bubu";
		Update update = Update.update("field", fieldValue);

		template.upsert(query, update, Sample.class);
		Sample result = template.findOne(query, Sample.class);

		assertThat(result).isNotNull();
		assertThat(result.field).isEqualTo(fieldValue);
		assertThat(result.id).isEqualTo(idValue);
	}

	@Test // DATAMONGO-392
	public void updatesShouldRetainTypeInformation() {

		Document doc = new Document();
		doc.id = "4711";
		doc.model = new ModelA("foo");
		template.insert(doc);

		Query query = new Query(Criteria.where("id").is(doc.id));
		String newModelValue = "bar";
		Update update = Update.update("model", new ModelA(newModelValue));
		template.updateFirst(query, update, Document.class);

		Document result = template.findOne(query, Document.class);

		assertThat(result).isNotNull();
		assertThat(result.id).isEqualTo(doc.id);
		assertThat(result.model).isNotNull();
		assertThat(result.model.value()).isEqualTo(newModelValue);
	}

	@Test // DATAMONGO-702
	public void queryShouldSupportRealAndAliasedPropertyNamesForFieldInclusions() {

		ObjectWith3AliasedFields obj = new ObjectWith3AliasedFields();
		obj.id = "4711";
		obj.property1 = "P1";
		obj.property2 = "P2";
		obj.property3 = "P3";

		template.insert(obj);

		Query query = new Query(Criteria.where("id").is(obj.id));
		query.fields() //
				.include("property2") // real property name
				.include("prop3"); // aliased property name

		ObjectWith3AliasedFields result = template.findOne(query, ObjectWith3AliasedFields.class);

		assertThat(result.id).isEqualTo(obj.id);
		assertThat(result.property1).isNull();
		assertThat(result.property2).isEqualTo(obj.property2);
		assertThat(result.property3).isEqualTo(obj.property3);
	}

	@Test // DATAMONGO-702
	public void queryShouldSupportRealAndAliasedPropertyNamesForFieldExclusions() {

		ObjectWith3AliasedFields obj = new ObjectWith3AliasedFields();
		obj.id = "4711";
		obj.property1 = "P1";
		obj.property2 = "P2";
		obj.property3 = "P3";

		template.insert(obj);

		Query query = new Query(Criteria.where("id").is(obj.id));
		query.fields() //
				.exclude("property2") // real property name
				.exclude("prop3"); // aliased property name

		ObjectWith3AliasedFields result = template.findOne(query, ObjectWith3AliasedFields.class);

		assertThat(result.id).isEqualTo(obj.id);
		assertThat(result.property1).isEqualTo(obj.property1);
		assertThat(result.property2).isNull();
		assertThat(result.property3).isNull();
	}

	@Test // DATAMONGO-702
	public void findMultipleWithQueryShouldSupportRealAndAliasedPropertyNamesForFieldExclusions() {

		ObjectWith3AliasedFields obj0 = new ObjectWith3AliasedFields();
		obj0.id = "4711";
		obj0.property1 = "P10";
		obj0.property2 = "P20";
		obj0.property3 = "P30";
		ObjectWith3AliasedFields obj1 = new ObjectWith3AliasedFields();
		obj1.id = "4712";
		obj1.property1 = "P11";
		obj1.property2 = "P21";
		obj1.property3 = "P31";

		template.insert(obj0);
		template.insert(obj1);

		Query query = new Query(Criteria.where("id").in(obj0.id, obj1.id));
		query.fields() //
				.exclude("property2") // real property name
				.exclude("prop3"); // aliased property name

		List<ObjectWith3AliasedFields> results = template.find(query, ObjectWith3AliasedFields.class);

		assertThat(results).isNotNull();
		assertThat(results.size()).isEqualTo(2);

		ObjectWith3AliasedFields result0 = results.get(0);
		assertThat(result0).isNotNull();
		assertThat(result0.id).isEqualTo(obj0.id);
		assertThat(result0.property1).isEqualTo(obj0.property1);
		assertThat(result0.property2).isNull();
		assertThat(result0.property3).isNull();

		ObjectWith3AliasedFields result1 = results.get(1);
		assertThat(result1).isNotNull();
		assertThat(result1.id).isEqualTo(obj1.id);
		assertThat(result1.property1).isEqualTo(obj1.property1);
		assertThat(result1.property2).isNull();
		assertThat(result1.property3).isNull();
	}

	@Test // DATAMONGO-702
	public void queryShouldSupportNestedPropertyNamesForFieldInclusions() {

		ObjectWith3AliasedFieldsAndNestedAddress obj = new ObjectWith3AliasedFieldsAndNestedAddress();
		obj.id = "4711";
		obj.property1 = "P1";
		obj.property2 = "P2";
		obj.property3 = "P3";
		Address address = new Address();
		String stateValue = "WA";
		address.state = stateValue;
		address.city = "Washington";
		obj.address = address;

		template.insert(obj);

		Query query = new Query(Criteria.where("id").is(obj.id));
		query.fields() //
				.include("property2") // real property name
				.include("address.state"); // aliased property name

		ObjectWith3AliasedFieldsAndNestedAddress result = template.findOne(query,
				ObjectWith3AliasedFieldsAndNestedAddress.class);

		assertThat(result.id).isEqualTo(obj.id);
		assertThat(result.property1).isNull();
		assertThat(result.property2).isEqualTo(obj.property2);
		assertThat(result.property3).isNull();
		assertThat(result.address).isNotNull();
		assertThat(result.address.city).isNull();
		assertThat(result.address.state).isEqualTo(stateValue);
	}

	@Test // DATAMONGO-709
	public void aQueryRestrictedWithOneRestrictedResultTypeShouldReturnOnlyInstancesOfTheRestrictedType() {

		BaseDoc doc0 = new BaseDoc();
		doc0.value = "foo";
		SpecialDoc doc1 = new SpecialDoc();
		doc1.value = "foo";
		doc1.specialValue = "specialfoo";
		VerySpecialDoc doc2 = new VerySpecialDoc();
		doc2.value = "foo";
		doc2.specialValue = "specialfoo";
		doc2.verySpecialValue = 4711;

		String collectionName = template.getCollectionName(BaseDoc.class);
		template.insert(doc0, collectionName);
		template.insert(doc1, collectionName);
		template.insert(doc2, collectionName);

		Query query = Query.query(where("value").is("foo")).restrict(SpecialDoc.class);
		List<BaseDoc> result = template.find(query, BaseDoc.class);

		assertThat(result).isNotNull();
		assertThat(result.size()).isEqualTo(1);
		assertThat(result.get(0)).isInstanceOf(SpecialDoc.class);
	}

	@Test // DATAMONGO-709
	public void aQueryRestrictedWithMultipleRestrictedResultTypesShouldReturnOnlyInstancesOfTheRestrictedTypes() {

		BaseDoc doc0 = new BaseDoc();
		doc0.value = "foo";
		SpecialDoc doc1 = new SpecialDoc();
		doc1.value = "foo";
		doc1.specialValue = "specialfoo";
		VerySpecialDoc doc2 = new VerySpecialDoc();
		doc2.value = "foo";
		doc2.specialValue = "specialfoo";
		doc2.verySpecialValue = 4711;

		String collectionName = template.getCollectionName(BaseDoc.class);
		template.insert(doc0, collectionName);
		template.insert(doc1, collectionName);
		template.insert(doc2, collectionName);

		Query query = Query.query(where("value").is("foo")).restrict(BaseDoc.class, VerySpecialDoc.class);
		List<BaseDoc> result = template.find(query, BaseDoc.class);

		assertThat(result).isNotNull();
		assertThat(result.size()).isEqualTo(2);
		assertThat(result.get(0).getClass()).isEqualTo((Object) BaseDoc.class);
		assertThat(result.get(1).getClass()).isEqualTo((Object) VerySpecialDoc.class);
	}

	@Test // DATAMONGO-709
	public void aQueryWithNoRestrictedResultTypesShouldReturnAllInstancesWithinTheGivenCollection() {

		BaseDoc doc0 = new BaseDoc();
		doc0.value = "foo";
		SpecialDoc doc1 = new SpecialDoc();
		doc1.value = "foo";
		doc1.specialValue = "specialfoo";
		VerySpecialDoc doc2 = new VerySpecialDoc();
		doc2.value = "foo";
		doc2.specialValue = "specialfoo";
		doc2.verySpecialValue = 4711;

		String collectionName = template.getCollectionName(BaseDoc.class);
		template.insert(doc0, collectionName);
		template.insert(doc1, collectionName);
		template.insert(doc2, collectionName);

		Query query = Query.query(where("value").is("foo"));
		List<BaseDoc> result = template.find(query, BaseDoc.class);

		assertThat(result).isNotNull();
		assertThat(result.size()).isEqualTo(3);
		assertThat(result.get(0).getClass()).isEqualTo((Object) BaseDoc.class);
		assertThat(result.get(1).getClass()).isEqualTo((Object) SpecialDoc.class);
		assertThat(result.get(2).getClass()).isEqualTo((Object) VerySpecialDoc.class);
	}

	@Test // DATAMONGO-771
	public void allowInsertWithPlainJsonString() {

		String id = "4711";
		String value = "bubu";
		String json = String.format("{_id:%s, field: '%s'}", id, value);

		template.insert(json, "sample");
		List<Sample> result = template.findAll(Sample.class);

		assertThat(result.size()).isEqualTo(1);
		assertThat(result.get(0).id).isEqualTo(id);
		assertThat(result.get(0).field).isEqualTo(value);
	}

	@Test // DATAMONGO-2028
	public void allowInsertOfDbObjectWithMappedTypes() {

		DBObject dbObject = new BasicDBObject("_id", "foo").append("duration", Duration.ofSeconds(100));
		template.insert(dbObject, "sample");
		List<org.bson.Document> result = template.findAll(org.bson.Document.class, "sample");

		assertThat(result.size()).isEqualTo(1);
		assertThat(result.get(0).getString("_id")).isEqualTo("foo");
		assertThat(result.get(0).getString("duration")).isEqualTo("PT1M40S");
	}

	@Test // DATAMONGO-816
	public void shouldExecuteQueryShouldMapQueryBeforeQueryExecution() {

		ObjectWithEnumValue o = new ObjectWithEnumValue();
		o.value = EnumValue.VALUE2;
		template.save(o);

		Query q = Query.query(Criteria.where("value").in(EnumValue.VALUE2));

		template.executeQuery(q, StringUtils.uncapitalize(ObjectWithEnumValue.class.getSimpleName()),
				new DocumentCallbackHandler() {

					@Override
					public void processDocument(org.bson.Document document) throws MongoException, DataAccessException {

						assertThat(document).isNotNull();

						ObjectWithEnumValue result = template.getConverter().read(ObjectWithEnumValue.class, document);

						assertThat(result.value).isEqualTo(EnumValue.VALUE2);
					}
				});
	}

	@Test // DATAMONGO-811
	public void updateFirstShouldIncreaseVersionForVersionedEntity() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";
		template.save(person);
		assertThat(person.id).isNotNull();

		Query qry = query(where("id").is(person.id));
		VersionedPerson personAfterFirstSave = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterFirstSave.version).isEqualTo(0L);

		template.updateFirst(qry, Update.update("lastname", "Bubu"), VersionedPerson.class);

		VersionedPerson personAfterUpdateFirst = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterUpdateFirst.version).isEqualTo(1L);
		assertThat(personAfterUpdateFirst.lastname).isEqualTo("Bubu");
	}

	@Test // DATAMONGO-811
	public void updateFirstShouldIncreaseVersionOnlyForFirstMatchingEntity() {

		VersionedPerson person1 = new VersionedPerson();
		person1.firstname = "Dave";

		VersionedPerson person2 = new VersionedPerson();
		person2.firstname = "Dave";

		template.save(person1);
		template.save(person2);
		Query q = query(where("id").in(person1.id, person2.id));

		template.updateFirst(q, Update.update("lastname", "Metthews"), VersionedPerson.class);

		for (VersionedPerson p : template.find(q, VersionedPerson.class)) {
			if ("Metthews".equals(p.lastname)) {
				assertThat(p.version).isEqualTo(Long.valueOf(1));
			} else {
				assertThat(p.version).isEqualTo(Long.valueOf(0));
			}
		}
	}

	@Test // DATAMONGO-811
	public void updateMultiShouldIncreaseVersionOfAllUpdatedEntities() {

		VersionedPerson person1 = new VersionedPerson();
		person1.firstname = "Dave";

		VersionedPerson person2 = new VersionedPerson();
		person2.firstname = "Dave";

		template.save(person1);
		template.save(person2);

		Query q = query(where("id").in(person1.id, person2.id));
		template.updateMulti(q, Update.update("lastname", "Metthews"), VersionedPerson.class);

		for (VersionedPerson p : template.find(q, VersionedPerson.class)) {
			assertThat(p.version).isEqualTo(Long.valueOf(1));
		}
	}

	@Test // DATAMONGO-686
	public void itShouldBePossibleToReuseAnExistingQuery() {

		Sample sample = new Sample();
		sample.id = "42";
		sample.field = "A";

		template.save(sample);

		Query query = new Query();
		query.addCriteria(where("_id").in("42", "43"));

		assertThat(template.count(query, Sample.class)).isEqualTo(1L);

		query.with(PageRequest.of(0, 10));
		query.with(Sort.by("field"));

		assertThat(template.find(query, Sample.class)).isNotEmpty();
	}

	@Test // DATAMONGO-807
	public void findAndModifyShouldRetrainTypeInformationWithinUpdatedType() {

		Document document = new Document();
		document.model = new ModelA("value1");

		template.save(document);

		Query query = query(where("id").is(document.id));
		Update update = Update.update("model", new ModelA("value2"));
		template.findAndModify(query, update, Document.class);

		Document retrieved = template.findOne(query, Document.class);
		assertThat(retrieved.model).isInstanceOf(ModelA.class);
		assertThat(retrieved.model.value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldRetainTypeInformationWithinUpdatedTypeOnDocumentWithNestedCollectionWhenWholeCollectionIsReplaced() {

		DocumentWithNestedCollection doc = new DocumentWithNestedCollection();

		Map<String, Model> entry = new HashMap<>();
		entry.put("key1", new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		entry.put("key2", new ModelA("value2"));

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("models", Collections.singletonList(entry));

		assertThat(template.findOne(query, DocumentWithNestedCollection.class)).isNotNull();

		template.findAndModify(query, update, DocumentWithNestedCollection.class);

		DocumentWithNestedCollection retrieved = template.findOne(query, DocumentWithNestedCollection.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.id).isEqualTo(doc.id);

		assertThat(retrieved.models.get(0).entrySet()).hasSize(2);

		assertThat(retrieved.models.get(0).get("key1")).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get("key1").value()).isEqualTo("value1");

		assertThat(retrieved.models.get(0).get("key2")).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get("key2").value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldRetainTypeInformationWithinUpdatedTypeOnDocumentWithNestedCollectionWhenFirstElementIsReplaced() {

		DocumentWithNestedCollection doc = new DocumentWithNestedCollection();

		Map<String, Model> entry = new HashMap<>();
		entry.put("key1", new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		entry.put("key2", new ModelA("value2"));

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("models.0", entry);

		assertThat(template.findOne(query, DocumentWithNestedCollection.class)).isNotNull();

		template.findAndModify(query, update, DocumentWithNestedCollection.class);

		DocumentWithNestedCollection retrieved = template.findOne(query, DocumentWithNestedCollection.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.id).isEqualTo(doc.id);

		assertThat(retrieved.models.get(0).entrySet()).hasSize(2);

		assertThat(retrieved.models.get(0).get("key1")).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get("key1").value()).isEqualTo("value1");

		assertThat(retrieved.models.get(0).get("key2")).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get("key2").value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationOnDocumentWithNestedCollectionObjectInsertedAtSecondIndex() {

		DocumentWithNestedCollection doc = new DocumentWithNestedCollection();

		Map<String, Model> entry = new HashMap<>();
		entry.put("key1", new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("models.1", Collections.singletonMap("key2", new ModelA("value2")));

		assertThat(template.findOne(query, DocumentWithNestedCollection.class)).isNotNull();

		template.findAndModify(query, update, DocumentWithNestedCollection.class);

		DocumentWithNestedCollection retrieved = template.findOne(query, DocumentWithNestedCollection.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.id).isEqualTo(doc.id);

		assertThat(retrieved.models.get(0).entrySet()).hasSize(1);
		assertThat(retrieved.models.get(1).entrySet()).hasSize(1);

		assertThat(retrieved.models.get(0).get("key1")).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get("key1").value()).isEqualTo("value1");

		assertThat(retrieved.models.get(1).get("key2")).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(1).get("key2").value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldRetainTypeInformationWithinUpdatedTypeOnEmbeddedDocumentWithCollectionWhenUpdatingPositionedElement()
			throws Exception {

		List<Model> models = new ArrayList<>();
		models.add(new ModelA("value1"));

		DocumentWithEmbeddedDocumentWithCollection doc = new DocumentWithEmbeddedDocumentWithCollection(
				new DocumentWithCollection(models));

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("embeddedDocument.models.0", new ModelA("value2"));

		assertThat(template.findOne(query, DocumentWithEmbeddedDocumentWithCollection.class)).isNotNull();

		template.findAndModify(query, update, DocumentWithEmbeddedDocumentWithCollection.class);

		DocumentWithEmbeddedDocumentWithCollection retrieved = template.findOne(query,
				DocumentWithEmbeddedDocumentWithCollection.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.embeddedDocument.models).hasSize(1);
		assertThat(retrieved.embeddedDocument.models.get(0).value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationWithinUpdatedTypeOnEmbeddedDocumentWithCollectionWhenUpdatingSecondElement()
			throws Exception {

		List<Model> models = new ArrayList<>();
		models.add(new ModelA("value1"));

		DocumentWithEmbeddedDocumentWithCollection doc = new DocumentWithEmbeddedDocumentWithCollection(
				new DocumentWithCollection(models));

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("embeddedDocument.models.1", new ModelA("value2"));

		assertThat(template.findOne(query, DocumentWithEmbeddedDocumentWithCollection.class)).isNotNull();

		template.findAndModify(query, update, DocumentWithEmbeddedDocumentWithCollection.class);

		DocumentWithEmbeddedDocumentWithCollection retrieved = template.findOne(query,
				DocumentWithEmbeddedDocumentWithCollection.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.embeddedDocument.models).hasSize(2);
		assertThat(retrieved.embeddedDocument.models.get(0).value()).isEqualTo("value1");
		assertThat(retrieved.embeddedDocument.models.get(1).value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationWithinUpdatedTypeOnEmbeddedDocumentWithCollectionWhenRewriting()
			throws Exception {

		List<Model> models = Arrays.<Model> asList(new ModelA("value1"));

		DocumentWithEmbeddedDocumentWithCollection doc = new DocumentWithEmbeddedDocumentWithCollection(
				new DocumentWithCollection(models));

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("embeddedDocument",
				new DocumentWithCollection(Arrays.<Model> asList(new ModelA("value2"))));

		assertThat(template.findOne(query, DocumentWithEmbeddedDocumentWithCollection.class)).isNotNull();

		template.findAndModify(query, update, DocumentWithEmbeddedDocumentWithCollection.class);

		DocumentWithEmbeddedDocumentWithCollection retrieved = template.findOne(query,
				DocumentWithEmbeddedDocumentWithCollection.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.embeddedDocument.models).hasSize(1);
		assertThat(retrieved.embeddedDocument.models.get(0).value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationWithinUpdatedTypeOnDocumentWithNestedLists() {

		DocumentWithNestedList doc = new DocumentWithNestedList();

		List<Model> entry = new ArrayList<>();
		entry.add(new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		Query query = query(where("id").is(doc.id));

		assertThat(template.findOne(query, DocumentWithNestedList.class)).isNotNull();

		Update update = Update.update("models.0.1", new ModelA("value2"));

		template.findAndModify(query, update, DocumentWithNestedList.class);

		DocumentWithNestedList retrieved = template.findOne(query, DocumentWithNestedList.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.id).isEqualTo(doc.id);

		assertThat(retrieved.models.get(0)).hasSize(2);

		assertThat(retrieved.models.get(0).get(0)).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get(0).value()).isEqualTo("value1");

		assertThat(retrieved.models.get(0).get(1)).isInstanceOf(ModelA.class);
		assertThat(retrieved.models.get(0).get(1).value()).isEqualTo("value2");
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldReplaceDocument() {

		org.bson.Document doc = new org.bson.Document("foo", "bar");
		template.save(doc, "findandreplace");

		org.bson.Document replacement = new org.bson.Document("foo", "baz");
		org.bson.Document previous = template.findAndReplace(query(where("foo").is("bar")), replacement,
				FindAndReplaceOptions.options(), org.bson.Document.class, "findandreplace");

		assertThat(previous).containsEntry("foo", "bar");
		assertThat(template.findOne(query(where("foo").is("baz")), org.bson.Document.class, "findandreplace")).isNotNull();
	}

	@Test // DATAMONGO-1827
	@MongoVersion(asOf = "3.6")
	public void findAndReplaceShouldErrorOnIdPresent() {

		template.save(new MyPerson("Walter"));

		MyPerson replacement = new MyPerson("Heisenberg");
		replacement.id = "invalid-id";

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> template.findAndReplace(query(where("name").is("Walter")), replacement));
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldErrorOnSkip() {

		assertThatIllegalArgumentException().isThrownBy(
				() -> template.findAndReplace(query(where("name").is("Walter")).skip(10), new MyPerson("Heisenberg")));
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldErrorOnLimit() {

		assertThatIllegalArgumentException().isThrownBy(
				() -> template.findAndReplace(query(where("name").is("Walter")).limit(10), new MyPerson("Heisenberg")));
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldConsiderSortAndUpdateFirstIfMultipleFound() {

		MyPerson walter1 = new MyPerson("Walter 1");
		MyPerson walter2 = new MyPerson("Walter 2");

		template.save(walter1);
		template.save(walter2);

		MyPerson replacement = new MyPerson("Heisenberg");

		template.findAndReplace(query(where("name").regex("Walter.*")).with(Sort.by(Direction.DESC, "name")), replacement);

		assertThat(template.findAll(MyPerson.class)).hasSize(2).contains(walter1).doesNotContain(walter2);
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldReplaceObject() {

		MyPerson person = new MyPerson("Walter");
		template.save(person);

		MyPerson previous = template.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"));

		assertThat(previous.getName()).isEqualTo("Walter");
		assertThat(template.findOne(query(where("id").is(person.id)), MyPerson.class)).hasFieldOrPropertyWithValue("name",
				"Heisenberg");
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldConsiderFields() {

		MyPerson person = new MyPerson("Walter");
		person.address = new Address("TX", "Austin");
		template.save(person);

		Query query = query(where("name").is("Walter"));
		query.fields().include("address");

		MyPerson previous = template.findAndReplace(query, new MyPerson("Heisenberg"));

		assertThat(previous.getName()).isNull();
		assertThat(previous.getAddress()).isEqualTo(person.address);
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceNonExistingWithUpsertFalse() {

		MyPerson previous = template.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"));

		assertThat(previous).isNull();
		assertThat(template.findAll(MyPerson.class)).isEmpty();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceNonExistingWithUpsertTrue() {

		MyPerson previous = template.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"),
				FindAndReplaceOptions.options().upsert());

		assertThat(previous).isNull();
		assertThat(template.findAll(MyPerson.class)).hasSize(1);
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldReplaceObjectReturingNew() {

		MyPerson person = new MyPerson("Walter");
		template.save(person);

		MyPerson updated = template.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"),
				FindAndReplaceOptions.options().returnNew());

		assertThat(updated.getName()).isEqualTo("Heisenberg");
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldProjectReturnedObjectCorrectly() {

		template.save(new MyPerson("Walter"));

		MyPersonProjection projection = template.findAndReplace(query(where("name").is("Walter")),
				new MyPerson("Heisenberg"), FindAndReplaceOptions.empty(), MyPerson.class, MyPersonProjection.class);

		assertThat(projection.getName()).isEqualTo("Walter");
	}

	@Test // DATAMONGO-407
	public void updatesShouldRetainTypeInformationEvenForCollections() {

		List<Model> models = Arrays.<Model> asList(new ModelA("foo"));

		DocumentWithCollection doc = new DocumentWithCollection(models);
		doc.id = "4711";
		template.insert(doc);

		Query query = new Query(Criteria.where("id").is(doc.id));
		query.addCriteria(where("models.value").is("foo"));
		String newModelValue = "bar";
		Update update = Update.update("models.$", new ModelA(newModelValue));
		template.updateFirst(query, update, DocumentWithCollection.class);

		Query findQuery = new Query(Criteria.where("id").is(doc.id));
		DocumentWithCollection result = template.findOne(findQuery, DocumentWithCollection.class);

		assertThat(result).isNotNull();
		assertThat(result.id).isEqualTo(doc.id);
		assertThat(result.models).isNotNull();
		assertThat(result.models).hasSize(1);
		assertThat(result.models.get(0).value()).isEqualTo(newModelValue);
	}

	@Test // DATAMONGO-812
	@MongoVersion(asOf = "2.4")
	public void updateMultiShouldAddValuesCorrectlyWhenUsingPushEachWithComplexTypes() {

		DocumentWithCollection document = new DocumentWithCollection(Collections.<Model> emptyList());
		template.save(document);
		Query query = query(where("id").is(document.id));
		assertThat(template.findOne(query, DocumentWithCollection.class).models).isEmpty();

		Update update = new Update().push("models").each(new ModelA("model-b"), new ModelA("model-c"));
		template.updateMulti(query, update, DocumentWithCollection.class);

		assertThat(template.findOne(query, DocumentWithCollection.class).models).hasSize(2);
	}

	@Test // DATAMONGO-812
	@MongoVersion(asOf = "2.4")
	public void updateMultiShouldAddValuesCorrectlyWhenUsingPushEachWithSimpleTypes() {

		DocumentWithCollectionOfSimpleType document = new DocumentWithCollectionOfSimpleType();
		document.values = Arrays.asList("spring");
		template.save(document);

		Query query = query(where("id").is(document.id));
		assertThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values).hasSize(1);

		Update update = new Update().push("values").each("data", "mongodb");
		template.updateMulti(query, update, DocumentWithCollectionOfSimpleType.class);

		assertThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values).hasSize(3);
	}

	@Test // DATAMONOGO-828
	public void updateFirstShouldDoNothingWhenCalledForEntitiesThatDoNotExist() {

		Query q = query(where("id").is(Long.MIN_VALUE));

		template.updateFirst(q, Update.update("lastname", "supercalifragilisticexpialidocious"), VersionedPerson.class);
		assertThat(template.findOne(q, VersionedPerson.class)).isNull();
	}

	@Test // DATAMONGO-354, DATAMONGO-1824
	@MongoVersion(until = "3.6")
	@SuppressWarnings("deprecation")
	public void testUpdateShouldAllowMultiplePushAll() {

		DocumentWithMultipleCollections doc = new DocumentWithMultipleCollections();
		doc.id = "1234";
		doc.string1 = Arrays.asList("spring");
		doc.string2 = Arrays.asList("one");

		template.save(doc);

		Update update = new Update().pushAll("string1", new Object[] { "data", "mongodb" });
		update.pushAll("string2", new String[] { "two", "three" });

		Query findQuery = new Query(Criteria.where("id").is(doc.id));
		template.updateFirst(findQuery, update, DocumentWithMultipleCollections.class);

		DocumentWithMultipleCollections result = template.findOne(findQuery, DocumentWithMultipleCollections.class);
		assertThat(result.string1).contains("spring", "data", "mongodb");
		assertThat(result.string2).contains("one", "two", "three");

	}

	@Test // DATAMONGO-404
	public void updateWithPullShouldRemoveNestedItemFromDbRefAnnotatedCollection() {

		Sample sample1 = new Sample("1", "A");
		Sample sample2 = new Sample("2", "B");
		template.save(sample1);
		template.save(sample2);

		DocumentWithDBRefCollection doc = new DocumentWithDBRefCollection();
		doc.id = "1";
		doc.dbRefAnnotatedList = Arrays.asList( //
				sample1, //
				sample2 //
		);
		template.save(doc);

		Update update = new Update().pull("dbRefAnnotatedList", doc.dbRefAnnotatedList.get(1));

		Query qry = query(where("id").is("1"));
		template.updateFirst(qry, update, DocumentWithDBRefCollection.class);

		DocumentWithDBRefCollection result = template.findOne(qry, DocumentWithDBRefCollection.class);

		assertThat(result).isNotNull();
		assertThat(result.dbRefAnnotatedList).hasSize(1);
		assertThat(result.dbRefAnnotatedList.get(0)).isNotNull();
		assertThat(result.dbRefAnnotatedList.get(0).id).isEqualTo((Object) "1");
	}

	@Test // DATAMONGO-404
	public void updateWithPullShouldRemoveNestedItemFromDbRefAnnotatedCollectionWhenGivenAnIdValueOfComponentTypeEntity() {

		Sample sample1 = new Sample("1", "A");
		Sample sample2 = new Sample("2", "B");
		template.save(sample1);
		template.save(sample2);

		DocumentWithDBRefCollection doc = new DocumentWithDBRefCollection();
		doc.id = "1";
		doc.dbRefAnnotatedList = Arrays.asList( //
				sample1, //
				sample2 //
		);
		template.save(doc);

		Update update = new Update().pull("dbRefAnnotatedList.id", "2");

		Query qry = query(where("id").is("1"));
		template.updateFirst(qry, update, DocumentWithDBRefCollection.class);

		DocumentWithDBRefCollection result = template.findOne(qry, DocumentWithDBRefCollection.class);

		assertThat(result).isNotNull();
		assertThat(result.dbRefAnnotatedList).hasSize(1);
		assertThat(result.dbRefAnnotatedList.get(0)).isNotNull();
		assertThat(result.dbRefAnnotatedList.get(0).id).isEqualTo((Object) "1");
	}

	@Test // DATAMONGO-852
	public void updateShouldNotBumpVersionNumberIfVersionPropertyIncludedInUpdate() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";
		template.save(person);
		assertThat(person.id).isNotNull();

		Query qry = query(where("id").is(person.id));
		VersionedPerson personAfterFirstSave = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterFirstSave.version).isEqualTo(0L);

		template.updateFirst(qry, Update.update("lastname", "Bubu").set("version", 100L), VersionedPerson.class);

		VersionedPerson personAfterUpdateFirst = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterUpdateFirst.version).isEqualTo(100L);
		assertThat(personAfterUpdateFirst.lastname).isEqualTo("Bubu");
	}

	@Test // DATAMONGO-468
	public void shouldBeAbleToUpdateDbRefPropertyWithDomainObject() {

		Sample sample1 = new Sample("1", "A");
		Sample sample2 = new Sample("2", "B");
		template.save(sample1);
		template.save(sample2);

		DocumentWithDBRefCollection doc = new DocumentWithDBRefCollection();
		doc.id = "1";
		doc.dbRefProperty = sample1;
		template.save(doc);

		Update update = new Update().set("dbRefProperty", sample2);

		Query qry = query(where("id").is("1"));
		template.updateFirst(qry, update, DocumentWithDBRefCollection.class);

		DocumentWithDBRefCollection updatedDoc = template.findOne(qry, DocumentWithDBRefCollection.class);

		assertThat(updatedDoc).isNotNull();
		assertThat(updatedDoc.dbRefProperty).isNotNull();
		assertThat(updatedDoc.dbRefProperty.id).isEqualTo(sample2.id);
		assertThat(updatedDoc.dbRefProperty.field).isEqualTo(sample2.field);
	}

	@Test // DATAMONGO-862
	public void testUpdateShouldWorkForPathsOnInterfaceMethods() {

		DocumentWithCollection document = new DocumentWithCollection(
				Arrays.<Model> asList(new ModelA("spring"), new ModelA("data")));

		template.save(document);

		Query query = query(where("id").is(document.id).and("models.value").exists(true));
		Update update = new Update().set("models.$.value", "mongodb");
		template.findAndModify(query, update, DocumentWithCollection.class);

		DocumentWithCollection result = template.findOne(query(where("id").is(document.id)), DocumentWithCollection.class);
		assertThat(result.models.get(0).value()).isEqualTo("mongodb");
	}

	@Test // DATAMONGO-773
	public void testShouldSupportQueryWithIncludedDbRefField() {

		Sample sample = new Sample("47111", "foo");
		template.save(sample);

		DocumentWithDBRefCollection doc = new DocumentWithDBRefCollection();
		doc.id = "4711";
		doc.dbRefProperty = sample;

		template.save(doc);

		Query qry = query(where("id").is(doc.id));
		qry.fields().include("dbRefProperty");

		List<DocumentWithDBRefCollection> result = template.find(qry, DocumentWithDBRefCollection.class);

		assertThat(result).isNotNull();
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isNotNull();
		assertThat(result.get(0).dbRefProperty).isNotNull();
		assertThat(result.get(0).dbRefProperty.field).isEqualTo(sample.field);
	}

	@Test // DATAMONGO-566
	public void testFindAllAndRemoveFullyReturnsAndRemovesDocuments() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");
		template.insert(Arrays.asList(spring, data, mongodb), Sample.class);

		Query qry = query(where("field").in("spring", "mongodb"));
		List<Sample> result = template.findAllAndRemove(qry, Sample.class);

		assertThat(result).hasSize(2);

		assertThat(template.getDb().getCollection("sample").countDocuments(
				new org.bson.Document("field", new org.bson.Document("$in", Arrays.asList("spring", "mongodb")))))
						.isEqualTo(0L);
		assertThat(template.getDb().getCollection("sample").countDocuments(new org.bson.Document("field", "data")))
				.isEqualTo(1L);
	}

	@Test // DATAMONGO-1001
	public void shouldAllowSavingOfLazyLoadedDbRefs() {

		template.dropCollection(SomeTemplate.class);
		template.dropCollection(SomeMessage.class);
		template.dropCollection(SomeContent.class);

		SomeContent content = new SomeContent();
		content.id = "content-1";
		content.text = "spring";
		template.save(content);

		SomeTemplate tmpl = new SomeTemplate();
		tmpl.id = "template-1";
		tmpl.content = content; // @DBRef(lazy=true) tmpl.content

		template.save(tmpl);

		SomeTemplate savedTmpl = template.findById(tmpl.id, SomeTemplate.class);

		SomeContent loadedContent = savedTmpl.getContent();
		loadedContent.setText("data");
		template.save(loadedContent);

		assertThat(template.findById(content.id, SomeContent.class).getText()).isEqualTo("data");

	}

	@Test // DATAMONGO-880
	public void savingAndReassigningLazyLoadingProxies() {

		template.dropCollection(SomeTemplate.class);
		template.dropCollection(SomeMessage.class);
		template.dropCollection(SomeContent.class);

		SomeContent content = new SomeContent();
		content.id = "C1";
		content.text = "BUBU";
		template.save(content);

		SomeTemplate tmpl = new SomeTemplate();
		tmpl.id = "T1";
		tmpl.content = content; // @DBRef(lazy=true) tmpl.content

		template.save(tmpl);

		SomeTemplate savedTmpl = template.findById(tmpl.id, SomeTemplate.class);

		SomeMessage message = new SomeMessage();
		message.id = "M1";
		message.dbrefContent = savedTmpl.content; // @DBRef message.dbrefContent
		message.normalContent = savedTmpl.content;

		template.save(message);

		SomeMessage savedMessage = template.findById(message.id, SomeMessage.class);

		assertThat(savedMessage.dbrefContent.text).isEqualTo(content.text);
		assertThat(savedMessage.normalContent.text).isEqualTo(content.text);
	}

	@Test // DATAMONGO-884
	public void callingNonObjectMethodsOnLazyLoadingProxyShouldReturnNullIfUnderlyingDbrefWasDeletedInbetween() {

		template.dropCollection(SomeTemplate.class);
		template.dropCollection(SomeContent.class);

		SomeContent content = new SomeContent();
		content.id = "C1";
		content.text = "BUBU";
		template.save(content);

		SomeTemplate tmpl = new SomeTemplate();
		tmpl.id = "T1";
		tmpl.content = content; // @DBRef(lazy=true) tmpl.content

		template.save(tmpl);

		SomeTemplate savedTmpl = template.findById(tmpl.id, SomeTemplate.class);

		template.remove(content);

		assertThat(savedTmpl.getContent().toString()).isEqualTo("someContent:C1$LazyLoadingProxy");
		assertThat(savedTmpl.getContent()).isInstanceOf(LazyLoadingProxy.class);
		assertThat(savedTmpl.getContent().getText()).isNull();
	}

	@Test // DATAMONGO-471
	public void updateMultiShouldAddValuesCorrectlyWhenUsingAddToSetWithEach() {

		DocumentWithCollectionOfSimpleType document = new DocumentWithCollectionOfSimpleType();
		document.values = Arrays.asList("spring");
		template.save(document);

		Query query = query(where("id").is(document.id));
		assertThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values).hasSize(1);

		Update update = new Update().addToSet("values").each("data", "mongodb");
		template.updateMulti(query, update, DocumentWithCollectionOfSimpleType.class);

		assertThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values).hasSize(3);
	}

	@Test // DATAMONGO-1210
	public void findAndModifyAddToSetWithEachShouldNotAddDuplicatesNorTypeHintForSimpleDocuments() {

		DocumentWithCollectionOfSamples doc = new DocumentWithCollectionOfSamples();
		doc.samples = Arrays.asList(new Sample(null, "sample1"));

		template.save(doc);

		Query query = query(where("id").is(doc.id));

		assertThat(template.findOne(query, DocumentWithCollectionOfSamples.class)).isNotNull();

		Update update = new Update().addToSet("samples").each(new Sample(null, "sample2"), new Sample(null, "sample1"));

		template.findAndModify(query, update, DocumentWithCollectionOfSamples.class);

		DocumentWithCollectionOfSamples retrieved = template.findOne(query, DocumentWithCollectionOfSamples.class);

		assertThat(retrieved).isNotNull();
		assertThat(retrieved.samples).hasSize(2);
		assertThat(retrieved.samples.get(0).field).isEqualTo("sample1");
		assertThat(retrieved.samples.get(1).field).isEqualTo("sample2");
	}

	@Test // DATAMONGO-888
	public void sortOnIdFieldPropertyShouldBeMappedCorrectly() {

		DoucmentWithNamedIdField one = new DoucmentWithNamedIdField();
		one.someIdKey = "1";
		one.value = "a";

		DoucmentWithNamedIdField two = new DoucmentWithNamedIdField();
		two.someIdKey = "2";
		two.value = "b";

		template.save(one);
		template.save(two);

		Query query = query(where("_id").in("1", "2")).with(Sort.by(Direction.DESC, "someIdKey"));
		assertThat(template.find(query, DoucmentWithNamedIdField.class)).containsExactly(two, one);
	}

	@Test // DATAMONGO-888
	public void sortOnAnnotatedFieldPropertyShouldBeMappedCorrectly() {

		DoucmentWithNamedIdField one = new DoucmentWithNamedIdField();
		one.someIdKey = "1";
		one.value = "a";

		DoucmentWithNamedIdField two = new DoucmentWithNamedIdField();
		two.someIdKey = "2";
		two.value = "b";

		template.save(one);
		template.save(two);

		Query query = query(where("_id").in("1", "2")).with(Sort.by(Direction.DESC, "value"));
		assertThat(template.find(query, DoucmentWithNamedIdField.class)).containsExactly(two, one);
	}

	@Test // DATAMONGO-913
	public void shouldRetrieveInitializedValueFromDbRefAssociationAfterLoad() {

		SomeContent content = new SomeContent();
		content.id = "content-1";
		content.name = "Content 1";
		content.text = "Some text";

		template.save(content);

		SomeTemplate tmpl = new SomeTemplate();
		tmpl.id = "template-1";
		tmpl.content = content;

		template.save(tmpl);

		SomeTemplate result = template.findOne(query(where("content").is(tmpl.getContent())), SomeTemplate.class);

		assertThat(result).isNotNull();
		assertThat(result.getContent()).isNotNull();
		assertThat(result.getContent().getId()).isNotNull();
		assertThat(result.getContent().getName()).isNotNull();
		assertThat(result.getContent().getText()).isEqualTo(content.getText());
	}

	@Test // DATAMONGO-913
	public void shouldReuseExistingDBRefInQueryFromDbRefAssociationAfterLoad() {

		SomeContent content = new SomeContent();
		content.id = "content-1";
		content.name = "Content 1";
		content.text = "Some text";

		template.save(content);

		SomeTemplate tmpl = new SomeTemplate();
		tmpl.id = "template-1";
		tmpl.content = content;

		template.save(tmpl);

		SomeTemplate result = template.findOne(query(where("content").is(tmpl.getContent())), SomeTemplate.class);

		// Use lazy-loading-proxy in query
		result = template.findOne(query(where("content").is(result.getContent())), SomeTemplate.class);

		assertThat(result.getContent().getName()).isNotNull();
		assertThat(result.getContent().getName()).isEqualTo(content.getName());
	}

	@Test // DATAMONGO-970
	public void insertsAndRemovesBasicDocumentCorrectly() {

		org.bson.Document object = new org.bson.Document("key", "value");
		template.insert(object, "collection");

		assertThat(object.get("_id")).isNotNull();
		assertThat(template.findAll(Document.class, "collection")).hasSize(1);

		template.remove(object, "collection");
		assertThat(template.findAll(Document.class, "collection")).hasSize(0);
	}

	@Test // DATAMONGO-1207
	public void ignoresNullElementsForInsertAll() {

		Address newYork = new Address("NY", "New York");
		Address washington = new Address("DC", "Washington");

		template.insertAll(Arrays.asList(newYork, null, washington));

		List<Address> result = template.findAll(Address.class);

		assertThat(result).hasSize(2);
		assertThat(result).contains(newYork, washington);
	}

	@Test // DATAMONGO-1176
	public void generatesIdForInsertAll() {

		Person walter = new Person(null, "Walter");
		Person jesse = new Person(null, "Jesse");

		template.insertAll(Arrays.asList(walter, jesse));

		List<Person> result = template.findAll(Person.class);

		assertThat(result).hasSize(2);
		assertThat(walter.getId()).isNotNull();
		assertThat(jesse.getId()).isNotNull();
	}

	@Test // DATAMONGO-1208
	public void takesSortIntoAccountWhenStreaming() {

		Person youngestPerson = new Person("John", 20);
		Person oldestPerson = new Person("Jane", 42);

		template.insertAll(Arrays.asList(oldestPerson, youngestPerson));

		Query q = new Query();
		q.with(Sort.by(Direction.ASC, "age"));
		CloseableIterator<Person> stream = template.stream(q, Person.class);

		assertThat(stream.next().getAge()).isEqualTo(youngestPerson.getAge());
		assertThat(stream.next().getAge()).isEqualTo(oldestPerson.getAge());
	}

	@Test // DATAMONGO-1208
	public void takesLimitIntoAccountWhenStreaming() {

		Person youngestPerson = new Person("John", 20);
		Person oldestPerson = new Person("Jane", 42);

		template.insertAll(Arrays.asList(oldestPerson, youngestPerson));

		Query q = new Query();
		q.with(PageRequest.of(0, 1, Sort.by(Direction.ASC, "age")));
		CloseableIterator<Person> stream = template.stream(q, Person.class);

		assertThat(stream.next().getAge()).isEqualTo(youngestPerson.getAge());
		assertThat(stream.hasNext()).isFalse();
	}

	@Test // DATAMONGO-1204
	public void resolvesCyclicDBRefCorrectly() {

		SomeMessage message = new SomeMessage();
		SomeContent content = new SomeContent();

		template.save(message);
		template.save(content);

		message.dbrefContent = content;
		content.dbrefMessage = message;

		template.save(message);
		template.save(content);

		SomeMessage messageLoaded = template.findOne(query(where("id").is(message.id)), SomeMessage.class);
		SomeContent contentLoaded = template.findOne(query(where("id").is(content.id)), SomeContent.class);

		assertThat(messageLoaded.dbrefContent.id).isEqualTo(contentLoaded.id);
		assertThat(contentLoaded.dbrefMessage.id).isEqualTo(messageLoaded.id);
	}

	@Test // DATAMONGO-1287, DATAMONGO-2004
	public void shouldReuseAlreadyResolvedLazyLoadedDBRefWhenUsedAsPersistenceConstructorArgument() {

		Document docInCtor = new Document();
		docInCtor.id = "doc-in-ctor";
		template.save(docInCtor);

		DocumentWithLazyDBrefUsedInPresistenceConstructor source = new DocumentWithLazyDBrefUsedInPresistenceConstructor(
				docInCtor);

		template.save(source);

		DocumentWithLazyDBrefUsedInPresistenceConstructor loaded = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBrefUsedInPresistenceConstructor.class);
		assertThat(loaded.refToDocUsedInCtor).isInstanceOf(LazyLoadingProxy.class);
		assertThat(loaded.refToDocNotUsedInCtor).isNull();
	}

	@Test // DATAMONGO-1287
	public void shouldNotReuseLazyLoadedDBRefWhenTypeUsedInPersistenceConstrcutorButValueRefersToAnotherProperty() {

		Document docNotUsedInCtor = new Document();
		docNotUsedInCtor.id = "doc-but-not-used-in-ctor";
		template.save(docNotUsedInCtor);

		DocumentWithLazyDBrefUsedInPresistenceConstructor source = new DocumentWithLazyDBrefUsedInPresistenceConstructor(
				null);
		source.refToDocNotUsedInCtor = docNotUsedInCtor;

		template.save(source);

		DocumentWithLazyDBrefUsedInPresistenceConstructor loaded = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBrefUsedInPresistenceConstructor.class);
		assertThat(loaded.refToDocNotUsedInCtor).isInstanceOf(LazyLoadingProxy.class);
		assertThat(loaded.refToDocUsedInCtor).isNull();
	}

	@Test // DATAMONGO-1287, DATAMONGO-2004
	public void shouldRespectParameterValueWhenAttemptingToReuseLazyLoadedDBRefUsedInPersistenceConstructor() {

		Document docInCtor = new Document();
		docInCtor.id = "doc-in-ctor";
		template.save(docInCtor);

		Document docNotUsedInCtor = new Document();
		docNotUsedInCtor.id = "doc-but-not-used-in-ctor";
		template.save(docNotUsedInCtor);

		DocumentWithLazyDBrefUsedInPresistenceConstructor source = new DocumentWithLazyDBrefUsedInPresistenceConstructor(
				docInCtor);
		source.refToDocNotUsedInCtor = docNotUsedInCtor;

		template.save(source);

		DocumentWithLazyDBrefUsedInPresistenceConstructor loaded = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBrefUsedInPresistenceConstructor.class);
		assertThat(loaded.refToDocUsedInCtor).isInstanceOf(LazyLoadingProxy.class);
		assertThat(loaded.refToDocNotUsedInCtor).isInstanceOf(LazyLoadingProxy.class);
	}

	@Test // DATAMONGO-1401
	public void updateShouldWorkForTypesContainingGeoJsonTypes() {

		WithGeoJson wgj = new WithGeoJson();
		wgj.id = "1";
		wgj.description = "datamongo-1401";
		wgj.point = new GeoJsonPoint(1D, 2D);

		template.save(wgj);

		wgj.description = "datamongo-1401-update";
		template.save(wgj);

		assertThat(template.findOne(query(where("id").is(wgj.id)), WithGeoJson.class).point).isEqualTo(wgj.point);
	}

	@Test // DATAMONGO-1404
	public void updatesDateValueCorrectlyWhenUsingMinOperator() {

		Calendar cal = Calendar.getInstance(Locale.US);
		cal.set(2013, 10, 13, 0, 0, 0);

		TypeWithDate twd = new TypeWithDate();
		twd.date = new Date();
		template.save(twd);
		template.updateFirst(query(where("id").is(twd.id)), new Update().min("date", cal.getTime()), TypeWithDate.class);

		TypeWithDate loaded = template.find(query(where("id").is(twd.id)), TypeWithDate.class).get(0);
		assertThat(loaded.date).isEqualTo(cal.getTime());
	}

	@Test // DATAMONGO-1404
	public void updatesNumericValueCorrectlyWhenUsingMinOperator() {

		TypeWithNumbers twn = new TypeWithNumbers();
		twn.byteVal = 100;
		twn.doubleVal = 200D;
		twn.floatVal = 300F;
		twn.intVal = 400;
		twn.longVal = 500L;

		// Note that $min operator uses String comparison for BigDecimal/BigInteger comparison according to BSON sort rules.
		twn.bigIntegerVal = new BigInteger("600");
		twn.bigDeciamVal = new BigDecimal("700.0");

		template.save(twn);

		byte byteVal = 90;
		Update update = new Update()//
				.min("byteVal", byteVal) //
				.min("doubleVal", 190D) //
				.min("floatVal", 290F) //
				.min("intVal", 390) //
				.min("longVal", 490) //
				.min("bigIntegerVal", new BigInteger("590")) //
				.min("bigDeciamVal", new BigDecimal("690")) //
		;

		template.updateFirst(query(where("id").is(twn.id)), update, TypeWithNumbers.class);

		TypeWithNumbers loaded = template.find(query(where("id").is(twn.id)), TypeWithNumbers.class).get(0);
		assertThat(loaded.byteVal).isEqualTo(byteVal);
		assertThat(loaded.doubleVal).isEqualTo(190D);
		assertThat(loaded.floatVal).isEqualTo(290F);
		assertThat(loaded.intVal).isEqualTo(390);
		assertThat(loaded.longVal).isEqualTo(490L);
		assertThat(loaded.bigIntegerVal).isEqualTo(new BigInteger("590"));
		assertThat(loaded.bigDeciamVal).isEqualTo(new BigDecimal("690"));
	}

	@Test // DATAMONGO-1404
	public void updatesDateValueCorrectlyWhenUsingMaxOperator() {

		Calendar cal = Calendar.getInstance(Locale.US);
		cal.set(2013, 10, 13, 0, 0, 0);

		TypeWithDate twd = new TypeWithDate();
		twd.date = cal.getTime();
		template.save(twd);

		cal.set(2019, 10, 13, 0, 0, 0);
		template.updateFirst(query(where("id").is(twd.id)), new Update().max("date", cal.getTime()), TypeWithDate.class);

		TypeWithDate loaded = template.find(query(where("id").is(twd.id)), TypeWithDate.class).get(0);
		assertThat(loaded.date).isEqualTo(cal.getTime());
	}

	@Test // DATAMONGO-1404
	public void updatesNumericValueCorrectlyWhenUsingMaxOperator() {

		TypeWithNumbers twn = new TypeWithNumbers();
		twn.byteVal = 100;
		twn.doubleVal = 200D;
		twn.floatVal = 300F;
		twn.intVal = 400;
		twn.longVal = 500L;

		// Note that $max operator uses String comparison for BigDecimal/BigInteger comparison according to BSON sort rules.
		twn.bigIntegerVal = new BigInteger("600");
		twn.bigDeciamVal = new BigDecimal("700.0");

		template.save(twn);

		byte byteVal = 101;
		Update update = new Update()//
				.max("byteVal", byteVal) //
				.max("doubleVal", 290D) //
				.max("floatVal", 390F) //
				.max("intVal", 490) //
				.max("longVal", 590) //
				.max("bigIntegerVal", new BigInteger("690")) //
				.max("bigDeciamVal", new BigDecimal("790")) //
		;

		template.updateFirst(query(where("id").is(twn.id)), update, TypeWithNumbers.class);

		TypeWithNumbers loaded = template.find(query(where("id").is(twn.id)), TypeWithNumbers.class).get(0);
		assertThat(loaded.byteVal).isEqualTo(byteVal);
		assertThat(loaded.doubleVal).isEqualTo(290D);
		assertThat(loaded.floatVal).isEqualTo(390F);
		assertThat(loaded.intVal).isEqualTo(490);
		assertThat(loaded.longVal).isEqualTo(590L);
		assertThat(loaded.bigIntegerVal).isEqualTo(new BigInteger("690"));
		assertThat(loaded.bigDeciamVal).isEqualTo(new BigDecimal("790"));
	}

	@Test // DATAMONGO-1404
	public void updatesBigNumberValueUsingStringComparisonWhenUsingMaxOperator() {

		TypeWithNumbers twn = new TypeWithNumbers();

		// Note that $max operator uses String comparison for BigDecimal/BigInteger comparison according to BSON sort rules.
		// Therefore "80" is considered greater than "700"
		twn.bigIntegerVal = new BigInteger("600");
		twn.bigDeciamVal = new BigDecimal("700.0");

		template.save(twn);

		Update update = new Update()//
				.max("bigIntegerVal", new BigInteger("70")) //
				.max("bigDeciamVal", new BigDecimal("80")) //
		;

		template.updateFirst(query(where("id").is(twn.id)), update, TypeWithNumbers.class);

		TypeWithNumbers loaded = template.find(query(where("id").is(twn.id)), TypeWithNumbers.class).get(0);
		assertThat(loaded.bigIntegerVal).isEqualTo(new BigInteger("70"));
		assertThat(loaded.bigDeciamVal).isEqualTo(new BigDecimal("80"));
	}

	@Test // DATAMONGO-1404
	public void updatesBigNumberValueUsingStringComparisonWhenUsingMinOperator() {

		TypeWithNumbers twn = new TypeWithNumbers();

		// Note that $max operator uses String comparison for BigDecimal/BigInteger comparison according to BSON sort rules.
		// Therefore "80" is considered greater than "700"
		twn.bigIntegerVal = new BigInteger("80");
		twn.bigDeciamVal = new BigDecimal("90.0");

		template.save(twn);

		Update update = new Update()//
				.min("bigIntegerVal", new BigInteger("700")) //
				.min("bigDeciamVal", new BigDecimal("800")) //
		;

		template.updateFirst(query(where("id").is(twn.id)), update, TypeWithNumbers.class);

		TypeWithNumbers loaded = template.find(query(where("id").is(twn.id)), TypeWithNumbers.class).get(0);
		assertThat(loaded.bigIntegerVal).isEqualTo(new BigInteger("700"));
		assertThat(loaded.bigDeciamVal).isEqualTo(new BigDecimal("800"));
	}

	@Test // DATAMONGO-1431, DATAMONGO-2323
	public void streamExecutionUsesExplicitCollectionName() {

		template.remove(new Query(), "some_special_collection");
		template.remove(new Query(), Document.class);

		Document document = new Document();

		template.insert(document, "some_special_collection");

		CloseableIterator<Document> stream = template.stream(new Query(), Document.class);
		assertThat(stream.hasNext()).isFalse();

		CloseableIterator<org.bson.Document> stream2 = template.stream(new Query(where("_id").is(document.id)),
				org.bson.Document.class, "some_special_collection");

		assertThat(stream2.hasNext()).isTrue();
		assertThat(stream2.next().get("_id")).isEqualTo(new ObjectId(document.id));
		assertThat(stream2.hasNext()).isFalse();
	}

	@Test // DATAMONGO-1194
	public void shouldFetchListOfReferencesCorrectly() {

		Sample one = new Sample("1", "jon snow");
		Sample two = new Sample("2", "tyrion lannister");

		template.save(one);
		template.save(two);

		DocumentWithDBRefCollection source = new DocumentWithDBRefCollection();
		source.dbRefAnnotatedList = Arrays.asList(two, one);

		template.save(source);

		assertThat(template.findOne(query(where("id").is(source.id)), DocumentWithDBRefCollection.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-1194
	public void shouldFetchListOfLazyReferencesCorrectly() {

		Sample one = new Sample("1", "jon snow");
		Sample two = new Sample("2", "tyrion lannister");

		template.save(one);
		template.save(two);

		DocumentWithDBRefCollection source = new DocumentWithDBRefCollection();
		source.lazyDbRefAnnotatedList = Arrays.asList(two, one);

		template.save(source);

		DocumentWithDBRefCollection target = template.findOne(query(where("id").is(source.id)),
				DocumentWithDBRefCollection.class);

		assertThat(target.lazyDbRefAnnotatedList).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getLazyDbRefAnnotatedList()).containsExactly(two, one);
	}

	@Test // DATAMONGO-1194
	public void shouldFetchMapOfLazyReferencesCorrectly() {

		Sample one = new Sample("1", "jon snow");
		Sample two = new Sample("2", "tyrion lannister");

		template.save(one);
		template.save(two);

		DocumentWithDBRefCollection source = new DocumentWithDBRefCollection();
		source.lazyDbRefAnnotatedMap = new LinkedHashMap<>();
		source.lazyDbRefAnnotatedMap.put("tyrion", two);
		source.lazyDbRefAnnotatedMap.put("jon", one);
		template.save(source);

		DocumentWithDBRefCollection target = template.findOne(query(where("id").is(source.id)),
				DocumentWithDBRefCollection.class);

		assertThat(target.lazyDbRefAnnotatedMap).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.lazyDbRefAnnotatedMap.values()).containsExactly(two, one);
	}

	@Test // DATAMONGO-2004
	public void shouldFetchLazyReferenceWithConstructorCreationCorrectly() {

		Sample one = new Sample("1", "jon snow");

		template.save(one);

		DocumentWithLazyDBRefsAndConstructorCreation source = new DocumentWithLazyDBRefsAndConstructorCreation(null, one,
				null, null);

		template.save(source);

		DocumentWithLazyDBRefsAndConstructorCreation target = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBRefsAndConstructorCreation.class);

		assertThat(target.lazyDbRefProperty).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.lazyDbRefProperty).isEqualTo(one);
	}

	@Test // DATAMONGO-2004
	public void shouldFetchMapOfLazyReferencesWithConstructorCreationCorrectly() {

		Sample one = new Sample("1", "jon snow");
		Sample two = new Sample("2", "tyrion lannister");

		template.save(one);
		template.save(two);

		Map<String, Sample> map = new LinkedHashMap<>();
		map.put("tyrion", two);
		map.put("jon", one);

		DocumentWithLazyDBRefsAndConstructorCreation source = new DocumentWithLazyDBRefsAndConstructorCreation(null, null,
				null, map);

		template.save(source);

		DocumentWithLazyDBRefsAndConstructorCreation target = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBRefsAndConstructorCreation.class);

		assertThat(target.lazyDbRefAnnotatedMap).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.lazyDbRefAnnotatedMap.values()).containsExactly(two, one);
	}

	@Test // DATAMONGO-2004
	public void shouldFetchListOfLazyReferencesWithConstructorCreationCorrectly() {

		Sample one = new Sample("1", "jon snow");
		Sample two = new Sample("2", "tyrion lannister");

		template.save(one);
		template.save(two);

		List<Sample> list = Arrays.asList(two, one);

		DocumentWithLazyDBRefsAndConstructorCreation source = new DocumentWithLazyDBRefsAndConstructorCreation(null, null,
				list, null);

		template.save(source);

		DocumentWithLazyDBRefsAndConstructorCreation target = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBRefsAndConstructorCreation.class);

		assertThat(target.lazyDbRefAnnotatedList).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getLazyDbRefAnnotatedList()).containsExactly(two, one);
	}

	@Test // DATAMONGO-1513
	@DirtiesContext
	public void populatesIdsAddedByEventListener() {

		context.addApplicationListener(new AbstractMongoEventListener<Document>() {

			@Override
			public void onBeforeSave(BeforeSaveEvent<Document> event) {
				event.getDocument().put("_id", UUID.randomUUID().toString());
			}
		});

		Document document = new Document();

		template.insertAll(Collections.singletonList(document));

		assertThat(document.id).isNotNull();
	}

	@Test // DATAMONGO-2189
	@DirtiesContext
	public void afterSaveEventContainsSavedObjectUsingInsertAll() {

		AtomicReference<ImmutableVersioned> saved = createAfterSaveReference();
		ImmutableVersioned source = new ImmutableVersioned();

		template.insertAll(Collections.singletonList(source));

		assertThat(saved.get()).isNotNull();
		assertThat(saved.get()).isNotSameAs(source);
		assertThat(saved.get().id).isNotNull();

	}

	@Test // DATAMONGO-2189
	@DirtiesContext
	public void afterSaveEventContainsSavedObjectUsingInsert() {

		AtomicReference<ImmutableVersioned> saved = createAfterSaveReference();
		ImmutableVersioned source = new ImmutableVersioned();

		template.insert(source);

		assertThat(saved.get()).isNotNull();
		assertThat(saved.get()).isNotSameAs(source);
		assertThat(saved.get().id).isNotNull();
	}

	@Test // DATAMONGO-1509
	public void findsByGenericNestedListElements() {

		List<Model> modelList = Collections.singletonList(new ModelA("value"));
		DocumentWithCollection dwc = new DocumentWithCollection(modelList);

		template.insert(dwc);

		Query query = query(where("models").is(modelList));
		assertThat(template.findOne(query, DocumentWithCollection.class)).isEqualTo(dwc);
	}

	@Test // DATAMONGO-1517
	@MongoVersion(asOf = "3.4")
	public void decimal128TypeShouldBeSavedAndLoadedCorrectly()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

		Class<?> decimal128Type = ClassUtils.resolveClassName("org.bson.types.Decimal128", null);

		WithObjectTypeProperty source = new WithObjectTypeProperty();
		source.id = "decimal128-property-value";
		source.value = decimal128Type.getConstructor(BigDecimal.class).newInstance(new BigDecimal(100));

		template.save(source);

		WithObjectTypeProperty loaded = template.findOne(query(where("id").is(source.id)), WithObjectTypeProperty.class);
		assertThat(loaded.getValue()).isInstanceOf(decimal128Type);
	}

	@Test // DATAMONGO-1718
	public void findAndRemoveAllWithoutExplicitDomainTypeShouldRemoveAndReturnEntitiesCorrectly() {

		Sample jon = new Sample("1", "jon snow");
		Sample bran = new Sample("2", "bran stark");
		Sample rickon = new Sample("3", "rickon stark");

		template.save(jon);
		template.save(bran);
		template.save(rickon);

		List<Sample> result = template.findAllAndRemove(query(where("field").regex(".*stark$")),
				template.getCollectionName(Sample.class));

		assertThat(result).hasSize(2);
		assertThat(result).contains(bran, rickon);
		assertThat(template.count(new BasicQuery("{}"), template.getCollectionName(Sample.class))).isEqualTo(1L);
	}

	@Test // DATAMONGO-1779
	public void appliesQueryLimitToEmptyQuery() {

		Sample first = new Sample("1", "Dave Matthews");
		Sample second = new Sample("2", "Carter Beauford");

		template.insertAll(Arrays.asList(first, second));

		assertThat(template.find(new Query().limit(1), Sample.class)).hasSize(1);
	}

	@Test // DATAMONGO-1870
	public void removeShouldConsiderLimit() {

		List<Sample> samples = IntStream.range(0, 100) //
				.mapToObj(i -> new Sample("id-" + i, i % 2 == 0 ? "stark" : "lannister")) //
				.collect(Collectors.toList());

		template.insertAll(samples);

		DeleteResult wr = template.remove(query(where("field").is("lannister")).limit(25), Sample.class);

		assertThat(wr.getDeletedCount()).isEqualTo(25L);
		assertThat(template.count(new Query(), Sample.class)).isEqualTo(75L);
	}

	@Test // DATAMONGO-1870
	public void removeShouldConsiderSkipAndSort() {

		List<Sample> samples = IntStream.range(0, 100) //
				.mapToObj(i -> new Sample("id-" + i, i % 2 == 0 ? "stark" : "lannister")) //
				.collect(Collectors.toList());

		template.insertAll(samples);

		DeleteResult wr = template.remove(new Query().skip(25).with(Sort.by("field")), Sample.class);

		assertThat(wr.getDeletedCount()).isEqualTo(75L);
		assertThat(template.count(new Query(), Sample.class)).isEqualTo(25L);
		assertThat(template.count(query(where("field").is("lannister")), Sample.class)).isEqualTo(25L);
		assertThat(template.count(query(where("field").is("stark")), Sample.class)).isEqualTo(0L);
	}

	@Test // DATAMONGO-1988
	public void findByNestedDocumentWithStringIdMappingToObjectIdMatchesDocumentsCorrectly() {

		DocumentWithNestedTypeHavingStringIdProperty source = new DocumentWithNestedTypeHavingStringIdProperty();
		source.id = "id-1";
		source.sample = new Sample();
		source.sample.id = new ObjectId().toHexString();

		template.save(source);

		DocumentWithNestedTypeHavingStringIdProperty target = template
				.query(DocumentWithNestedTypeHavingStringIdProperty.class)
				.matching(query(where("sample.id").is(source.sample.id))).firstValue();

		assertThat(target).isEqualTo(source);
	}

	@Test // DATAMONGO-1992
	public void writesAuditingMetadataForImmutableTypes() {

		ImmutableAudited source = new ImmutableAudited(null, null);
		ImmutableAudited result = template.save(source);

		assertThat(result).isNotSameAs(source).describedAs("Expected a different instances to be returned!");
		assertThat(result.modified).isNotNull().describedAs("Auditing field must not be null!");

		ImmutableAudited read = template.findOne(query(where("id").is(result.getId())), ImmutableAudited.class);

		assertThat(read.modified).isEqualTo(result.modified).describedAs("Expected auditing information to be read!");
	}

	@Test // DATAMONGO-1798
	public void saveAndLoadStringThatIsAnObjectIdAsString() {

		RawStringId source = new RawStringId();
		source.id = new ObjectId().toHexString();
		source.value = "new value";

		template.save(source);

		org.bson.Document result = template
				.execute(db -> (org.bson.Document) db.getCollection(template.getCollectionName(RawStringId.class))
						.find(Filters.eq("_id", source.id)).limit(1).into(new ArrayList()).iterator().next());

		assertThat(result).isNotNull();
		assertThat(result.get("_id")).isEqualTo(source.id);

		RawStringId target = template.findOne(query(where("id").is(source.id)), RawStringId.class);
		assertThat(target).isEqualTo(source);
	}

	@Test // DATAMONGO-2193
	public void shouldNotConvertStringToObjectIdForNonIdField() {

		ObjectId outerId = new ObjectId();
		String innerId = new ObjectId().toHexString();

		org.bson.Document source = new org.bson.Document() //
				.append("_id", outerId) //
				.append("inner", new org.bson.Document("id", innerId).append("value", "boooh"));

		template.getDb().getCollection(template.getCollectionName(Outer.class)).insertOne(source);

		Outer target = template.findOne(query(where("inner.id").is(innerId)), Outer.class);
		assertThat(target).isNotNull();
		assertThat(target.id).isEqualTo(outerId);
		assertThat(target.inner.id).isEqualTo(innerId);
	}

	private AtomicReference<ImmutableVersioned> createAfterSaveReference() {

		AtomicReference<ImmutableVersioned> saved = new AtomicReference<>();
		context.addApplicationListener(new AbstractMongoEventListener<ImmutableVersioned>() {

			@Override
			public void onAfterSave(AfterSaveEvent<ImmutableVersioned> event) {
				saved.set(event.getSource());
			}
		});

		return saved;
	}

	static class TypeWithNumbers {

		@Id String id;
		Integer intVal;
		Float floatVal;
		Long longVal;
		Double doubleVal;
		BigDecimal bigDeciamVal;
		BigInteger bigIntegerVal;
		Byte byteVal;
	}

	static class DoucmentWithNamedIdField {

		@Id String someIdKey;

		@Field(value = "val") //
		String value;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (someIdKey == null ? 0 : someIdKey.hashCode());
			result = prime * result + (value == null ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof DoucmentWithNamedIdField)) {
				return false;
			}
			DoucmentWithNamedIdField other = (DoucmentWithNamedIdField) obj;
			if (someIdKey == null) {
				if (other.someIdKey != null) {
					return false;
				}
			} else if (!someIdKey.equals(other.someIdKey)) {
				return false;
			}
			if (value == null) {
				if (other.value != null) {
					return false;
				}
			} else if (!value.equals(other.value)) {
				return false;
			}
			return true;
		}

	}

	@Data
	static class DocumentWithDBRefCollection {

		@Id public String id;

		@Field("db_ref_list") // DATAMONGO-1058
		@org.springframework.data.mongodb.core.mapping.DBRef //
		public List<Sample> dbRefAnnotatedList;

		@org.springframework.data.mongodb.core.mapping.DBRef //
		public Sample dbRefProperty;

		@Field("lazy_db_ref_list") // DATAMONGO-1194
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) //
		public List<Sample> lazyDbRefAnnotatedList;

		@Field("lazy_db_ref_map") // DATAMONGO-1194
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) public Map<String, Sample> lazyDbRefAnnotatedMap;
	}

	@Data
	@AllArgsConstructor
	static class DocumentWithLazyDBRefsAndConstructorCreation {

		@Id public String id;

		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) //
		public Sample lazyDbRefProperty;

		@Field("lazy_db_ref_list") @org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) //
		public List<Sample> lazyDbRefAnnotatedList;

		@Field("lazy_db_ref_map") @org.springframework.data.mongodb.core.mapping.DBRef(
				lazy = true) public Map<String, Sample> lazyDbRefAnnotatedMap;
	}

	@EqualsAndHashCode
	static class DocumentWithCollection {

		@Id String id;
		List<Model> models;

		DocumentWithCollection(List<Model> models) {
			this.models = models;
		}
	}

	static class DocumentWithCollectionOfSimpleType {

		@Id String id;
		List<String> values;
	}

	static class DocumentWithCollectionOfSamples {
		@Id String id;
		List<Sample> samples;
	}

	@EqualsAndHashCode
	static class DocumentWithNestedTypeHavingStringIdProperty {

		@Id String id;
		Sample sample;
	}

	static class DocumentWithMultipleCollections {
		@Id String id;
		List<String> string1;
		List<String> string2;
	}

	static class DocumentWithNestedCollection {
		@Id String id;
		List<Map<String, Model>> models = new ArrayList<>();
	}

	static class DocumentWithNestedList {
		@Id String id;
		List<List<Model>> models = new ArrayList<>();
	}

	static class DocumentWithEmbeddedDocumentWithCollection {
		@Id String id;
		DocumentWithCollection embeddedDocument;

		DocumentWithEmbeddedDocumentWithCollection(DocumentWithCollection embeddedDocument) {
			this.embeddedDocument = embeddedDocument;
		}
	}

	static interface Model {
		String value();

		String id();
	}

	@EqualsAndHashCode
	static class ModelA implements Model {

		@Id String id;
		private String value;

		ModelA(String value) {
			this.value = value;
		}

		@Override
		public String value() {
			return this.value;
		}

		@Override
		public String id() {
			return id;
		}
	}

	static class Document {

		@Id public String id;
		public Model model;
	}

	static class MyId {

		String first;
		String second;
	}

	static class TypeWithMyId {

		@Id MyId id;
	}

	@EqualsAndHashCode
	@NoArgsConstructor
	static class Sample {

		@Id String id;
		String field;

		public Sample(String id, String field) {
			this.id = id;
			this.field = field;
		}
	}

	static class TestClass {

		DateTime myDate;

		@PersistenceConstructor
		TestClass(DateTime myDate) {
			this.myDate = myDate;
		}
	}

	static class PersonWithConvertedId {

		String id;
		String name;
	}

	static enum DateTimeToDateConverter implements Converter<DateTime, Date> {

		INSTANCE;

		public Date convert(DateTime source) {
			return source == null ? null : source.toDate();
		}
	}

	static enum DateToDateTimeConverter implements Converter<Date, DateTime> {

		INSTANCE;

		public DateTime convert(Date source) {
			return source == null ? null : new DateTime(source.getTime());
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class MyPerson {

		String id;
		String name;
		Address address;

		public MyPerson(String name) {
			this.name = name;
		}
	}

	interface MyPersonProjection {

		String getName();
	}

	static class Address {

		String state;
		String city;

		Address() {}

		Address(String state, String city) {
			this.state = state;
			this.city = city;
		}

		@Override
		public boolean equals(Object obj) {

			if (obj == this) {
				return true;
			}

			if (!(obj instanceof Address)) {
				return false;
			}

			Address that = (Address) obj;

			return ObjectUtils.nullSafeEquals(this.city, that.city) && //
					ObjectUtils.nullSafeEquals(this.state, that.state);
		}

		@Override
		public int hashCode() {

			int result = 17;

			result += 31 * ObjectUtils.nullSafeHashCode(this.city);
			result += 31 * ObjectUtils.nullSafeHashCode(this.state);

			return result;
		}
	}

	static class VersionedPerson {

		@Version Long version;
		String id, firstname, lastname;
	}

	static class TypeWithFieldAnnotation {

		@Id ObjectId id;
		@Field("email") String emailAddress;
	}

	static class TypeWithDate {

		@Id String id;
		Date date;
	}

	static class ObjectWith3AliasedFields {

		@Id String id;
		@Field("prop1") String property1;
		@Field("prop2") String property2;
		@Field("prop3") String property3;
	}

	static class ObjectWith3AliasedFieldsAndNestedAddress extends ObjectWith3AliasedFields {
		@Field("adr") Address address;
	}

	static enum EnumValue {
		VALUE1, VALUE2, VALUE3
	}

	static class ObjectWithEnumValue {

		@Id String id;
		EnumValue value;
	}

	public static class SomeTemplate {

		String id;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) SomeContent content;

		public SomeContent getContent() {
			return content;
		}
	}

	@EqualsAndHashCode
	public static class SomeContent {

		String id;
		String text;
		String name;
		@org.springframework.data.mongodb.core.mapping.DBRef SomeMessage dbrefMessage;

		public String getName() {
			return name;
		}

		public void setText(String text) {
			this.text = text;

		}

		public String getId() {
			return id;
		}

		public String getText() {
			return text;
		}
	}

	static class SomeMessage {
		String id;
		@org.springframework.data.mongodb.core.mapping.DBRef SomeContent dbrefContent;
		SomeContent normalContent;
	}

	static class DocumentWithLazyDBrefUsedInPresistenceConstructor {

		@Id String id;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) Document refToDocUsedInCtor;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) Document refToDocNotUsedInCtor;

		@PersistenceConstructor
		public DocumentWithLazyDBrefUsedInPresistenceConstructor(Document refToDocUsedInCtor) {
			this.refToDocUsedInCtor = refToDocUsedInCtor;
		}

	}

	static class WithGeoJson {

		@Id String id;
		@Version //
		Integer version;
		String description;
		GeoJsonPoint point;
	}

	@Data
	static class WithObjectTypeProperty {

		@Id String id;
		Object value;
	}

	static class PersonWithIdPropertyOfTypeUUIDListener
			extends AbstractMongoEventListener<PersonWithIdPropertyOfTypeUUID> {

		@Override
		public void onBeforeConvert(BeforeConvertEvent<PersonWithIdPropertyOfTypeUUID> event) {

			PersonWithIdPropertyOfTypeUUID person = event.getSource();

			if (person.getId() != null) {
				return;
			}

			person.setId(UUID.randomUUID());
		}
	}

	public static class Message {

		private ObjectId id;

		private String text;

		private Date timestamp;

		public Message() {}

		public Message(String text) {
			super();
			this.text = text;
			this.timestamp = new Date();
		}

		public Message(String text, Date timestamp) {
			super();
			this.text = text;
			this.timestamp = timestamp;
		}

		public ObjectId getId() {
			return id;
		}

		public void setId(ObjectId id) {
			this.id = id;
		}

		public String getText() {
			return text;
		}

		public void setText(String text) {
			this.text = text;
		}

		public Date getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Date timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public String toString() {
			return "Message [id=" + id + ", text=" + text + ", timestamp=" + timestamp + "]";
		}

	}

	// DATAMONGO-1992

	@AllArgsConstructor
	@Wither
	static class ImmutableVersioned {

		final @Id String id;
		final @Version Long version;

		public ImmutableVersioned() {
			id = null;
			version = null;
		}
	}

	@Value
	@Wither
	static class ImmutableAudited {
		@Id String id;
		@LastModifiedDate Instant modified;
	}

	@Data
	static class RawStringId {

		@MongoId String id;
		String value;
	}

	static class Outer {

		@Id ObjectId id;
		Inner inner;
	}

	static class Inner {

		@Field("id") String id;
		String value;
	}
}
