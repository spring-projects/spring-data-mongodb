/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.ReflectiveWriteConcernInvoker.*;
import static org.springframework.data.mongodb.core.ReflectiveWriteResultInvoker.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.core.query.Update.*;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxy;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.Index.Duplicates;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.data.util.CloseableIterator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

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
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoTemplateTests {

	private static final org.springframework.data.util.Version TWO_DOT_FOUR = org.springframework.data.util.Version
			.parse("2.4");
	private static final org.springframework.data.util.Version TWO_DOT_EIGHT = org.springframework.data.util.Version
			.parse("2.8");
	private static final org.springframework.data.util.Version THREE_DOT_FOUR = org.springframework.data.util.Version
			.parse("3.4");

	@Autowired MongoTemplate template;
	@Autowired MongoDbFactory factory;

	ConfigurableApplicationContext context;
	MongoTemplate mappingTemplate;
	org.springframework.data.util.Version mongoVersion;

	@Rule public ExpectedException thrown = ExpectedException.none();

	@Autowired
	public void setApplicationContext(ConfigurableApplicationContext context) {

		this.context = context;

		context.addApplicationListener(new PersonWithIdPropertyOfTypeUUIDListener());
	}

	@Autowired
	public void setMongo(Mongo mongo) throws Exception {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(DateToDateTimeConverter.INSTANCE, DateTimeToDateConverter.INSTANCE));

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<Class<?>>(
				Arrays.asList(PersonWith_idPropertyOfTypeObjectId.class, PersonWith_idPropertyOfTypeString.class,
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
		queryMongoVersionIfNecessary();

		this.mappingTemplate.setApplicationContext(context);
	}

	@After
	public void cleanUp() {
		cleanDb();
	}

	private void queryMongoVersionIfNecessary() {

		if (mongoVersion == null) {
			CommandResult result = template.executeCommand("{ buildInfo: 1 }");
			mongoVersion = org.springframework.data.util.Version.parse(result.get("version").toString());
		}
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
	}

	@Test
	public void insertsSimpleEntityCorrectly() throws Exception {

		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);

		List<Person> result = template.find(new Query(Criteria.where("_id").is(person.getId())), Person.class);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(person));
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
			assertThat(e.getMessage(), containsString("E11000 duplicate key error"));
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

		thrown.expect(DataIntegrityViolationException.class);
		thrown.expectMessage("array");
		thrown.expectMessage("age");
		thrown.expectMessage("failed");

		Query query = new Query(Criteria.where("firstName").is("Amol"));
		Update upd = new Update().push("age", 29);
		template.updateFirst(query, upd, Person.class);
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
			assertThat(e.getMessage(), containsString("E11000 duplicate key error"));
		}
	}

	@Test // DATAMONGO-480
	public void rejectsDuplicateIdInInsertAll() {

		thrown.expect(DataIntegrityViolationException.class);
		thrown.expectMessage("E11000 duplicate key error");

		MongoTemplate template = new MongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		List<Person> records = new ArrayList<Person>();
		records.add(person);
		records.add(person);

		template.insertAll(records);
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testEnsureIndex() throws Exception {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1);
		Person p2 = new Person("Sven");
		p2.setAge(40);
		template.insert(p2);

		template.indexOps(Person.class).ensureIndex(new Index().on("age", Direction.DESC).unique(Duplicates.DROP));

		DBCollection coll = template.getCollection(template.getCollectionName(Person.class));
		List<DBObject> indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(2));
		String indexKey = null;
		boolean unique = false;
		boolean dropDupes = false;
		for (DBObject ix : indexInfo) {
			if ("age_-1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
				unique = (Boolean) ix.get("unique");
				if (mongoVersion.isLessThan(TWO_DOT_EIGHT)) {
					dropDupes = (Boolean) ix.get("dropDups");
					assertThat(dropDupes, is(true));
				} else {
					assertThat(ix.get("dropDups"), is(nullValue()));
				}
			}
		}
		assertThat(indexKey, is("{ \"age\" : -1}"));
		assertThat(unique, is(true));

		List<IndexInfo> indexInfoList = template.indexOps(Person.class).getIndexInfo();

		assertThat(indexInfoList.size(), is(2));
		IndexInfo ii = indexInfoList.get(1);
		assertThat(ii.isUnique(), is(true));

		if (mongoVersion.isLessThan(TWO_DOT_EIGHT)) {
			assertThat(ii.isDropDuplicates(), is(true));
		} else {
			assertThat(ii.isDropDuplicates(), is(false));
		}

		assertThat(ii.isSparse(), is(false));

		List<IndexField> indexFields = ii.getIndexFields();
		IndexField field = indexFields.get(0);

		assertThat(field, is(IndexField.create("age", Direction.DESC)));
	}

	@Test // DATAMONGO-746
	public void testReadIndexInfoForIndicesCreatedViaMongoShellCommands() throws Exception {

		String command = "db." + template.getCollectionName(Person.class)
				+ ".createIndex({'age':-1}, {'unique':true, 'sparse':true})";
		template.indexOps(Person.class).dropAllIndexes();

		assertThat(template.indexOps(Person.class).getIndexInfo().isEmpty(), is(true));
		factory.getDb().eval(command);

		List<DBObject> indexInfo = template.getCollection(template.getCollectionName(Person.class)).getIndexInfo();
		String indexKey = null;
		boolean unique = false;

		for (DBObject ix : indexInfo) {
			if ("age_-1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
				unique = (Boolean) ix.get("unique");
			}
		}

		assertThat(indexKey, is("{ \"age\" : -1.0}"));
		assertThat(unique, is(true));

		IndexInfo info = template.indexOps(Person.class).getIndexInfo().get(1);
		assertThat(info.isUnique(), is(true));
		assertThat(info.isSparse(), is(true));

		List<IndexField> indexFields = info.getIndexFields();
		IndexField field = indexFields.get(0);

		assertThat(field, is(IndexField.create("age", Direction.DESC)));
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
		assertThat(p1.getId(), notNullValue());
		PersonWithIdPropertyOfTypeString p1q = mongoTemplate.findOne(new Query(where("id").is(p1.getId())),
				PersonWithIdPropertyOfTypeString.class);
		assertThat(p1q, notNullValue());
		assertThat(p1q.getId(), is(p1.getId()));
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
		assertThat(p2.getId(), notNullValue());
		PersonWithIdPropertyOfTypeString p2q = mongoTemplate.findOne(new Query(where("id").is(p2.getId())),
				PersonWithIdPropertyOfTypeString.class);
		assertThat(p2q, notNullValue());
		assertThat(p2q.getId(), is(p2.getId()));
		checkCollectionContents(PersonWithIdPropertyOfTypeString.class, 2);

		// String _id - generated
		PersonWith_idPropertyOfTypeString p3 = new PersonWith_idPropertyOfTypeString();
		p3.setFirstName("Sven_3");
		p3.setAge(22);
		// insert
		mongoTemplate.insert(p3);
		// also try save
		mongoTemplate.save(p3);
		assertThat(p3.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeString p3q = mongoTemplate.findOne(new Query(where("_id").is(p3.get_id())),
				PersonWith_idPropertyOfTypeString.class);
		assertThat(p3q, notNullValue());
		assertThat(p3q.get_id(), is(p3.get_id()));
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
		assertThat(p4.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeString p4q = mongoTemplate.findOne(new Query(where("_id").is(p4.get_id())),
				PersonWith_idPropertyOfTypeString.class);
		assertThat(p4q, notNullValue());
		assertThat(p4q.get_id(), is(p4.get_id()));
		checkCollectionContents(PersonWith_idPropertyOfTypeString.class, 2);

		// ObjectId id - generated
		PersonWithIdPropertyOfTypeObjectId p5 = new PersonWithIdPropertyOfTypeObjectId();
		p5.setFirstName("Sven_5");
		p5.setAge(22);
		// insert
		mongoTemplate.insert(p5);
		// also try save
		mongoTemplate.save(p5);
		assertThat(p5.getId(), notNullValue());
		PersonWithIdPropertyOfTypeObjectId p5q = mongoTemplate.findOne(new Query(where("id").is(p5.getId())),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p5q, notNullValue());
		assertThat(p5q.getId(), is(p5.getId()));
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
		assertThat(p6.getId(), notNullValue());
		PersonWithIdPropertyOfTypeObjectId p6q = mongoTemplate.findOne(new Query(where("id").is(p6.getId())),
				PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(p6q, notNullValue());
		assertThat(p6q.getId(), is(p6.getId()));
		checkCollectionContents(PersonWithIdPropertyOfTypeObjectId.class, 2);

		// ObjectId _id - generated
		PersonWith_idPropertyOfTypeObjectId p7 = new PersonWith_idPropertyOfTypeObjectId();
		p7.setFirstName("Sven_7");
		p7.setAge(22);
		// insert
		mongoTemplate.insert(p7);
		// also try save
		mongoTemplate.save(p7);
		assertThat(p7.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeObjectId p7q = mongoTemplate.findOne(new Query(where("_id").is(p7.get_id())),
				PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p7q, notNullValue());
		assertThat(p7q.get_id(), is(p7.get_id()));
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
		assertThat(p8.get_id(), notNullValue());
		PersonWith_idPropertyOfTypeObjectId p8q = mongoTemplate.findOne(new Query(where("_id").is(p8.get_id())),
				PersonWith_idPropertyOfTypeObjectId.class);
		assertThat(p8q, notNullValue());
		assertThat(p8q.get_id(), is(p8.get_id()));
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
		assertThat(p9.getId(), notNullValue());
		PersonWithIdPropertyOfTypeInteger p9q = mongoTemplate.findOne(new Query(where("id").in(p9.getId())),
				PersonWithIdPropertyOfTypeInteger.class);
		assertThat(p9q, notNullValue());
		assertThat(p9q.getId(), is(p9.getId()));
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
		assertThat(p9bi.getId(), notNullValue());
		PersonWithIdPropertyOfTypeBigInteger p9qbi = mongoTemplate.findOne(new Query(where("id").in(p9bi.getId())),
				PersonWithIdPropertyOfTypeBigInteger.class);
		assertThat(p9qbi, notNullValue());
		assertThat(p9qbi.getId(), is(p9bi.getId()));
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
		assertThat(p10.getId(), notNullValue());
		PersonWithIdPropertyOfPrimitiveInt p10q = mongoTemplate.findOne(new Query(where("id").in(p10.getId())),
				PersonWithIdPropertyOfPrimitiveInt.class);
		assertThat(p10q, notNullValue());
		assertThat(p10q.getId(), is(p10.getId()));
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
		assertThat(p11.getId(), notNullValue());
		PersonWithIdPropertyOfTypeLong p11q = mongoTemplate.findOne(new Query(where("id").in(p11.getId())),
				PersonWithIdPropertyOfTypeLong.class);
		assertThat(p11q, notNullValue());
		assertThat(p11q.getId(), is(p11.getId()));
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
		assertThat(p12.getId(), notNullValue());
		PersonWithIdPropertyOfPrimitiveLong p12q = mongoTemplate.findOne(new Query(where("id").in(p12.getId())),
				PersonWithIdPropertyOfPrimitiveLong.class);
		assertThat(p12q, notNullValue());
		assertThat(p12q.getId(), is(p12.getId()));
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
		assertThat(p13.getId(), notNullValue());
		PersonWithIdPropertyOfTypeUUID p13q = mongoTemplate.findOne(new Query(where("id").in(p13.getId())),
				PersonWithIdPropertyOfTypeUUID.class);
		assertThat(p13q, notNullValue());
		assertThat(p13q.getId(), is(p13.getId()));
		checkCollectionContents(PersonWithIdPropertyOfTypeUUID.class, 1);
	}

	private void checkCollectionContents(Class<?> entityClass, int count) {
		assertThat(template.findAll(entityClass).size(), is(count));
	}

	@Test // DATAMONGO-234
	public void testFindAndUpdate() {

		template.insert(new Person("Tom", 21));
		template.insert(new Person("Dick", 22));
		template.insert(new Person("Harry", 23));

		Query query = new Query(Criteria.where("firstName").is("Harry"));
		Update update = new Update().inc("age", 1);
		Person p = template.findAndModify(query, update, Person.class); // return old
		assertThat(p.getFirstName(), is("Harry"));
		assertThat(p.getAge(), is(23));
		p = template.findOne(query, Person.class);
		assertThat(p.getAge(), is(24));

		p = template.findAndModify(query, update, Person.class, "person");
		assertThat(p.getAge(), is(24));
		p = template.findOne(query, Person.class);
		assertThat(p.getAge(), is(25));

		p = template.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), Person.class);
		assertThat(p.getAge(), is(26));

		p = template.findAndModify(query, update, null, Person.class, "person");
		assertThat(p.getAge(), is(26));
		p = template.findOne(query, Person.class);
		assertThat(p.getAge(), is(27));

		Query query2 = new Query(Criteria.where("firstName").is("Mary"));
		p = template.findAndModify(query2, update, new FindAndModifyOptions().returnNew(true).upsert(true), Person.class);
		assertThat(p.getFirstName(), is("Mary"));
		assertThat(p.getAge(), is(1));

	}

	@Test
	public void testFindAndUpdateUpsert() {
		template.insert(new Person("Tom", 21));
		template.insert(new Person("Dick", 22));
		Query query = new Query(Criteria.where("firstName").is("Harry"));
		Update update = new Update().set("age", 23);
		Person p = template.findAndModify(query, update, new FindAndModifyOptions().upsert(true).returnNew(true),
				Person.class);
		assertThat(p.getFirstName(), is("Harry"));
		assertThat(p.getAge(), is(23));
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
		assertThat(found1, notNullValue());
		assertThat(found2, notNullValue());
		assertThat(notFound, nullValue());
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
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(1));
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
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
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
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
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
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
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
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(2));
		assertThat(results3.size(), is(2));
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

		List<Integer> l1 = new ArrayList<Integer>();
		l1.add(11);
		l1.add(21);
		l1.add(41);
		Query q1 = new Query(Criteria.where("age").in(l1));
		List<PersonWithIdPropertyOfTypeObjectId> results1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		Query q2 = new Query(Criteria.where("age").in(l1.toArray()));
		List<PersonWithIdPropertyOfTypeObjectId> results2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(results1.size(), is(3));
		assertThat(results2.size(), is(3));
		try {
			List<Integer> l2 = new ArrayList<Integer>();
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
		assertThat(results1.size(), is(1));
		assertThat(results2.size(), is(2));
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
		assertThat(results.size(), is(3));
		for (PersonWithIdPropertyOfTypeObjectId p : results) {
			assertThat(p.getAge(), isOneOf(11, 21, 31));
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

		WriteResult wr = template.updateMulti(new Query(), u, PersonWithIdPropertyOfTypeObjectId.class);

		if (wasAcknowledged(wr)) {
			assertThat(wr.getN(), is(2));
		}

		Query q1 = new Query(Criteria.where("age").in(11, 21));
		List<PersonWithIdPropertyOfTypeObjectId> r1 = template.find(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(r1.size(), is(0));
		Query q2 = new Query(Criteria.where("age").is(10));
		List<PersonWithIdPropertyOfTypeObjectId> r2 = template.find(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(r2.size(), is(2));
		for (PersonWithIdPropertyOfTypeObjectId p : r2) {
			assertThat(p.getAge(), is(10));
			assertThat(p.getFirstName(), is("Bob"));
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
		assertThat(found1, notNullValue());
		Query _q = new Query(Criteria.where("_id").is(p1.getId()));
		template.remove(_q, PersonWithIdPropertyOfTypeObjectId.class);
		PersonWithIdPropertyOfTypeObjectId notFound1 = template.findOne(q1, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(notFound1, nullValue());

		PersonWithIdPropertyOfTypeObjectId p2 = new PersonWithIdPropertyOfTypeObjectId();
		p2.setFirstName("Bubba_to_be_removed");
		p2.setAge(51);
		template.insert(p2);

		Query q2 = new Query(Criteria.where("id").is(p2.getId()));
		PersonWithIdPropertyOfTypeObjectId found2 = template.findOne(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(found2, notNullValue());
		template.remove(q2, PersonWithIdPropertyOfTypeObjectId.class);
		PersonWithIdPropertyOfTypeObjectId notFound2 = template.findOne(q2, PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(notFound2, nullValue());
	}

	@Test
	public void testAddingToList() {
		PersonWithAList p = new PersonWithAList();
		p.setFirstName("Sven");
		p.setAge(22);
		template.insert(p);

		Query q1 = new Query(Criteria.where("id").is(p.getId()));
		PersonWithAList p2 = template.findOne(q1, PersonWithAList.class);
		assertThat(p2, notNullValue());
		assertThat(p2.getWishList().size(), is(0));

		p2.addToWishList("please work!");

		template.save(p2);

		PersonWithAList p3 = template.findOne(q1, PersonWithAList.class);
		assertThat(p3, notNullValue());
		assertThat(p3.getWishList().size(), is(1));

		Friend f = new Friend();
		p.setFirstName("Erik");
		p.setAge(21);

		p3.addFriend(f);
		template.save(p3);

		PersonWithAList p4 = template.findOne(q1, PersonWithAList.class);
		assertThat(p4, notNullValue());
		assertThat(p4.getWishList().size(), is(1));
		assertThat(p4.getFriends().size(), is(1));

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
		q2.with(new Sort(Direction.DESC, "age"));
		PersonWithAList p5 = template.findOne(q2, PersonWithAList.class);
		assertThat(p5.getFirstName(), is("Mark"));
	}

	@Test
	public void testUsingReadPreference() throws Exception {
		this.template.execute("readPref", new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				assertThat(collection.getOptions(), is(0));
				assertThat(collection.getDB().getOptions(), is(0));
				return null;
			}
		});
		MongoTemplate slaveTemplate = new MongoTemplate(factory);
		slaveTemplate.setReadPreference(ReadPreference.secondary());
		slaveTemplate.execute("readPref", new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				assertThat(collection.getReadPreference(), is(ReadPreference.secondary()));
				assertThat(collection.getDB().getOptions(), is(0));
				return null;
			}
		});
	}

	@Test // DATADOC-166
	public void removingNullIsANoOp() {
		template.remove(null);
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
		assertThat(result, is(notNullValue()));
		assertThat(result.getId(), is(person.getId()));
		assertThat(result.getFirstName(), is("Carter"));
	}

	@Test
	public void testWriteConcernResolver() {

		PersonWithIdPropertyOfTypeObjectId person = new PersonWithIdPropertyOfTypeObjectId();
		person.setId(new ObjectId());
		person.setFirstName("Dave");

		template.setWriteConcern(noneOrUnacknowledged());
		template.save(person);
		WriteResult result = template.updateFirst(query(where("id").is(person.getId())), update("firstName", "Carter"),
				PersonWithIdPropertyOfTypeObjectId.class);

		FsyncSafeWriteConcernResolver resolver = new FsyncSafeWriteConcernResolver();
		template.setWriteConcernResolver(resolver);
		Query q = query(where("_id").is(person.getId()));
		Update u = update("firstName", "Carter");
		result = template.updateFirst(q, u, PersonWithIdPropertyOfTypeObjectId.class);

		MongoAction lastMongoAction = resolver.getMongoAction();
		assertThat(lastMongoAction.getCollectionName(), is("personWithIdPropertyOfTypeObjectId"));
		assertThat(lastMongoAction.getDefaultWriteConcern(), equalTo(noneOrUnacknowledged()));
		assertThat(lastMongoAction.getDocument(), notNullValue());
		assertThat(lastMongoAction.getEntityType().toString(), is(PersonWithIdPropertyOfTypeObjectId.class.toString()));
		assertThat(lastMongoAction.getMongoActionOperation(), is(MongoActionOperation.UPDATE));
		assertThat(lastMongoAction.getQuery(), equalTo(q.getQueryObject()));
	}

	private class FsyncSafeWriteConcernResolver implements WriteConcernResolver {

		private MongoAction mongoAction;

		public WriteConcern resolve(MongoAction action) {
			this.mongoAction = action;
			return WriteConcern.FSYNC_SAFE;
		}

		public MongoAction getMongoAction() {
			return mongoAction;
		}
	}

	@Test // DATADOC-246
	public void updatesDBRefsCorrectly() {

		DBRef first = new DBRef("foo", new ObjectId());
		DBRef second = new DBRef("bar", new ObjectId());

		template.updateFirst(null, update("dbRefs", Arrays.asList(first, second)), ClassWithDBRefs.class);
	}

	class ClassWithDBRefs {
		List<DBRef> dbrefs;
	}

	@Test // DATADOC-202
	public void executeDocument() {
		template.insert(new Person("Tom"));
		template.insert(new Person("Dick"));
		template.insert(new Person("Harry"));
		final List<String> names = new ArrayList<String>();
		template.executeQuery(new Query(), template.getCollectionName(Person.class), new DocumentCallbackHandler() {
			public void processDocument(DBObject dbObject) {
				String name = (String) dbObject.get("firstName");
				if (name != null) {
					names.add(name);
				}
			}
		});
		assertEquals(3, names.size());
		// template.remove(new Query(), Person.class);
	}

	@Test // DATADOC-202
	public void executeDocumentWithCursorPreparer() {
		template.insert(new Person("Tom"));
		template.insert(new Person("Dick"));
		template.insert(new Person("Harry"));
		final List<String> names = new ArrayList<String>();
		template.executeQuery(new Query(), template.getCollectionName(Person.class), new DocumentCallbackHandler() {
			public void processDocument(DBObject dbObject) {
				String name = (String) dbObject.get("firstName");
				if (name != null) {
					names.add(name);
				}
			}
		}, new CursorPreparer() {

			public DBCursor prepare(DBCursor cursor) {
				cursor.limit(1);
				return cursor;
			}

		});
		assertEquals(1, names.size());
		// template.remove(new Query(), Person.class);
	}

	@Test // DATADOC-183
	public void countsDocumentsCorrectly() {

		assertThat(template.count(new Query(), Person.class), is(0L));

		Person dave = new Person("Dave");
		Person carter = new Person("Carter");

		template.save(dave);
		template.save(carter);

		assertThat(template.count(null, Person.class), is(2L));
		assertThat(template.count(query(where("firstName").is("Carter")), Person.class), is(1L));
	}

	@Test(expected = IllegalArgumentException.class) // DATADOC-183
	public void countRejectsNullEntityClass() {
		template.count(null, (Class<?>) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATADOC-183
	public void countRejectsEmptyCollectionName() {
		template.count(null, "");
	}

	@Test(expected = IllegalArgumentException.class) // DATADOC-183
	public void countRejectsNullCollectionName() {
		template.count(null, (String) null);
	}

	@Test
	public void returnsEntityWhenQueryingForDateTime() {

		DateTime dateTime = new DateTime(2011, 3, 3, 12, 0, 0, 0);
		TestClass testClass = new TestClass(dateTime);
		mappingTemplate.save(testClass);

		List<TestClass> testClassList = mappingTemplate.find(new Query(Criteria.where("myDate").is(dateTime.toDate())),
				TestClass.class);
		assertThat(testClassList.size(), is(1));
		assertThat(testClassList.get(0).myDate, is(testClass.myDate));
	}

	@Test // DATADOC-230
	public void removesEntityFromCollection() {

		template.remove(new Query(), "mycollection");

		Person person = new Person("Dave");

		template.save(person, "mycollection");
		assertThat(template.findAll(TestClass.class, "mycollection").size(), is(1));

		template.remove(person, "mycollection");
		assertThat(template.findAll(Person.class, "mycollection").isEmpty(), is(true));
	}

	@Test // DATADOC-349
	public void removesEntityWithAnnotatedIdIfIdNeedsMassaging() {

		String id = new ObjectId().toString();

		Sample sample = new Sample();
		sample.id = id;

		template.save(sample);

		assertThat(template.findOne(query(where("id").is(id)), Sample.class).id, is(id));

		template.remove(sample);
		assertThat(template.findOne(query(where("id").is(id)), Sample.class), is(nullValue()));
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
		assertThat(result.size(), is(1));
		assertThat(result.get(0).field, is("Beauford"));
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

		assertThat(result, hasSize(1));
		assertThat(result.get(0), hasProperty("name", is("Oleg")));

		query = new BasicQuery("{'address.state' : 'PA' }");
		result = template.find(query, MyPerson.class);

		assertThat(result, hasSize(1));
		assertThat(result.get(0), hasProperty("name", is("Oleg")));
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

		assertThat(result, hasSize(1));
		assertThat(result.get(0).version, is(0));

		// Version change
		person = result.get(0);
		person.firstName = "Patryk2";

		template.save(person);

		assertThat(person.version, is(1));

		result = mappingTemplate.findAll(PersonWithVersionPropertyOfTypeInteger.class);

		assertThat(result, hasSize(1));
		assertThat(result.get(0).version, is(1));

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

		DBObject dbObject = new BasicDBObject();
		dbObject.put("firstName", "Oliver");

		template.insert(dbObject, template.determineCollectionName(PersonWithVersionPropertyOfTypeInteger.class));
	}

	@Test // DATAMONGO-1617
	public void doesNotFailOnInsertForEntityWithNonAutogeneratableId() {

		PersonWithIdPropertyOfTypeUUID person = new PersonWithIdPropertyOfTypeUUID();
		person.setFirstName("Laszlo");
		person.setAge(33);

		template.insert(person);
		assertThat(person.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-539
	public void removesObjectFromExplicitCollection() {

		String collectionName = "explicit";
		template.remove(new Query(), collectionName);

		PersonWithConvertedId person = new PersonWithConvertedId();
		person.name = "Dave";
		template.save(person, collectionName);
		assertThat(template.findAll(PersonWithConvertedId.class, collectionName).isEmpty(), is(false));

		template.remove(person, collectionName);
		assertThat(template.findAll(PersonWithConvertedId.class, collectionName).isEmpty(), is(true));
	}

	// DATAMONGO-549
	public void savesMapCorrectly() {

		Map<String, String> map = new HashMap<String, String>();
		map.put("key", "value");

		template.save(map, "maps");
	}

	@Test(expected = MappingException.class) // DATAMONGO-549
	public void savesMongoPrimitiveObjectCorrectly() {
		template.save(new Object(), "collection");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-549
	public void rejectsNullObjectToBeSaved() {
		template.save(null);
	}

	@Test // DATAMONGO-550
	public void savesPlainDbObjectCorrectly() {

		DBObject dbObject = new BasicDBObject("foo", "bar");
		template.save(dbObject, "collection");

		assertThat(dbObject.containsField("_id"), is(true));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-550
	public void rejectsPlainObjectWithOutExplicitCollection() {

		DBObject dbObject = new BasicDBObject("foo", "bar");
		template.save(dbObject, "collection");

		template.findById(dbObject.get("_id"), DBObject.class);
	}

	@Test // DATAMONGO-550
	public void readsPlainDbObjectById() {

		DBObject dbObject = new BasicDBObject("foo", "bar");
		template.save(dbObject, "collection");

		DBObject result = template.findById(dbObject.get("_id"), DBObject.class, "collection");
		assertThat(result.get("foo"), is(dbObject.get("foo")));
		assertThat(result.get("_id"), is(dbObject.get("_id")));
	}

	@Test // DATAMONGO-551
	public void writesPlainString() {
		template.save("{ 'foo' : 'bar' }", "collection");
	}

	@Test(expected = MappingException.class) // DATAMONGO-551
	public void rejectsNonJsonStringForSave() {
		template.save("Foobar!", "collection");
	}

	@Test // DATAMONGO-588
	public void initializesVersionOnInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insert(person);

		assertThat(person.version, is(0));
	}

	@Test // DATAMONGO-588
	public void initializesVersionOnBatchInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insertAll(Arrays.asList(person));

		assertThat(person.version, is(0));
	}

	@Test // DATAMONGO-568
	public void queryCantBeNull() {

		List<PersonWithIdPropertyOfTypeObjectId> result = template.findAll(PersonWithIdPropertyOfTypeObjectId.class);
		assertThat(template.find(null, PersonWithIdPropertyOfTypeObjectId.class), is(result));
	}

	@Test // DATAMONGO-620
	public void versionsObjectIntoDedicatedCollection() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.save(person, "personX");
		assertThat(person.version, is(0));

		template.save(person, "personX");
		assertThat(person.version, is(1));
	}

	@Test // DATAMONGO-621
	public void correctlySetsLongVersionProperty() {

		PersonWithVersionPropertyOfTypeLong person = new PersonWithVersionPropertyOfTypeLong();
		person.firstName = "Dave";

		template.save(person);
		assertThat(person.version, is(0L));
	}

	@Test(expected = DuplicateKeyException.class) // DATAMONGO-622
	public void preventsDuplicateInsert() {

		template.setWriteConcern(WriteConcern.SAFE);

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.save(person);
		assertThat(person.version, is(0));

		person.version = null;
		template.save(person);
	}

	@Test // DATAMONGO-629
	public void countAndFindWithoutTypeInformation() {

		Person person = new Person();
		template.save(person);

		Query query = query(where("_id").is(person.getId()));
		String collectionName = template.getCollectionName(Person.class);

		assertThat(template.find(query, HashMap.class, collectionName), hasSize(1));
		assertThat(template.count(query, collectionName), is(1L));
	}

	@Test // DATAMONGO-571
	public void nullsPropertiesForVersionObjectUpdates() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";

		template.save(person);
		assertThat(person.id, is(notNullValue()));

		person.lastname = null;
		template.save(person);

		person = template.findOne(query(where("id").is(person.id)), VersionedPerson.class);
		assertThat(person.lastname, is(nullValue()));
	}

	@Test // DATAMONGO-571
	public void nullsValuesForUpdatesOfUnversionedEntity() {

		Person person = new Person("Dave");
		template.save(person);

		person.setFirstName(null);
		template.save(person);

		person = template.findOne(query(where("id").is(person.getId())), Person.class);
		assertThat(person.getFirstName(), is(nullValue()));
	}

	@Test // DATAMONGO-679
	public void savesJsonStringCorrectly() {

		DBObject dbObject = new BasicDBObject().append("first", "first").append("second", "second");

		template.save(dbObject.toString(), "collection");

		List<DBObject> result = template.findAll(DBObject.class, "collection");
		assertThat(result.size(), is(1));
		assertThat(result.get(0).containsField("first"), is(true));
	}

	@Test
	public void executesExistsCorrectly() {

		Sample sample = new Sample();
		template.save(sample);

		Query query = query(where("id").is(sample.id));

		assertThat(template.exists(query, Sample.class), is(true));
		assertThat(template.exists(query(where("_id").is(sample.id)), template.getCollectionName(Sample.class)), is(true));
		assertThat(template.exists(query, Sample.class, template.getCollectionName(Sample.class)), is(true));
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
		assertThat(result.emailAddress, is("new"));
	}

	@Test // DATAMONGO-671
	public void findsEntityByDateReference() {

		TypeWithDate entity = new TypeWithDate();
		entity.date = new Date(System.currentTimeMillis() - 10);
		template.save(entity);

		Query query = query(where("date").lt(new Date()));
		List<TypeWithDate> result = template.find(query, TypeWithDate.class);

		assertThat(result, hasSize(1));
		assertThat(result.get(0).date, is(notNullValue()));
	}

	@Test // DATAMONGO-540
	public void findOneAfterUpsertForNonExistingObjectReturnsTheInsertedObject() {

		String idValue = "4711";
		Query query = new Query(Criteria.where("id").is(idValue));

		String fieldValue = "bubu";
		Update update = Update.update("field", fieldValue);

		template.upsert(query, update, Sample.class);
		Sample result = template.findOne(query, Sample.class);

		assertThat(result, is(notNullValue()));
		assertThat(result.field, is(fieldValue));
		assertThat(result.id, is(idValue));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.id, is(doc.id));
		assertThat(result.model, is(notNullValue()));
		assertThat(result.model.value(), is(newModelValue));
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

		assertThat(result.id, is(obj.id));
		assertThat(result.property1, is(nullValue()));
		assertThat(result.property2, is(obj.property2));
		assertThat(result.property3, is(obj.property3));
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

		assertThat(result.id, is(obj.id));
		assertThat(result.property1, is(obj.property1));
		assertThat(result.property2, is(nullValue()));
		assertThat(result.property3, is(nullValue()));
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

		assertThat(results, is(notNullValue()));
		assertThat(results.size(), is(2));

		ObjectWith3AliasedFields result0 = results.get(0);
		assertThat(result0, is(notNullValue()));
		assertThat(result0.id, is(obj0.id));
		assertThat(result0.property1, is(obj0.property1));
		assertThat(result0.property2, is(nullValue()));
		assertThat(result0.property3, is(nullValue()));

		ObjectWith3AliasedFields result1 = results.get(1);
		assertThat(result1, is(notNullValue()));
		assertThat(result1.id, is(obj1.id));
		assertThat(result1.property1, is(obj1.property1));
		assertThat(result1.property2, is(nullValue()));
		assertThat(result1.property3, is(nullValue()));
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

		assertThat(result.id, is(obj.id));
		assertThat(result.property1, is(nullValue()));
		assertThat(result.property2, is(obj.property2));
		assertThat(result.property3, is(nullValue()));
		assertThat(result.address, is(notNullValue()));
		assertThat(result.address.city, is(nullValue()));
		assertThat(result.address.state, is(stateValue));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(instanceOf(SpecialDoc.class)));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(2));
		assertThat(result.get(0).getClass(), is((Object) BaseDoc.class));
		assertThat(result.get(1).getClass(), is((Object) VerySpecialDoc.class));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(3));
		assertThat(result.get(0).getClass(), is((Object) BaseDoc.class));
		assertThat(result.get(1).getClass(), is((Object) SpecialDoc.class));
		assertThat(result.get(2).getClass(), is((Object) VerySpecialDoc.class));
	}

	@Test // DATAMONGO-771
	public void allowInsertWithPlainJsonString() {

		String id = "4711";
		String value = "bubu";
		String json = String.format("{_id:%s, field: '%s'}", id, value);

		template.insert(json, "sample");
		List<Sample> result = template.findAll(Sample.class);

		assertThat(result.size(), is(1));
		assertThat(result.get(0).id, is(id));
		assertThat(result.get(0).field, is(value));
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
					public void processDocument(DBObject dbObject) throws MongoException, DataAccessException {

						assertThat(dbObject, is(notNullValue()));

						ObjectWithEnumValue result = template.getConverter().read(ObjectWithEnumValue.class, dbObject);

						assertThat(result.value, is(EnumValue.VALUE2));
					}
				});
	}

	@Test // DATAMONGO-811
	public void updateFirstShouldIncreaseVersionForVersionedEntity() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";
		template.save(person);
		assertThat(person.id, is(notNullValue()));

		Query qry = query(where("id").is(person.id));
		VersionedPerson personAfterFirstSave = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterFirstSave.version, is(0L));

		template.updateFirst(qry, Update.update("lastname", "Bubu"), VersionedPerson.class);

		VersionedPerson personAfterUpdateFirst = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterUpdateFirst.version, is(1L));
		assertThat(personAfterUpdateFirst.lastname, is("Bubu"));
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
				assertThat(p.version, equalTo(Long.valueOf(1)));
			} else {
				assertThat(p.version, equalTo(Long.valueOf(0)));
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
			assertThat(p.version, equalTo(Long.valueOf(1)));
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

		assertThat(template.count(query, Sample.class), is(1L));

		query.with(new PageRequest(0, 10));
		query.with(new Sort("field"));

		assertThat(template.find(query, Sample.class), is(not(empty())));
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
		assertThat(retrieved.model, instanceOf(ModelA.class));
		assertThat(retrieved.model.value(), equalTo("value2"));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldRetainTypeInformationWithinUpdatedTypeOnDocumentWithNestedCollectionWhenWholeCollectionIsReplaced() {

		DocumentWithNestedCollection doc = new DocumentWithNestedCollection();

		Map<String, Model> entry = new HashMap<String, Model>();
		entry.put("key1", new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		entry.put("key2", new ModelA("value2"));

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("models", Collections.singletonList(entry));

		assertThat(template.findOne(query, DocumentWithNestedCollection.class), notNullValue());

		template.findAndModify(query, update, DocumentWithNestedCollection.class);

		DocumentWithNestedCollection retrieved = template.findOne(query, DocumentWithNestedCollection.class);

		assertThat(retrieved, is(notNullValue()));
		assertThat(retrieved.id, is(doc.id));

		assertThat(retrieved.models.get(0).entrySet(), hasSize(2));

		assertThat(retrieved.models.get(0).get("key1"), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get("key1").value(), equalTo("value1"));

		assertThat(retrieved.models.get(0).get("key2"), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get("key2").value(), equalTo("value2"));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldRetainTypeInformationWithinUpdatedTypeOnDocumentWithNestedCollectionWhenFirstElementIsReplaced() {

		DocumentWithNestedCollection doc = new DocumentWithNestedCollection();

		Map<String, Model> entry = new HashMap<String, Model>();
		entry.put("key1", new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		entry.put("key2", new ModelA("value2"));

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("models.0", entry);

		assertThat(template.findOne(query, DocumentWithNestedCollection.class), notNullValue());

		template.findAndModify(query, update, DocumentWithNestedCollection.class);

		DocumentWithNestedCollection retrieved = template.findOne(query, DocumentWithNestedCollection.class);

		assertThat(retrieved, is(notNullValue()));
		assertThat(retrieved.id, is(doc.id));

		assertThat(retrieved.models.get(0).entrySet(), hasSize(2));

		assertThat(retrieved.models.get(0).get("key1"), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get("key1").value(), equalTo("value1"));

		assertThat(retrieved.models.get(0).get("key2"), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get("key2").value(), equalTo("value2"));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationOnDocumentWithNestedCollectionObjectInsertedAtSecondIndex() {

		DocumentWithNestedCollection doc = new DocumentWithNestedCollection();

		Map<String, Model> entry = new HashMap<String, Model>();
		entry.put("key1", new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("models.1", Collections.singletonMap("key2", new ModelA("value2")));

		assertThat(template.findOne(query, DocumentWithNestedCollection.class), notNullValue());

		template.findAndModify(query, update, DocumentWithNestedCollection.class);

		DocumentWithNestedCollection retrieved = template.findOne(query, DocumentWithNestedCollection.class);

		assertThat(retrieved, is(notNullValue()));
		assertThat(retrieved.id, is(doc.id));

		assertThat(retrieved.models.get(0).entrySet(), hasSize(1));
		assertThat(retrieved.models.get(1).entrySet(), hasSize(1));

		assertThat(retrieved.models.get(0).get("key1"), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get("key1").value(), equalTo("value1"));

		assertThat(retrieved.models.get(1).get("key2"), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(1).get("key2").value(), equalTo("value2"));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldRetainTypeInformationWithinUpdatedTypeOnEmbeddedDocumentWithCollectionWhenUpdatingPositionedElement()
			throws Exception {

		List<Model> models = new ArrayList<Model>();
		models.add(new ModelA("value1"));

		DocumentWithEmbeddedDocumentWithCollection doc = new DocumentWithEmbeddedDocumentWithCollection(
				new DocumentWithCollection(models));

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("embeddedDocument.models.0", new ModelA("value2"));

		assertThat(template.findOne(query, DocumentWithEmbeddedDocumentWithCollection.class), notNullValue());

		template.findAndModify(query, update, DocumentWithEmbeddedDocumentWithCollection.class);

		DocumentWithEmbeddedDocumentWithCollection retrieved = template.findOne(query,
				DocumentWithEmbeddedDocumentWithCollection.class);

		assertThat(retrieved, notNullValue());
		assertThat(retrieved.embeddedDocument.models, hasSize(1));
		assertThat(retrieved.embeddedDocument.models.get(0).value(), is("value2"));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationWithinUpdatedTypeOnEmbeddedDocumentWithCollectionWhenUpdatingSecondElement()
			throws Exception {

		List<Model> models = new ArrayList<Model>();
		models.add(new ModelA("value1"));

		DocumentWithEmbeddedDocumentWithCollection doc = new DocumentWithEmbeddedDocumentWithCollection(
				new DocumentWithCollection(models));

		template.save(doc);

		Query query = query(where("id").is(doc.id));
		Update update = Update.update("embeddedDocument.models.1", new ModelA("value2"));

		assertThat(template.findOne(query, DocumentWithEmbeddedDocumentWithCollection.class), notNullValue());

		template.findAndModify(query, update, DocumentWithEmbeddedDocumentWithCollection.class);

		DocumentWithEmbeddedDocumentWithCollection retrieved = template.findOne(query,
				DocumentWithEmbeddedDocumentWithCollection.class);

		assertThat(retrieved, notNullValue());
		assertThat(retrieved.embeddedDocument.models, hasSize(2));
		assertThat(retrieved.embeddedDocument.models.get(0).value(), is("value1"));
		assertThat(retrieved.embeddedDocument.models.get(1).value(), is("value2"));
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

		assertThat(template.findOne(query, DocumentWithEmbeddedDocumentWithCollection.class), notNullValue());

		template.findAndModify(query, update, DocumentWithEmbeddedDocumentWithCollection.class);

		DocumentWithEmbeddedDocumentWithCollection retrieved = template.findOne(query,
				DocumentWithEmbeddedDocumentWithCollection.class);

		assertThat(retrieved, notNullValue());
		assertThat(retrieved.embeddedDocument.models, hasSize(1));
		assertThat(retrieved.embeddedDocument.models.get(0).value(), is("value2"));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyShouldAddTypeInformationWithinUpdatedTypeOnDocumentWithNestedLists() {

		DocumentWithNestedList doc = new DocumentWithNestedList();

		List<Model> entry = new ArrayList<Model>();
		entry.add(new ModelA("value1"));
		doc.models.add(entry);

		template.save(doc);

		Query query = query(where("id").is(doc.id));

		assertThat(template.findOne(query, DocumentWithNestedList.class), notNullValue());

		Update update = Update.update("models.0.1", new ModelA("value2"));

		template.findAndModify(query, update, DocumentWithNestedList.class);

		DocumentWithNestedList retrieved = template.findOne(query, DocumentWithNestedList.class);

		assertThat(retrieved, is(notNullValue()));
		assertThat(retrieved.id, is(doc.id));

		assertThat(retrieved.models.get(0), hasSize(2));

		assertThat(retrieved.models.get(0).get(0), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get(0).value(), equalTo("value1"));

		assertThat(retrieved.models.get(0).get(1), instanceOf(ModelA.class));
		assertThat(retrieved.models.get(0).get(1).value(), equalTo("value2"));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.id, is(doc.id));
		assertThat(result.models, is(notNullValue()));
		assertThat(result.models, hasSize(1));
		assertThat(result.models.get(0).value(), is(newModelValue));
	}

	@Test // DATAMONGO-812
	public void updateMultiShouldAddValuesCorrectlyWhenUsingPushEachWithComplexTypes() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(TWO_DOT_FOUR), is(true));

		DocumentWithCollection document = new DocumentWithCollection(Collections.<Model> emptyList());
		template.save(document);
		Query query = query(where("id").is(document.id));
		assumeThat(template.findOne(query, DocumentWithCollection.class).models, hasSize(1));

		Update update = new Update().push("models").each(new ModelA("model-b"), new ModelA("model-c"));
		template.updateMulti(query, update, DocumentWithCollection.class);

		assertThat(template.findOne(query, DocumentWithCollection.class).models, hasSize(3));
	}

	@Test // DATAMONGO-812
	public void updateMultiShouldAddValuesCorrectlyWhenUsingPushEachWithSimpleTypes() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(TWO_DOT_FOUR), is(true));

		DocumentWithCollectionOfSimpleType document = new DocumentWithCollectionOfSimpleType();
		document.values = Arrays.asList("spring");
		template.save(document);

		Query query = query(where("id").is(document.id));
		assumeThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values, hasSize(1));

		Update update = new Update().push("values").each("data", "mongodb");
		template.updateMulti(query, update, DocumentWithCollectionOfSimpleType.class);

		assertThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values, hasSize(3));
	}

	@Test // DATAMONOGO-828
	public void updateFirstShouldDoNothingWhenCalledForEntitiesThatDoNotExist() {

		Query q = query(where("id").is(Long.MIN_VALUE));

		template.updateFirst(q, Update.update("lastname", "supercalifragilisticexpialidocious"), VersionedPerson.class);
		assertThat(template.findOne(q, VersionedPerson.class), nullValue());
	}

	@Test // DATAMONGO-354
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
		assertThat(result.string1, hasItems("spring", "data", "mongodb"));
		assertThat(result.string2, hasItems("one", "two", "three"));

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

		assertThat(result, is(notNullValue()));
		assertThat(result.dbRefAnnotatedList, hasSize(1));
		assertThat(result.dbRefAnnotatedList.get(0), is(notNullValue()));
		assertThat(result.dbRefAnnotatedList.get(0).id, is((Object) "1"));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.dbRefAnnotatedList, hasSize(1));
		assertThat(result.dbRefAnnotatedList.get(0), is(notNullValue()));
		assertThat(result.dbRefAnnotatedList.get(0).id, is((Object) "1"));
	}

	@Test // DATAMONGO-852
	public void updateShouldNotBumpVersionNumberIfVersionPropertyIncludedInUpdate() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";
		template.save(person);
		assertThat(person.id, is(notNullValue()));

		Query qry = query(where("id").is(person.id));
		VersionedPerson personAfterFirstSave = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterFirstSave.version, is(0L));

		template.updateFirst(qry, Update.update("lastname", "Bubu").set("version", 100L), VersionedPerson.class);

		VersionedPerson personAfterUpdateFirst = template.findOne(qry, VersionedPerson.class);
		assertThat(personAfterUpdateFirst.version, is(100L));
		assertThat(personAfterUpdateFirst.lastname, is("Bubu"));
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

		assertThat(updatedDoc, is(notNullValue()));
		assertThat(updatedDoc.dbRefProperty, is(notNullValue()));
		assertThat(updatedDoc.dbRefProperty.id, is(sample2.id));
		assertThat(updatedDoc.dbRefProperty.field, is(sample2.field));
	}

	@Test // DATAMONGO-862
	public void testUpdateShouldWorkForPathsOnInterfaceMethods() {

		DocumentWithCollection document = new DocumentWithCollection(
				Arrays.<Model> asList(new ModelA("spring"), new ModelA("data")));

		template.save(document);

		Query query = query(where("id").is(document.id).and("models._id").exists(true));
		Update update = new Update().set("models.$.value", "mongodb");
		template.findAndModify(query, update, DocumentWithCollection.class);

		DocumentWithCollection result = template.findOne(query(where("id").is(document.id)), DocumentWithCollection.class);
		assertThat(result.models.get(0).value(), is("mongodb"));
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

		assertThat(result, is(notNullValue()));
		assertThat(result, hasSize(1));
		assertThat(result.get(0), is(notNullValue()));
		assertThat(result.get(0).dbRefProperty, is(notNullValue()));
		assertThat(result.get(0).dbRefProperty.field, is(sample.field));
	}

	@Test // DATAMONGO-566
	public void testFindAllAndRemoveFullyReturnsAndRemovesDocuments() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");
		template.insert(Arrays.asList(spring, data, mongodb), Sample.class);

		Query qry = query(where("field").in("spring", "mongodb"));
		List<Sample> result = template.findAllAndRemove(qry, Sample.class);

		assertThat(result, hasSize(2));

		assertThat(
				template.getDb().getCollection("sample")
						.find(new BasicDBObject("field", new BasicDBObject("$in", Arrays.asList("spring", "mongodb")))).count(),
				is(0));
		assertThat(template.getDb().getCollection("sample").find(new BasicDBObject("field", "data")).count(), is(1));
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

		assertThat(template.findById(content.id, SomeContent.class).getText(), is("data"));

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

		assertThat(savedMessage.dbrefContent.text, is(content.text));
		assertThat(savedMessage.normalContent.text, is(content.text));
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

		assertThat(savedTmpl.getContent().toString(), is("someContent:C1$LazyLoadingProxy"));
		assertThat(savedTmpl.getContent(), is(instanceOf(LazyLoadingProxy.class)));
		assertThat(savedTmpl.getContent().getText(), is(nullValue()));
	}

	@Test // DATAMONGO-471
	public void updateMultiShouldAddValuesCorrectlyWhenUsingAddToSetWithEach() {

		DocumentWithCollectionOfSimpleType document = new DocumentWithCollectionOfSimpleType();
		document.values = Arrays.asList("spring");
		template.save(document);

		Query query = query(where("id").is(document.id));
		assumeThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values, hasSize(1));

		Update update = new Update().addToSet("values").each("data", "mongodb");
		template.updateMulti(query, update, DocumentWithCollectionOfSimpleType.class);

		assertThat(template.findOne(query, DocumentWithCollectionOfSimpleType.class).values, hasSize(3));
	}

	@Test // DATAMONGO-1210
	public void findAndModifyAddToSetWithEachShouldNotAddDuplicatesNorTypeHintForSimpleDocuments() {

		DocumentWithCollectionOfSamples doc = new DocumentWithCollectionOfSamples();
		doc.samples = Arrays.asList(new Sample(null, "sample1"));

		template.save(doc);

		Query query = query(where("id").is(doc.id));

		assertThat(template.findOne(query, DocumentWithCollectionOfSamples.class), notNullValue());

		Update update = new Update().addToSet("samples").each(new Sample(null, "sample2"), new Sample(null, "sample1"));

		template.findAndModify(query, update, DocumentWithCollectionOfSamples.class);

		DocumentWithCollectionOfSamples retrieved = template.findOne(query, DocumentWithCollectionOfSamples.class);

		assertThat(retrieved, notNullValue());
		assertThat(retrieved.samples, hasSize(2));
		assertThat(retrieved.samples.get(0).field, is("sample1"));
		assertThat(retrieved.samples.get(1).field, is("sample2"));
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

		Query query = query(where("_id").in("1", "2")).with(new Sort(Direction.DESC, "someIdKey"));
		assertThat(template.find(query, DoucmentWithNamedIdField.class), contains(two, one));
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

		Query query = query(where("_id").in("1", "2")).with(new Sort(Direction.DESC, "value"));
		assertThat(template.find(query, DoucmentWithNamedIdField.class), contains(two, one));
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

		assertThat(result, is(notNullValue()));
		assertThat(result.getContent(), is(notNullValue()));
		assertThat(result.getContent().getId(), is(notNullValue()));
		assertThat(result.getContent().getName(), is(notNullValue()));
		assertThat(result.getContent().getText(), is(content.getText()));
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

		assertNotNull(result.getContent().getName());
		assertThat(result.getContent().getName(), is(content.getName()));
	}

	@Test // DATAMONGO-970
	public void insertsAndRemovesBasicDbObjectCorrectly() {

		BasicDBObject object = new BasicDBObject("key", "value");
		template.insert(object, "collection");

		assertThat(object.get("_id"), is(notNullValue()));
		assertThat(template.findAll(DBObject.class, "collection"), hasSize(1));

		template.remove(object, "collection");
		assertThat(template.findAll(DBObject.class, "collection"), hasSize(0));
	}

	@Test // DATAMONGO-1207
	public void ignoresNullElementsForInsertAll() {

		Address newYork = new Address("NY", "New York");
		Address washington = new Address("DC", "Washington");

		template.insertAll(Arrays.asList(newYork, null, washington));

		List<Address> result = template.findAll(Address.class);

		assertThat(result, hasSize(2));
		assertThat(result, hasItems(newYork, washington));
	}

	@Test // DATAMONGO-1208
	public void takesSortIntoAccountWhenStreaming() {

		Person youngestPerson = new Person("John", 20);
		Person oldestPerson = new Person("Jane", 42);

		template.insertAll(Arrays.asList(oldestPerson, youngestPerson));

		Query q = new Query();
		q.with(new Sort(Direction.ASC, "age"));
		CloseableIterator<Person> stream = template.stream(q, Person.class);

		assertThat(stream.next().getAge(), is(youngestPerson.getAge()));
		assertThat(stream.next().getAge(), is(oldestPerson.getAge()));
	}

	@Test // DATAMONGO-1208
	public void takesLimitIntoAccountWhenStreaming() {

		Person youngestPerson = new Person("John", 20);
		Person oldestPerson = new Person("Jane", 42);

		template.insertAll(Arrays.asList(oldestPerson, youngestPerson));

		Query q = new Query();
		q.with(new PageRequest(0, 1, new Sort(Direction.ASC, "age")));
		CloseableIterator<Person> stream = template.stream(q, Person.class);

		assertThat(stream.next().getAge(), is(youngestPerson.getAge()));
		assertThat(stream.hasNext(), is(false));
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

		assertThat(messageLoaded.dbrefContent.id, is(contentLoaded.id));
		assertThat(contentLoaded.dbrefMessage.id, is(messageLoaded.id));
	}

	@Test // DATAMONGO-1287
	public void shouldReuseAlreadyResolvedLazyLoadedDBRefWhenUsedAsPersistenceConstrcutorArgument() {

		Document docInCtor = new Document();
		docInCtor.id = "doc-in-ctor";
		template.save(docInCtor);

		DocumentWithLazyDBrefUsedInPresistenceConstructor source = new DocumentWithLazyDBrefUsedInPresistenceConstructor(
				docInCtor);

		template.save(source);

		DocumentWithLazyDBrefUsedInPresistenceConstructor loaded = template.findOne(query(where("id").is(source.id)),
				DocumentWithLazyDBrefUsedInPresistenceConstructor.class);
		assertThat(loaded.refToDocUsedInCtor, not(instanceOf(LazyLoadingProxy.class)));
		assertThat(loaded.refToDocNotUsedInCtor, nullValue());
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
		assertThat(loaded.refToDocNotUsedInCtor, instanceOf(LazyLoadingProxy.class));
		assertThat(loaded.refToDocUsedInCtor, nullValue());
	}

	@Test // DATAMONGO-1287
	public void shouldRespectParamterValueWhenAttemptingToReuseLazyLoadedDBRefUsedInPersistenceConstrcutor() {

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
		assertThat(loaded.refToDocUsedInCtor, not(instanceOf(LazyLoadingProxy.class)));
		assertThat(loaded.refToDocNotUsedInCtor, instanceOf(LazyLoadingProxy.class));
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

		assertThat(template.findOne(query(where("id").is(wgj.id)), WithGeoJson.class).point, is(equalTo(wgj.point)));
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
		assertThat(loaded.date, equalTo(cal.getTime()));
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
		assertThat(loaded.byteVal, equalTo(byteVal));
		assertThat(loaded.doubleVal, equalTo(190D));
		assertThat(loaded.floatVal, equalTo(290F));
		assertThat(loaded.intVal, equalTo(390));
		assertThat(loaded.longVal, equalTo(490L));
		assertThat(loaded.bigIntegerVal, equalTo(new BigInteger("590")));
		assertThat(loaded.bigDeciamVal, equalTo(new BigDecimal("690")));
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
		assertThat(loaded.date, equalTo(cal.getTime()));
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
		assertThat(loaded.byteVal, equalTo(byteVal));
		assertThat(loaded.doubleVal, equalTo(290D));
		assertThat(loaded.floatVal, equalTo(390F));
		assertThat(loaded.intVal, equalTo(490));
		assertThat(loaded.longVal, equalTo(590L));
		assertThat(loaded.bigIntegerVal, equalTo(new BigInteger("690")));
		assertThat(loaded.bigDeciamVal, equalTo(new BigDecimal("790")));
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
		assertThat(loaded.bigIntegerVal, equalTo(new BigInteger("70")));
		assertThat(loaded.bigDeciamVal, equalTo(new BigDecimal("80")));
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
		assertThat(loaded.bigIntegerVal, equalTo(new BigInteger("700")));
		assertThat(loaded.bigDeciamVal, equalTo(new BigDecimal("800")));
	}

	@Test // DATAMONGO-1431
	public void streamExecutionUsesExplicitCollectionName() {

		template.remove(new Query(), "some_special_collection");
		template.remove(new Query(), Document.class);

		Document document = new Document();

		template.insert(document, "some_special_collection");

		CloseableIterator<Document> stream = template.stream(new Query(), Document.class);

		assertThat(stream.hasNext(), is(false));

		stream = template.stream(new Query(), Document.class, "some_special_collection");

		assertThat(stream.hasNext(), is(true));
		assertThat(stream.next().id, is(document.id));
		assertThat(stream.hasNext(), is(false));
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

		assertThat(template.findOne(query(where("id").is(source.id)), DocumentWithDBRefCollection.class), is(source));
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

		assertThat(target.lazyDbRefAnnotatedList, instanceOf(LazyLoadingProxy.class));
		assertThat(target.getLazyDbRefAnnotatedList(), contains(two, one));
	}

	@Test // DATAMONGO-1194
	public void shouldFetchMapOfLazyReferencesCorrectly() {

		Sample one = new Sample("1", "jon snow");
		Sample two = new Sample("2", "tyrion lannister");

		template.save(one);
		template.save(two);

		DocumentWithDBRefCollection source = new DocumentWithDBRefCollection();
		source.lazyDbRefAnnotatedMap = new LinkedHashMap<String, Sample>();
		source.lazyDbRefAnnotatedMap.put("tyrion", two);
		source.lazyDbRefAnnotatedMap.put("jon", one);
		template.save(source);

		DocumentWithDBRefCollection target = template.findOne(query(where("id").is(source.id)),
				DocumentWithDBRefCollection.class);

		assertThat(target.lazyDbRefAnnotatedMap, instanceOf(LazyLoadingProxy.class));
		assertThat(target.lazyDbRefAnnotatedMap.values(), contains(two, one));
	}

	@Test // DATAMONGO-1513
	@DirtiesContext
	public void populatesIdsAddedByEventListener() {

		context.addApplicationListener(new AbstractMongoEventListener<Document>() {

			@Override
			public void onBeforeSave(BeforeSaveEvent<Document> event) {
				event.getDBObject().put("_id", UUID.randomUUID().toString());
			}
		});

		Document document = new Document();

		template.insertAll(Arrays.asList(document));

		assertThat(document.id, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-1517
	 */
	@Test
	public void decimal128TypeShouldBeSavedAndLoadedCorrectly()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_FOUR), is(true));
		assumeThat(MongoClientVersion.isMongo34Driver(), is(true));

		Class<?> decimal128Type = ClassUtils.resolveClassName("org.bson.types.Decimal128", null);

		WithObjectTypeProperty source = new WithObjectTypeProperty();
		source.id = "decimal128-property-value";
		source.value = decimal128Type.getConstructor(BigDecimal.class).newInstance(new BigDecimal(100));

		template.save(source);

		WithObjectTypeProperty loaded = template.findOne(query(where("id").is(source.id)), WithObjectTypeProperty.class);
		assertThat(loaded.getValue(), instanceOf(decimal128Type));
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
				template.determineCollectionName(Sample.class));

		assertThat(result, hasSize(2));
		assertThat(result, containsInAnyOrder(bran, rickon));
		assertThat(template.count(new BasicQuery("{}"), template.determineCollectionName(Sample.class)), is(equalTo(1L)));
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

	static class DocumentWithMultipleCollections {
		@Id String id;
		List<String> string1;
		List<String> string2;
	}

	static class DocumentWithNestedCollection {
		@Id String id;
		List<Map<String, Model>> models = new ArrayList<Map<String, Model>>();
	}

	static class DocumentWithNestedList {
		@Id String id;
		List<List<Model>> models = new ArrayList<List<Model>>();
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

	public static class MyPerson {

		String id;
		String name;
		Address address;

		public String getName() {
			return name;
		}
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
}
