/*
 * Copyright 2011-2024 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.dao.DataAccessException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.AbstractMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoTypeMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.util.TypeInformation;

import com.mongodb.DBRef;

/**
 * Abstract base class for unit tests to specify behaviour we expect from {@link MongoOperations}. Subclasses return
 * instances of their implementation and thus can see if it correctly implements the {@link MongoOperations} interface.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
public abstract class MongoOperationsUnitTests {

	@Mock CollectionCallback<Object> collectionCallback;
	@Mock DbCallback<Object> dbCallback;

	MongoConverter converter;
	Person person;
	List<Person> persons;

	@BeforeEach
	public final void operationsSetUp() {

		person = new Person("Oliver");
		persons = Arrays.asList(person);

		converter = new AbstractMongoConverter(null) {

			public void write(Object t, Bson bson) {
				((Document) bson).put("firstName", person.getFirstName());
			}

			@SuppressWarnings("unchecked")
			public <S extends Object> S read(Class<S> clazz, Bson bson) {
				return (S) person;
			}

			public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
				return null;
			}

			public Object convertToMongoType(Object obj, TypeInformation<?> typeInformation) {
				return null;
			}

			public DBRef toDBRef(Object object, MongoPersistentProperty referringProperty) {
				return null;
			}

			@Override
			public MongoTypeMapper getTypeMapper() {
				return null;
			}

			@Override
			public ProjectionFactory getProjectionFactory() {
				return null;
			}

			@Override
			public CustomConversions getCustomConversions() {
				return null;
			}

			@Override
			public <R> R project(EntityProjection<R, ?> descriptor, Bson bson) {
				return null;
			}
		};
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void rejectsNullForCollectionCallback() {
		assertThatIllegalArgumentException().isThrownBy(() -> getOperations().execute("test", (CollectionCallback) null));
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void rejectsNullForCollectionCallback2() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> getOperations().execute("collection", (CollectionCallback) null));
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void rejectsNullForDbCallback() {
		assertThatIllegalArgumentException().isThrownBy(() -> getOperations().execute((DbCallback) null));
	}

	@Test
	public void convertsExceptionForCollectionExists() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.collectionExists("foo");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForCreateCollection() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.createCollection("foo");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForCreateCollection2() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.createCollection("foo", CollectionOptions.empty().size(1).maxDocuments(1).capped());
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForDropCollection() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.dropCollection("foo");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForExecuteCollectionCallback() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.execute("test", collectionCallback);
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForExecuteDbCallback() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.execute(dbCallback);
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForExecuteCollectionCallbackAndCollection() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.execute("collection", collectionCallback);
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForExecuteCommand() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.executeCommand(new Document());
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForExecuteStringCommand() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.executeCommand("");
			}
		}.assertException(IllegalArgumentException.class);
	}

	@Test
	public void convertsExceptionForGetCollection() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.findAll(Object.class);
			}
		}.assertException(MappingException.class);
	}

	@Test
	public void convertsExceptionForGetCollectionWithCollectionName() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.getCollection("collection");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForGetCollectionWithCollectionNameAndType() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.findAll(Object.class, "collection");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForGetCollectionWithCollectionNameTypeAndReader() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.findAll(Object.class, "collection");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForGetCollectionNames() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.getCollectionNames();
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForInsert() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.insert(person);
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForInsert2() {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.insert(person, "collection");
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForInsertList() throws Exception {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.insertAll(persons);
			}
		}.assertDataAccessException();
	}

	@Test
	public void convertsExceptionForGetInsertList2() throws Exception {
		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.insert(persons, "collection");
			}
		}.assertDataAccessException();
	}

	@Test // DATAMONGO-341
	public void geoNearRejectsNullNearQuery() {

		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.geoNear(null, Person.class);
			}
		}.assertDataAccessException();
	}

	@Test // DATAMONGO-341
	public void geoNearRejectsNullNearQueryifCollectionGiven() {

		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.geoNear(null, Person.class, "collection");
			}
		}.assertDataAccessException();
	}

	@Test // DATAMONGO-341
	public void geoNearRejectsNullEntityClass() {

		final NearQuery query = NearQuery.near(new Point(10, 20));

		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.geoNear(query, null);
			}
		}.assertDataAccessException();
	}

	@Test // DATAMONGO-341
	public void geoNearRejectsNullEntityClassIfCollectionGiven() {

		final NearQuery query = NearQuery.near(new Point(10, 20));

		new Execution() {
			@Override
			public void doWith(MongoOperations operations) {
				operations.geoNear(query, null, "collection");
			}
		}.assertDataAccessException();
	}

	private abstract class Execution {

		public void assertDataAccessException() {
			assertException(DataAccessException.class);
		}

		public void assertException(Class<? extends Exception> exception) {

			assertThatThrownBy(() -> doWith(getOperationsForExceptionHandling())).isInstanceOf(exception);
		}

		public abstract void doWith(MongoOperations operations);
	}

	/**
	 * Expects an {@link MongoOperations} instance that will be used to check that invoking methods on it will only cause
	 * {@link DataAccessException}s.
	 *
	 * @return
	 */
	protected abstract MongoOperations getOperationsForExceptionHandling();

	/**
	 * Returns a plain {@link MongoOperations}.
	 *
	 * @return
	 */
	protected abstract MongoOperations getOperations();

}
