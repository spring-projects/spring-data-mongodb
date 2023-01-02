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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;

import com.mongodb.BasicDBList;

/**
 * Unit tests for {@link ConvertingParameterAccessor}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class ConvertingParameterAccessorUnitTests {

	@Mock MongoDatabaseFactory factory;
	@Mock MongoParameterAccessor accessor;

	private MongoMappingContext context;
	private MappingMongoConverter converter;
	private DbRefResolver resolver;

	@BeforeEach
	void setUp() {

		when(factory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		this.context = new MongoMappingContext();
		this.resolver = new DefaultDbRefResolver(factory);
		this.converter = new MappingMongoConverter(resolver, context);
	}

	@Test
	void rejectsNullDbRefResolver() {
		assertThatIllegalArgumentException().isThrownBy(() -> new MappingMongoConverter((DbRefResolver) null, context));
	}

	@Test
	void rejectsNullContext() {
		assertThatIllegalArgumentException().isThrownBy(() -> new MappingMongoConverter(resolver, null));
	}

	@Test
	void convertsCollectionUponAccess() {

		when(accessor.getBindableValue(0)).thenReturn(Arrays.asList("Foo"));

		ConvertingParameterAccessor parameterAccessor = new ConvertingParameterAccessor(converter, accessor);
		Object result = parameterAccessor.getBindableValue(0);

		BasicDBList reference = new BasicDBList();
		reference.add("Foo");

		assertThat(result).isEqualTo((Object) reference);
	}

	@Test // DATAMONGO-505
	void convertsAssociationsToDBRef() {

		Property property = new Property();
		property.id = 5L;

		Object result = setupAndConvert(property);

		assertThat(result).isInstanceOf(com.mongodb.DBRef.class);
		com.mongodb.DBRef dbRef = (com.mongodb.DBRef) result;
		assertThat(dbRef.getCollectionName()).isEqualTo("property");
		assertThat(dbRef.getId()).isEqualTo((Object) 5L);
	}

	@Test // DATAMONGO-505
	void convertsAssociationsToDBRefForCollections() {

		Property property = new Property();
		property.id = 5L;

		Object result = setupAndConvert(Arrays.asList(property));

		assertThat(result).isInstanceOf(Collection.class);
		Collection<?> collection = (Collection<?>) result;

		assertThat(collection).hasSize(1);
		Object element = collection.iterator().next();

		assertThat(element).isInstanceOf(com.mongodb.DBRef.class);
		com.mongodb.DBRef dbRef = (com.mongodb.DBRef) element;
		assertThat(dbRef.getCollectionName()).isEqualTo("property");
		assertThat(dbRef.getId()).isEqualTo((Object) 5L);
	}

	private Object setupAndConvert(Object... parameters) {

		MongoParameterAccessor delegate = new StubParameterAccessor(parameters);
		PotentiallyConvertingIterator iterator = new ConvertingParameterAccessor(converter, delegate).iterator();

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(Entity.class);
		MongoPersistentProperty property = entity.getRequiredPersistentProperty("property");

		return iterator.nextConverted(property);
	}

	static class Entity {

		@DBRef Property property;
	}

	static class Property {

		Long id;
	}
}
