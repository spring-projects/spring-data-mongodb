/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.mockito.Mockito.*;

import org.bson.conversions.Bson;
import org.junit.Test;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverters.ObjectIdToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToObjectIdConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link AbstractMongoConverter}.
 *
 * @author Oliver Gierke
 */
public class AbstractMongoConverterUnitTests {

	@Test // DATAMONGO-1324
	public void registersObjectIdConvertersExplicitly() {

		DefaultConversionService conversionService = spy(new DefaultConversionService());

		new SampleMongoConverter(conversionService).afterPropertiesSet();

		verify(conversionService).addConverter(StringToObjectIdConverter.INSTANCE);
		verify(conversionService).addConverter(ObjectIdToStringConverter.INSTANCE);
	}

	static class SampleMongoConverter extends AbstractMongoConverter {

		public SampleMongoConverter(GenericConversionService conversionService) {
			super(conversionService);
		}

		@Override
		public MongoTypeMapper getTypeMapper() {
			throw new UnsupportedOperationException();
		}

		@Override
		public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R> R read(Class<R> type, Bson source) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void write(Object source, Bson sink) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object convertToMongoType(Object obj, TypeInformation<?> typeInformation) {
			throw new UnsupportedOperationException();
		}

		@Override
		public DBRef toDBRef(Object object, MongoPersistentProperty referingProperty) {
			throw new UnsupportedOperationException();
		}
	}
}
