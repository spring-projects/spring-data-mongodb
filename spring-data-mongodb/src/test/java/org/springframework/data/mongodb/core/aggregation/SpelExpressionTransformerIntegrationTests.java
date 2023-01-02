/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for {@link SpelExpressionTransformer}.
 *
 * @author Thomas Darimont
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class SpelExpressionTransformerIntegrationTests {

	@Autowired MongoDatabaseFactory mongoDbFactory;

	SpelExpressionTransformer transformer;
	DbRefResolver dbRefResolver;

	@Before
	public void setUp() {
		this.transformer = new SpelExpressionTransformer();
		this.dbRefResolver = new DefaultDbRefResolver(mongoDbFactory);
	}

	@Test // DATAMONGO-774
	public void shouldConvertCompoundExpressionToPropertyPath() {

		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, new MongoMappingContext());
		TypeBasedAggregationOperationContext ctxt = new TypeBasedAggregationOperationContext(Data.class,
				new MongoMappingContext(), new QueryMapper(converter));
		assertThat(transformer.transform("item.primitiveIntValue", ctxt, new Object[0]).toString())
				.isEqualTo("$item.primitiveIntValue");
	}

	@Test // DATAMONGO-774
	public void shouldThrowExceptionIfNestedPropertyCannotBeFound() {

		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, new MongoMappingContext());
		TypeBasedAggregationOperationContext ctxt = new TypeBasedAggregationOperationContext(Data.class,
				new MongoMappingContext(), new QueryMapper(converter));

		assertThatExceptionOfType(InvalidPersistentPropertyPath.class).isThrownBy(() -> {
			transformer.transform("item.value2", ctxt, new Object[0]).toString();
		});
	}
}
