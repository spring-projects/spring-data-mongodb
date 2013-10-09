/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link SpelExpressionToMongoExpressionTransformer}.
 * 
 * @author Thomas Darimont
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class SpelExpressionToMongoExpressionTransformerIntegrationTests {

	@Autowired MongoDbFactory mongoDbFactory;

	@Rule public ExpectedException exception = ExpectedException.none();

	SpelExpressionToMongoExpressionTransformer transformer = SpelExpressionToMongoExpressionTransformer.INSTANCE;

	@Test
	public void shouldConvertCompoundExpressionToPropertyPath() {

		MappingMongoConverter converter = new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		TypeBasedAggregationOperationContext ctxt = new TypeBasedAggregationOperationContext(Data.class,
				new MongoMappingContext(), new QueryMapper(converter));
		assertThat(transformer.transform("item.value", ctxt, new Object[0]).toString(), is("$item.value"));
	}

	@Test
	public void shouldThrowExceptionIfNestedPropertyCannotBeFound() {

		exception.expect(MappingException.class);
		exception.expectMessage("value2");

		MappingMongoConverter converter = new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		TypeBasedAggregationOperationContext ctxt = new TypeBasedAggregationOperationContext(Data.class,
				new MongoMappingContext(), new QueryMapper(converter));
		assertThat(transformer.transform("item.value2", ctxt, new Object[0]).toString(), is("$item.value2"));
	}
}
