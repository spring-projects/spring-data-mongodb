/*
 * Copyright 2019-2020 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * @author Christoph Strobl
 */
public class EntityOperationUnitTests {

	EntityOperations ops;
	MongoMappingContext mappingContext = new MongoMappingContext();
	ConversionService conversionService = new DefaultConversionService();

	@Before
	public void setUp() {
		ops = new EntityOperations(mappingContext);
	}

	@Test // DATAMONGO-2293
	public void populateIdShouldReturnTargetBeanWhenIdIsNull() {
		assertThat(initAdaptibleEntity(new DomainTypeWithIdProperty()).populateIdIfNecessary(null)).isNotNull();
	}

	<T> AdaptibleEntity<T> initAdaptibleEntity(T source) {
		return ops.forEntity(source, conversionService);
	}

	private static class DomainTypeWithIdProperty {

		@Id String id;
		String value;
	}
}
