/*
 * Copyright 2010-2011 the original author or authors.
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

import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Central Mongo specific converter interface which combines {@link MongoWriter} and {@link MongoReader}.
 * 
 * @author Oliver Gierke
 */
public interface MongoConverter extends MongoWriter<Object>, MongoReader<Object> {

	/**
	 * Returns the underlying {@link MappingContext} used by the converter.
	 * 
	 * @return never {@literal null}
	 */
	MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext();

	/**
	 * Returns the underlying {@link ConversionService} used by the converter.
	 * 
	 * @return never {@literal null}.
	 */
	ConversionService getConversionService();
}
