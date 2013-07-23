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

import org.springframework.data.convert.EntityConverter;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBObject;

/**
 * Central Mongo specific converter interface which combines {@link MongoWriter} and {@link MongoReader}.
 * 
 * @author Oliver Gierke
 */
public interface MongoConverter extends
		EntityConverter<MongoPersistentEntity<?>, MongoPersistentProperty, Object, DBObject>, MongoWriter<Object>,
		EntityReader<Object, DBObject> {

	MongoTypeMapper getMongoTypeMapper();
}
