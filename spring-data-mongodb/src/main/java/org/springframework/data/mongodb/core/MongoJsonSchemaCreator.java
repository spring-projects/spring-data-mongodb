/*
 * Copyright 2019-2021 the original author or authors.
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

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.util.Assert;

/**
 * {@link MongoJsonSchemaCreator} extracts the {@link MongoJsonSchema} for a given {@link Class} by applying the
 * following mapping rules.
 * <p>
 * <strong>Required Properties</strong>
 * <ul>
 * <li>Properties of primitive type</li>
 * </ul>
 * <strong>Ignored Properties</strong>
 * <ul>
 * <li>All properties annotated with {@link org.springframework.data.annotation.Transient}</li>
 * </ul>
 * <strong>Property Type Mapping</strong>
 * <ul>
 * <li>{@link java.lang.Object} -> {@code type : 'object'}</li>
 * <li>{@link java.util.Arrays} -> {@code type : 'array'}</li>
 * <li>{@link java.util.Collection} -> {@code type : 'array'}</li>
 * <li>{@link java.util.Map} -> {@code type : 'object'}</li>
 * <li>{@link java.lang.Enum} -> {@code type : 'string', enum : [the enum values]}</li>
 * <li>Simple Types -> {@code type : 'the corresponding bson type' }</li>
 * <li>Domain Types -> {@code type : 'object', properties : &#123;the types properties&#125; }</li>
 * </ul>
 * <br />
 * {@link org.springframework.data.annotation.Id _id} properties using types that can be converted into
 * {@link org.bson.types.ObjectId} like {@link String} will be mapped to {@code type : 'object'} unless there is more
 * specific information available via the {@link org.springframework.data.mongodb.core.mapping.MongoId} annotation.
 * </p>
 *
 * @author Christoph Strobl
 * @since 2.2
 */
public interface MongoJsonSchemaCreator {

	/**
	 * Create the {@link MongoJsonSchema} for the given {@link Class type}.
	 *
	 * @param type must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	default MongoJsonSchema createSchemaFor(Class<?> type) {
		return createSchemaFor(type, false);
	}

	MongoJsonSchema createSchemaFor(Class<?> type, boolean encryptedFieldsOnly);

	/**
	 * Creates a new {@link MongoJsonSchemaCreator} that is aware of conversions applied by the given
	 * {@link MongoConverter}.
	 *
	 * @param mongoConverter must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 */
	static MongoJsonSchemaCreator create(MongoConverter mongoConverter) {

		Assert.notNull(mongoConverter, "MongoConverter must not be null!");
		return new MappingMongoJsonSchemaCreator(mongoConverter);
	}

	/**
	 * Creates a new {@link MongoJsonSchemaCreator} that is aware of conversions applied by the given
	 * {@link MongoConverter}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 */
	static MongoJsonSchemaCreator create(MappingContext mappingContext) {

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(config -> {}));

		converter.afterPropertiesSet();

		return create(converter);
	}

	static MongoJsonSchemaCreator create() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(MongoSimpleTypes.HOLDER);
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(config -> {}));
		converter.afterPropertiesSet();

		return create(converter);
	}
}
