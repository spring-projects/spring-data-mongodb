/*
 * Copyright 2019-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.mapping.Unwrapped.Nullable;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.util.Assert;

/**
 * {@link MongoJsonSchemaCreator} extracts the {@link MongoJsonSchema} for a given {@link Class} by applying the
 * following mapping rules.
 * <p>
 * <strong>Required Properties</strong>
 * </p>
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
 * {@link Encrypted} properties will contain {@literal encrypt} information.
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
	MongoJsonSchema createSchemaFor(Class<?> type);

	/**
	 * Create a merged {@link MongoJsonSchema} out of the individual schemas of the given types by merging their
	 * properties into one large {@link MongoJsonSchema schema}.
	 *
	 * @param types must not be {@literal null} nor contain {@literal null}.
	 * @return new instance of {@link MongoJsonSchema}.
	 * @since 3.4
	 */
	default MongoJsonSchema mergedSchemaFor(Class<?>... types) {

		MongoJsonSchema[] schemas = Arrays.stream(types).map(this::createSchemaFor).toArray(MongoJsonSchema[]::new);
		return MongoJsonSchema.merge(schemas);
	}

	/**
	 * Filter matching {@link JsonSchemaProperty properties}.
	 *
	 * @param filter the {@link Predicate} to evaluate for inclusion. Must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 * @since 3.3
	 */
	MongoJsonSchemaCreator filter(Predicate<JsonSchemaPropertyContext> filter);

	/**
	 * Entry point to specify additional behavior for a given path.
	 *
	 * @param path the path using {@literal dot '.'} notation.
	 * @return new instance of {@link PropertySpecifier}.
	 * @since 3.4
	 */
	PropertySpecifier property(String path);

	/**
	 * The context in which a specific {@link #getProperty()} is encountered during schema creation.
	 *
	 * @since 3.3
	 */
	interface JsonSchemaPropertyContext {

		/**
		 * The path to a given field/property in dot notation.
		 *
		 * @return never {@literal null}.
		 */
		String getPath();

		/**
		 * The current property.
		 *
		 * @return never {@literal null}.
		 */
		MongoPersistentProperty getProperty();

		/**
		 * Obtain the {@link MongoPersistentEntity} for a given property.
		 *
		 * @param property must not be {@literal null}.
		 * @param <T>
		 * @return {@literal null} if the property is not an entity. It is nevertheless recommend to check
		 *         {@link PersistentProperty#isEntity()} first.
		 */
		@Nullable
		<T> MongoPersistentEntity<T> resolveEntity(MongoPersistentProperty property);

	}

	/**
	 * A filter {@link Predicate} that matches {@link Encrypted encrypted properties} and those having nested ones.
	 *
	 * @return new instance of {@link Predicate}.
	 * @since 3.3
	 */
	static Predicate<JsonSchemaPropertyContext> encryptedOnly() {

		return new Predicate<JsonSchemaPropertyContext>() {

			// cycle guard
			private final Set<MongoPersistentProperty> seen = new HashSet<>();

			@Override
			public boolean test(JsonSchemaPropertyContext context) {
				return extracted(context.getProperty(), context);
			}

			private boolean extracted(MongoPersistentProperty property, JsonSchemaPropertyContext context) {
				if (property.isAnnotationPresent(Encrypted.class)) {
					return true;
				}

				if (!property.isEntity() || seen.contains(property)) {
					return false;
				}

				seen.add(property);

				for (MongoPersistentProperty nested : context.resolveEntity(property)) {
					if (extracted(nested, context)) {
						return true;
					}
				}
				return false;
			}
		};
	}

	/**
	 * Creates a new {@link MongoJsonSchemaCreator} that is aware of conversions applied by the given
	 * {@link MongoConverter}.
	 *
	 * @param mongoConverter must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 */
	static MongoJsonSchemaCreator create(MongoConverter mongoConverter) {

		Assert.notNull(mongoConverter, "MongoConverter must not be null");
		return new MappingMongoJsonSchemaCreator(mongoConverter);
	}

	/**
	 * Creates a new {@link MongoJsonSchemaCreator} that is aware of type mappings and potential
	 * {@link org.springframework.data.spel.spi.EvaluationContextExtension extensions}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 * @since 3.3
	 */
	static MongoJsonSchemaCreator create(MappingContext mappingContext) {

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(config -> {}));
		converter.afterPropertiesSet();

		return create(converter);
	}

	/**
	 * Creates a new {@link MongoJsonSchemaCreator} that does not consider potential extensions - suitable for testing. We
	 * recommend to use {@link #create(MappingContext)}.
	 *
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 * @since 3.3
	 */
	static MongoJsonSchemaCreator create() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(MongoSimpleTypes.HOLDER);
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		converter.setCustomConversions(MongoCustomConversions.create(config -> {}));
		converter.afterPropertiesSet();

		return create(converter);
	}

	/**
	 * @author Christoph Strobl
	 * @since 3.4
	 */
	interface PropertySpecifier {

		/**
		 * Set additional type parameters for polymorphic ones.
		 *
		 * @param types must not be {@literal null}.
		 * @return the source
		 */
		MongoJsonSchemaCreator withTypes(Class<?>... types);
	}
}
