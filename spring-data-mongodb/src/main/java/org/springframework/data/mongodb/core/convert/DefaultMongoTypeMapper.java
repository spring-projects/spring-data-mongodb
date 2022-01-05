/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeInformationMapper;
import org.springframework.data.mapping.Alias;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Default implementation of {@link MongoTypeMapper} allowing configuration of the key to lookup and store type
 * information in {@link Document}. The key defaults to {@link #DEFAULT_TYPE_KEY}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class DefaultMongoTypeMapper extends DefaultTypeMapper<Bson> implements MongoTypeMapper {

	public static final String DEFAULT_TYPE_KEY = "_class";
	@SuppressWarnings("rawtypes") //
	private static final TypeInformation<List> LIST_TYPE_INFO = ClassTypeInformation.from(List.class);
	@SuppressWarnings("rawtypes") //
	private static final TypeInformation<Map> MAP_TYPE_INFO = ClassTypeInformation.from(Map.class);

	private final TypeAliasAccessor<Bson> accessor;
	private final @Nullable String typeKey;
	private UnaryOperator<Class<?>> writeTarget = UnaryOperator.identity();

	/**
	 * Create a new {@link MongoTypeMapper} with fully-qualified type hints using {@code _class}.
	 */
	public DefaultMongoTypeMapper() {
		this(DEFAULT_TYPE_KEY);
	}

	/**
	 * Create a new {@link MongoTypeMapper} with fully-qualified type hints using {@code typeKey}.
	 *
	 * @param typeKey name of the field to read and write type hints. Can be {@literal null} to disable type hints.
	 */
	public DefaultMongoTypeMapper(@Nullable String typeKey) {
		this(typeKey, Collections.singletonList(new SimpleTypeInformationMapper()));
	}

	/**
	 * Create a new {@link MongoTypeMapper} with fully-qualified type hints using {@code typeKey}.
	 *
	 * @param typeKey name of the field to read and write type hints. Can be {@literal null} to disable type hints.
	 * @param mappingContext the mapping context.
	 */
	public DefaultMongoTypeMapper(@Nullable String typeKey,
			MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext) {
		this(typeKey, new DocumentTypeAliasAccessor(typeKey), mappingContext,
				Collections.singletonList(new SimpleTypeInformationMapper()));
	}

	/**
	 * Create a new {@link MongoTypeMapper} with fully-qualified type hints using {@code typeKey}. Uses
	 * {@link UnaryOperator} to apply {@link CustomConversions}.
	 *
	 * @param typeKey name of the field to read and write type hints. Can be {@literal null} to disable type hints.
	 * @param mappingContext the mapping context to look up types using type hints.
	 * @see MappingMongoConverter#getWriteTarget(Class)
	 */
	public DefaultMongoTypeMapper(@Nullable String typeKey,
			MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext, UnaryOperator<Class<?>> writeTarget) {
		this(typeKey, new DocumentTypeAliasAccessor(typeKey), mappingContext,
				Collections.singletonList(new SimpleTypeInformationMapper()));
		this.writeTarget = writeTarget;
	}

	/**
	 * Create a new {@link MongoTypeMapper} with fully-qualified type hints using {@code typeKey}. Uses
	 * {@link TypeInformationMapper} to map type hints.
	 *
	 * @param typeKey name of the field to read and write type hints. Can be {@literal null} to disable type hints.
	 * @param mappers must not be {@literal null}.
	 */
	public DefaultMongoTypeMapper(@Nullable String typeKey, List<? extends TypeInformationMapper> mappers) {
		this(typeKey, new DocumentTypeAliasAccessor(typeKey), null, mappers);
	}

	private DefaultMongoTypeMapper(@Nullable String typeKey, TypeAliasAccessor<Bson> accessor,
			MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext,
			List<? extends TypeInformationMapper> mappers) {

		super(accessor, mappingContext, mappers);

		this.typeKey = typeKey;
		this.accessor = accessor;
	}

	public boolean isTypeKey(String key) {
		return typeKey == null ? false : typeKey.equals(key);
	}

	@Override
	public void writeTypeRestrictions(Document result, @Nullable Set<Class<?>> restrictedTypes) {

		if (ObjectUtils.isEmpty(restrictedTypes)) {
			return;
		}

		BasicDBList restrictedMappedTypes = new BasicDBList();

		for (Class<?> restrictedType : restrictedTypes) {

			Alias typeAlias = getAliasFor(ClassTypeInformation.from(restrictedType));

			if (!ObjectUtils.nullSafeEquals(Alias.NONE, typeAlias) && typeAlias.isPresent()) {
				restrictedMappedTypes.add(typeAlias.getValue());
			}
		}

		accessor.writeTypeTo(result, new Document("$in", restrictedMappedTypes));
	}

	@Override
	public Class<?> getWriteTargetTypeFor(Class<?> source) {
		return writeTarget.apply(source);
	}

	@Override
	protected TypeInformation<?> getFallbackTypeFor(Bson source) {
		return source instanceof BasicDBList ? LIST_TYPE_INFO : MAP_TYPE_INFO;
	}

	/**
	 * {@link TypeAliasAccessor} to store aliases in a {@link Document}.
	 *
	 * @author Oliver Gierke
	 */
	public static final class DocumentTypeAliasAccessor implements TypeAliasAccessor<Bson> {

		private final @Nullable String typeKey;

		public DocumentTypeAliasAccessor(@Nullable String typeKey) {
			this.typeKey = typeKey;
		}

		public Alias readAliasFrom(Bson source) {

			if (source instanceof List) {
				return Alias.NONE;
			}

			if (source instanceof Document) {
				return Alias.ofNullable(((Document) source).get(typeKey));
			} else if (source instanceof DBObject) {
				return Alias.ofNullable(((DBObject) source).get(typeKey));
			}

			throw new IllegalArgumentException("Cannot read alias from " + source.getClass());
		}

		public void writeTypeTo(Bson sink, Object alias) {

			if (typeKey != null) {

				if (sink instanceof Document) {
					((Document) sink).put(typeKey, alias);
				} else if (sink instanceof DBObject) {
					((DBObject) sink).put(typeKey, alias);
				}
			}
		}
	}
}
