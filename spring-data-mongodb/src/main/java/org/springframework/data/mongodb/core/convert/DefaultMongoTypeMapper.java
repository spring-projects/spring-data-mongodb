/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeInformationMapper;
import org.springframework.data.mapping.Alias;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.util.ObjectUtils;

/**
 * Default implementation of {@link MongoTypeMapper} allowing configuration of the key to lookup and store type
 * information in {@link Document}. The key defaults to {@link #DEFAULT_TYPE_KEY}. Actual type-to-{@link String}
 * conversion and back is done in {@link #getTypeString(TypeInformation)} or {@link #getTypeInformation(String)}
 * respectively.
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
	private final String typeKey;

	public DefaultMongoTypeMapper() {
		this(DEFAULT_TYPE_KEY);
	}

	public DefaultMongoTypeMapper(String typeKey) {
		this(typeKey, Arrays.asList(new SimpleTypeInformationMapper()));
	}

	public DefaultMongoTypeMapper(String typeKey, MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext) {
		this(typeKey, new DocumentTypeAliasAccessor(typeKey), mappingContext,
				Arrays.asList(new SimpleTypeInformationMapper()));
	}

	public DefaultMongoTypeMapper(String typeKey, List<? extends TypeInformationMapper> mappers) {
		this(typeKey, new DocumentTypeAliasAccessor(typeKey), null, mappers);
	}

	private DefaultMongoTypeMapper(String typeKey, TypeAliasAccessor<Bson> accessor,
			MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext,
			List<? extends TypeInformationMapper> mappers) {

		super(accessor, mappingContext, mappers);

		this.typeKey = typeKey;
		this.accessor = accessor;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoTypeMapper#isTypeKey(java.lang.String)
	 */
	public boolean isTypeKey(String key) {
		return typeKey == null ? false : typeKey.equals(key);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoTypeMapper#writeTypeRestrictions(java.util.Set)
	 */
	@Override
	public void writeTypeRestrictions(Document result, Set<Class<?>> restrictedTypes) {

		if (restrictedTypes == null || restrictedTypes.isEmpty()) {
			return;
		}

		BasicDBList restrictedMappedTypes = new BasicDBList();

		for (Class<?> restrictedType : restrictedTypes) {

			Alias typeAlias = getAliasFor(ClassTypeInformation.from(restrictedType));

			if (typeAlias != null && !ObjectUtils.nullSafeEquals(Alias.NONE, typeAlias) && typeAlias.getValue().isPresent()) {
				restrictedMappedTypes.add(typeAlias.getValue().get());
			}
		}

		accessor.writeTypeTo(result, new Document("$in", restrictedMappedTypes));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.DefaultTypeMapper#getFallbackTypeFor(java.lang.Object)
	 */
	@Override
	protected Optional<TypeInformation<?>> getFallbackTypeFor(Bson source) {
		return Optional.of(source instanceof BasicDBList ? LIST_TYPE_INFO : MAP_TYPE_INFO);
	}

	/**
	 * {@link TypeAliasAccessor} to store aliases in a {@link Document}.
	 * 
	 * @author Oliver Gierke
	 */
	public static final class DocumentTypeAliasAccessor implements TypeAliasAccessor<Bson> {

		private final String typeKey;

		public DocumentTypeAliasAccessor(String typeKey) {
			this.typeKey = typeKey;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#readAliasFrom(java.lang.Object)
		 */
		public Alias readAliasFrom(Bson source) {

			if (source instanceof List) {
				return Alias.NONE;
			}

			if (source instanceof Document) {
				return Alias.ofOptional(Optional.ofNullable(((Document) source).get(typeKey)));
			} else if (source instanceof DBObject) {
				return Alias.ofOptional(Optional.ofNullable(((DBObject) source).get(typeKey)));
			}

			throw new IllegalArgumentException("Cannot read alias from " + source.getClass());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#writeTypeTo(java.lang.Object, java.lang.Object)
		 */
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
