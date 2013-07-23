/*
 * Copyright 2011 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeInformationMapper;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Default implementation of {@link MongoTypeMapper} allowing configuration of the key to lookup and store type
 * information in {@link DBObject}. The key defaults to {@link #DEFAULT_TYPE_KEY}. Actual type-to-{@link String}
 * conversion and back is done in {@link #getTypeString(TypeInformation)} or {@link #getTypeInformation(String)}
 * respectively.
 * 
 * @author Oliver Gierke
 */
public class DefaultMongoTypeMapper extends DefaultTypeMapper<DBObject> implements MongoTypeMapper {

	public static final String DEFAULT_TYPE_KEY = "_class";
	@SuppressWarnings("rawtypes") private static final TypeInformation<List> LIST_TYPE_INFO = ClassTypeInformation
			.from(List.class);
	@SuppressWarnings("rawtypes") private static final TypeInformation<Map> MAP_TYPE_INFO = ClassTypeInformation
			.from(Map.class);
	private String typeKey = DEFAULT_TYPE_KEY;

	public DefaultMongoTypeMapper() {
		this(DEFAULT_TYPE_KEY, Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
	}

	public DefaultMongoTypeMapper(String typeKey) {
		super(new DBObjectTypeAliasAccessor(typeKey));
		this.typeKey = typeKey;
	}

	public DefaultMongoTypeMapper(String typeKey, MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext) {
		super(new DBObjectTypeAliasAccessor(typeKey), mappingContext, Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
		this.typeKey = typeKey;
	}

	public DefaultMongoTypeMapper(String typeKey, List<? extends TypeInformationMapper> mappers) {
		super(new DBObjectTypeAliasAccessor(typeKey), mappers);
		this.typeKey = typeKey;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoTypeMapper#isTypeKey(java.lang.String)
	 */
	public boolean isTypeKey(String key) {
		return typeKey == null ? false : typeKey.equals(key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoTypeMapper#writeTypeRestrictions(java.util.Set)
	 */
	@Override
	public void writeTypeRestrictions(DBObject result, Set<Class<?>> restrictedTypes) {

		if (!restrictedTypes.isEmpty()) {
			List<String> restrictedMappedTypes = new ArrayList<String>();
			for (Class<?> restrictedType : restrictedTypes) {

				Object typeAlias = getAliasFor(ClassTypeInformation.from(restrictedType));
				if (typeAlias != null) {
					restrictedMappedTypes.add(typeAlias.toString());
				}
			}

			result.putAll(Criteria.where(typeKey).in(restrictedMappedTypes).getCriteriaObject());
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.DefaultTypeMapper#getFallbackTypeFor(java.lang.Object)
	 */
	@Override
	protected TypeInformation<?> getFallbackTypeFor(DBObject source) {
		return source instanceof BasicDBList ? LIST_TYPE_INFO : MAP_TYPE_INFO;
	}

	/**
	 * @author Oliver Gierke
	 */
	public static final class DBObjectTypeAliasAccessor implements TypeAliasAccessor<DBObject> {

		private final String typeKey;

		public DBObjectTypeAliasAccessor(String typeKey) {
			this.typeKey = typeKey;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#readAliasFrom(java.lang.Object)
		 */
		public Object readAliasFrom(DBObject source) {

			if (source instanceof BasicDBList) {
				return null;
			}

			return source.get(typeKey);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#writeTypeTo(java.lang.Object, java.lang.Object)
		 */
		public void writeTypeTo(DBObject sink, Object alias) {
			if (typeKey != null) {
				sink.put(typeKey, alias);
			}
		}
	}
}
