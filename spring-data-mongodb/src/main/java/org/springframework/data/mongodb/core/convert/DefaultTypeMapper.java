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

import java.util.List;

import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Default implementation of {@link TypeMapper} allowing configuration of the key to lookup and store type information
 * in {@link DBObject}. The key defaults to {@link #DEFAULT_TYPE_KEY}. Actual type-to-{@link String} conversion and back
 * is done in {@link #getTypeString(TypeInformation)} or {@link #getTypeInformation(String)} respectively.
 * 
 * @author Oliver Gierke
 */
public class DefaultTypeMapper implements TypeMapper {

	public static final String DEFAULT_TYPE_KEY = "_class";
	@SuppressWarnings("rawtypes")
	private static final TypeInformation<List> LIST_TYPE_INFORMATION = ClassTypeInformation.from(List.class);

	private String typeKey = DEFAULT_TYPE_KEY;

	/**
	 * Sets the key to store the type information under. If set to {@literal null} no type information will be stored in
	 * the document.
	 * 
	 * @param typeKey the typeKey to set
	 */
	public void setTypeKey(String typeKey) {
		this.typeKey = typeKey;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.TypeMapper#isTypeKey(java.lang.String)
	 */
	public boolean isTypeKey(String key) {
		return typeKey == null ? false : typeKey.equals(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.TypeMapper#readType(com.mongodb.DBObject)
	 */
	public TypeInformation<?> readType(DBObject dbObject) {

		if (dbObject instanceof BasicDBList) {
			return LIST_TYPE_INFORMATION;
		}

		if (typeKey == null) {
			return null;
		}

		Object classToBeUsed = dbObject.get(typeKey);

		if (classToBeUsed == null) {
			return null;
		}

		return getTypeInformation(classToBeUsed.toString());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.TypeMapper#writeType(java.lang.Class, com.mongodb.DBObject)
	 */
	public void writeType(Class<?> type, DBObject dbObject) {
		writeType(ClassTypeInformation.from(type), dbObject);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.TypeMapper#writeType(java.lang.Class, com.mongodb.DBObject)
	 */
	public void writeType(TypeInformation<?> info, DBObject dbObject) {

		Assert.notNull(info);

		if (typeKey == null) {
			return;
		}

		String string = getTypeString(info);

		if (string != null) {
			dbObject.put(typeKey, getTypeString(info));
		}
	}

	/**
	 * Turn the given type information into the String representation that shall be stored inside the {@link DBObject}. If
	 * the returned String is {@literal null} no type information will be stored. Default implementation simply returns
	 * the fully-qualified class name.
	 * 
	 * @param typeInformation must not be {@literal null}.
	 * @return the String representation to be stored or {@literal null} if no type information shall be stored.
	 */
	protected String getTypeString(TypeInformation<?> typeInformation) {
		return typeInformation.getType().getName();
	}

	/**
	 * Returns the {@link TypeInformation} that shall be used when the given {@link String} value is found as type hint.
	 * The default implementation will simply interpret the given value as fully-qualified class name and try to load the
	 * class. Will return {@literal null} in case the given {@link String} is empty. Will not be called in case no
	 * {@link String} was found for the configured type key at all.
	 * 
	 * @param value the type to load, must not be {@literal null}.
	 * @return the type to be used for the given {@link String} representation or {@literal null} if nothing found or the
	 *         class cannot be loaded.
	 */
	protected TypeInformation<?> getTypeInformation(String value) {

		if (!StringUtils.hasText(value)) {
			return null;
		}

		try {
			return ClassTypeInformation.from(ClassUtils.forName(value, null));
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
}
