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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * {@link TypeMapper} allowing to configure a {@link Map} containing {@link Class} to {@link String} mappings that will
 * be used to map the values found under the configured type key (see {@link DefaultTypeMapper#setTypeKey(String)}. This
 * allows declarative type mapping in a Spring config file for example.
 * 
 * @author Oliver Gierke
 */
public class ConfigurableTypeMapper extends DefaultTypeMapper {

	private final Map<TypeInformation<?>, String> typeMap;
	private boolean handleUnmappedClasses = false;

	/**
	 * Creates a new {@link ConfigurableTypeMapper} for the given type map.
	 * 
	 * @param sourceTypeMap must not be {@literal null}.
	 */
	public ConfigurableTypeMapper(Map<? extends Class<?>, String> sourceTypeMap) {

		Assert.notNull(sourceTypeMap);

		this.typeMap = new HashMap<TypeInformation<?>, String>(sourceTypeMap.size());

		for (Entry<? extends Class<?>, String> entry : sourceTypeMap.entrySet()) {
			TypeInformation<?> key = ClassTypeInformation.from(entry.getKey());
			String value = entry.getValue();

			if (typeMap.containsValue(value)) {
				throw new IllegalArgumentException(String.format(
						"Detected mapping ambiguity! String %s cannot be mapped to more than one type!", value));
			}

			this.typeMap.put(key, value);
		}
	}

	/**
	 * Configures whether to try to handle unmapped classes by simply writing the class' name or loading the class as
	 * specified in the superclass. Defaults to {@literal false}.
	 * 
	 * @param handleUnmappedClasses the handleUnmappedClasses to set
	 */
	public void setHandleUnmappedClasses(boolean handleUnmappedClasses) {
		this.handleUnmappedClasses = handleUnmappedClasses;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DefaultTypeMapper#getTypeInformation(java.lang.String)
	 */
	@Override
	protected TypeInformation<?> getTypeInformation(String value) {

		for (Entry<TypeInformation<?>, String> entry : typeMap.entrySet()) {
			if (entry.getValue().equals(value)) {
				return entry.getKey();
			}
		}

		return handleUnmappedClasses ? super.getTypeInformation(value) : null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DefaultTypeMapper#getTypeString(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected String getTypeString(TypeInformation<?> typeInformation) {

		String key = typeMap.get(typeInformation);
		return key != null ? key : handleUnmappedClasses ? super.getTypeString(typeInformation) : null;
	}
}
