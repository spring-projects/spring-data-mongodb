/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.Locale;

/**
 * {@link FieldNamingStrategy} that abbreviates field names by using the very first letter of the camel case parts of
 * the {@link MongoPersistentProperty}'s name.
 * 
 * @since 1.3
 * @author Oliver Gierke
 */
public class CamelCaseAbbreviatingFieldNamingStrategy implements FieldNamingStrategy {

	private static final String CAMEL_CASE_PATTERN = "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])";

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.FieldNamingStrategy#getFieldName(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty)
	 */
	public String getFieldName(MongoPersistentProperty property) {

		String[] parts = property.getName().split(CAMEL_CASE_PATTERN);
		StringBuilder builder = new StringBuilder();

		for (String part : parts) {
			builder.append(part.substring(0, 1).toLowerCase(Locale.US));
		}

		return builder.toString();
	}
}
