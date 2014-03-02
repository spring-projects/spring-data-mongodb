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

/**
 * {@link FieldNamingStrategy} that translates typical camel case Java 
 * property names to lower case JSON element names, separated by 
 * underscores.
 * 
 * Source: jackson-databind, Apache 2 License
 * https://github.com/FasterXML/jackson-databind/blob/2.3/src/main/java/com/fasterxml/jackson/databind/PropertyNamingStrategy.java
 * 
 * @since 1.5
 * @author Ryan Tenney
 */
public class LowerCaseWithUnderscoresFieldNamingStrategy implements FieldNamingStrategy {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.FieldNamingStrategy#getFieldName(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty)
	 */
	public String getFieldName(MongoPersistentProperty property) {
		String input = property.getName();
		int length = input.length();
		StringBuilder result = new StringBuilder(length * 2);
		int resultLength = 0;
		boolean wasPrevTranslated = false;
		for (int i = 0; i < length; i++) {
			char c = input.charAt(i);
			if (i > 0 || c != '_') { // skip first starting underscore
				if (Character.isUpperCase(c)) {
					if (!wasPrevTranslated && resultLength > 0 && result.charAt(resultLength - 1) != '_') {
						result.append('_');
						resultLength++;
					}
					c = Character.toLowerCase(c);
					wasPrevTranslated = true;
				}
				else {
					wasPrevTranslated = false;
				}
				result.append(c);
				resultLength++;
			}
		}
		return resultLength > 0 ? result.toString() : input;
	}

}
