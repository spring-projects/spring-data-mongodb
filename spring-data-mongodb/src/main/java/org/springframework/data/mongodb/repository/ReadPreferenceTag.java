/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository;

/**
 * Annotation used by {@link ReadPreference} for define {@link com.mongodb.Tag}
 *
 * @author Jorge Rodríguez
 * @since 4.2
 */
public @interface ReadPreferenceTag {

	/**
	 * Set the name of tag
	 * @return name of tag
	 */
	String name();

	/**
	 * Set the value of tag
	 * @return value of tag
	 */
	String value();
}
