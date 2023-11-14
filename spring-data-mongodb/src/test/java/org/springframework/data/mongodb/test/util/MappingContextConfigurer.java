/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.lang.Nullable;

/**
 * Utility to configure {@link org.springframework.data.mongodb.core.mapping.MongoMappingContext} properties.
 *
 * @author Christoph Strobl
 */
public class MappingContextConfigurer {

	private @Nullable Set<Class<?>> intitalEntitySet;
	boolean autocreateIndex = false;

	public void autocreateIndex(boolean autocreateIndex) {
		this.autocreateIndex = autocreateIndex;
	}

	public void initialEntitySet(Set<Class<?>> initialEntitySet) {
		this.intitalEntitySet = initialEntitySet;
	}

	public void initialEntitySet(Class<?>... initialEntitySet) {
		this.intitalEntitySet = Set.of(initialEntitySet);
	}

	Set<Class<?>> initialEntitySet() {
		return intitalEntitySet != null ? intitalEntitySet : Collections.emptySet();
	}
}
