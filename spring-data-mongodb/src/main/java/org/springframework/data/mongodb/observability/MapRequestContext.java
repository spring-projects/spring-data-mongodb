/*
 * Copyright 2022-2025 the original author or authors.
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
package org.springframework.data.mongodb.observability;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import com.mongodb.RequestContext;

/**
 * A {@link Map}-based {@link RequestContext}.
 *
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0.0
 * @deprecated since 5.0 in favor of native MongoDB Java Driver observability support.
 */
@Deprecated(since = "5.0",  forRemoval = true)
record MapRequestContext(Map<Object, Object> map) implements RequestContext {

	public MapRequestContext() {
		this(new HashMap<>());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key) {

		T value = (T) map.get(key);
		if (value != null) {
			return value;
		}
		throw new NoSuchElementException("%s is missing".formatted(key));
	}

	@Override
	public boolean hasKey(Object key) {
		return map.get(key) != null;
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public void put(Object key, Object value) {
		map.put(key, value);
	}

	@Override
	public void delete(Object key) {
		map.remove(key);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return map.entrySet().stream();
	}
}
