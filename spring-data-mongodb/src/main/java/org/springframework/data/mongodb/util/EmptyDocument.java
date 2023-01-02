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
package org.springframework.data.mongodb.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.bson.Document;
import org.jetbrains.annotations.Nullable;

/**
 * Empty variant of {@link Document}.
 *
 * @author Mark Paluch
 */
class EmptyDocument extends Document {

	@Override
	public Document append(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object put(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends String, ?> map) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean replace(String key, Object oldValue, Object newValue) {
		throw new UnsupportedOperationException();
	}

	@Nullable
	@Override
	public Object replace(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return Collections.emptySet();
	}

	@Override
	public Collection<Object> values() {
		return Collections.emptyList();
	}

	@Override
	public Set<String> keySet() {
		return Collections.emptySet();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

}
