/*
 * Copyright 2020. the original author or authors.
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

/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.util;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class Fields<O> implements Iterable<Field<?, O>> {

	private final Class<O> owner;
	private final Map<String, Field<?, O>> fields;

	public Fields(Class<O> owner) {

		this.owner = owner;
		this.fields = new LinkedHashMap<>();
	}

	public Fields<O> add(Field<?, O> field) {

		this.fields.put(field.getFieldName(), field.owner(owner));
		return this;
	}

	public boolean hasField(String fieldName) {
		return this.fields.containsKey(fieldName);
	}

	public <S> Field<S, O> getField(String fieldName) {
		return (Field<S, O>) this.fields.get(fieldName);
	}

	public void doWithFields(BiConsumer<String, Field<?, O>> consumer) {
		fields.forEach(consumer);
	}

	@Override
	public Iterator<Field<?, O>> iterator() {
		return fields.values().iterator();
	}
}
