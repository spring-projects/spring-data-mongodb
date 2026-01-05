/*
 * Copyright 2010-present the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.lang.Contract;
import org.springframework.util.ClassUtils;

/**
 * @author Thomas Risberg
 * @author John Brisbin
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class BasicUpdate extends Update {

	private final Document updateObject;

	public BasicUpdate(String updateString) {
		this(Document.parse(updateString));
	}

	public BasicUpdate(Document updateObject) {
		this.updateObject = updateObject;
	}

	@Override
	@Contract("_, _ -> this")
	public Update set(String key, @Nullable Object value) {
		setOperationValue("$set", key, value);
		return this;
	}

	@Override
	@Contract("_ -> this")
	public Update unset(String key) {
		setOperationValue("$unset", key, 1);
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update inc(String key, Number inc) {
		setOperationValue("$inc", key, inc);
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update push(String key, @Nullable Object value) {
		setOperationValue("$push", key, value);
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update addToSet(String key, @Nullable Object value) {
		setOperationValue("$addToSet", key, value);
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update pop(String key, Position pos) {
		setOperationValue("$pop", key, (pos == Position.FIRST ? -1 : 1));
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update pull(String key, @Nullable Object value) {
		setOperationValue("$pull", key, value);
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update pullAll(String key, Object[] values) {
		setOperationValue("$pullAll", key, List.of(values), (o, o2) -> {

			if (o instanceof List<?> prev && o2 instanceof List<?> currentValue) {
				List<Object> merged = new ArrayList<>(prev.size() + currentValue.size());
				merged.addAll(prev);
				merged.addAll(currentValue);
				return merged;
			}

			return o2;
		});
		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public Update rename(String oldName, String newName) {
		setOperationValue("$rename", oldName, newName);
		return this;
	}

	@Override
	public boolean modifies(String key) {
		return super.modifies(key) || Update.fromDocument(getUpdateObject()).modifies(key);
	}

	@Override
	public Document getUpdateObject() {
		return updateObject;
	}

	void setOperationValue(String operator, String key, @Nullable Object value) {
		setOperationValue(operator, key, value, (o, o2) -> o2);
	}

	void setOperationValue(String operator, String key, @Nullable Object value,
			BiFunction<Object, Object, Object> mergeFunction) {

		if (!updateObject.containsKey(operator)) {
			updateObject.put(operator, Collections.singletonMap(key, value));
		} else {
			Object o = updateObject.get(operator);
			if (o instanceof Map<?, ?> existing) {
				Map<Object, Object> target = new LinkedHashMap<>(existing);

				if (target.containsKey(key)) {
					target.put(key, mergeFunction.apply(target.get(key), value));
				} else {
					target.put(key, value);
				}
				updateObject.put(operator, target);
			} else {
				throw new IllegalStateException(
						"Cannot add ['%s' : { '%s' : ... }]. Operator already exists with value of type [%s] which is not suitable for appending"
								.formatted(operator, key,
										o != null ? ClassUtils.getShortName(o.getClass()) : "null"));
			}
		}
	}

}
