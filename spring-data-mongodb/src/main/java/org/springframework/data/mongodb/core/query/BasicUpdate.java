/*
 * Copyright 2010-2025 the original author or authors.
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.lang.Nullable;
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
		super();
		this.updateObject = Document.parse(updateString);
	}

	public BasicUpdate(Document updateObject) {
		super();
		this.updateObject = updateObject;
	}

	@Override
	public Update set(String key, @Nullable Object value) {
		setOperationValue("$set", key, value);
		return this;
	}

	@Override
	public Update unset(String key) {
		setOperationValue("$unset", key, 1);
		return this;
	}

	@Override
	public Update inc(String key, Number inc) {
		setOperationValue("$inc", key, inc);
		return this;
	}

	@Override
	public Update push(String key, @Nullable Object value) {
		setOperationValue("$push", key, value);
		return this;
	}

	@Override
	public Update addToSet(String key, @Nullable Object value) {
		setOperationValue("$addToSet", key, value);
		return this;
	}

	@Override
	public Update pop(String key, Position pos) {
		setOperationValue("$pop", key, (pos == Position.FIRST ? -1 : 1));
		return this;
	}

	@Override
	public Update pull(String key, @Nullable Object value) {
		setOperationValue("$pull", key, value);
		return this;
	}

	@Override
	public Update pullAll(String key, Object[] values) {
		setOperationValue("$pullAll", key, List.of(values));
		return this;
	}

	@Override
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

	void setOperationValue(String operator, String key, Object value) {

		if (!updateObject.containsKey(operator)) {
			updateObject.put(operator, Collections.singletonMap(key, value));
		} else {
			Object existingValue = updateObject.get(operator);
			if (existingValue instanceof Map<?, ?> existing) {
				Map<Object, Object> target = new LinkedHashMap<>(existing);
				target.put(key, value);
				updateObject.put(operator, target);
			} else {
				throw new IllegalStateException(
						"Cannot add ['%s' : { '%s' : ... }]. Operator already exists with value of type [%s] which is not suitable for appending"
								.formatted(operator, key,
										existingValue != null ? ClassUtils.getShortName(existingValue.getClass()) : "null"));
			}
		}
	}

}
