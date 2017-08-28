/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Arrays;
import java.util.Collections;

import org.bson.Document;

/**
 * @author Thomas Risberg
 * @author John Brisbin
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class BasicUpdate extends Update {

	private Document updateObject;

	public BasicUpdate(String updateString) {
		super();
		this.updateObject = Document.parse(updateString);
	}

	public BasicUpdate(Document updateObject) {
		super();
		this.updateObject = updateObject;
	}

	@Override
	public Update set(String key, Object value) {
		updateObject.put("$set", Collections.singletonMap(key, value));
		return this;
	}

	@Override
	public Update unset(String key) {
		updateObject.put("$unset", Collections.singletonMap(key, 1));
		return this;
	}

	@Override
	public Update inc(String key, Number inc) {
		updateObject.put("$inc", Collections.singletonMap(key, inc));
		return this;
	}

	@Override
	public Update push(String key, Object value) {
		updateObject.put("$push", Collections.singletonMap(key, value));
		return this;
	}

	@Override
	public Update pushAll(String key, Object[] values) {
		Document keyValue = new Document();
		keyValue.put(key, values);
		updateObject.put("$pushAll", keyValue);
		return this;
	}

	@Override
	public Update addToSet(String key, Object value) {
		updateObject.put("$addToSet", Collections.singletonMap(key, value));
		return this;
	}

	@Override
	public Update pop(String key, Position pos) {
		updateObject.put("$pop", Collections.singletonMap(key, (pos == Position.FIRST ? -1 : 1)));
		return this;
	}

	@Override
	public Update pull(String key, Object value) {
		updateObject.put("$pull", Collections.singletonMap(key, value));
		return this;
	}

	@Override
	public Update pullAll(String key, Object[] values) {
		Document keyValue = new Document();
		keyValue.put(key, Arrays.copyOf(values, values.length));
		updateObject.put("$pullAll", keyValue);
		return this;
	}

	@Override
	public Update rename(String oldName, String newName) {
		updateObject.put("$rename", Collections.singletonMap(oldName, newName));
		return this;
	}

	@Override
	public Document getUpdateObject() {
		return updateObject;
	}

}
