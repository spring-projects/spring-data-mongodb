/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 1.5
 */
public class CompoundIndexDefinition extends Index {

	private DBObject keys;

	public CompoundIndexDefinition(DBObject keys) {
		this.keys = keys;
	}

	@Override
	public DBObject getIndexKeys() {

		BasicDBObject dbo = new BasicDBObject();
		dbo.putAll(this.keys);
		dbo.putAll(super.getIndexKeys());
		return dbo;
	}

}
