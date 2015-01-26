/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import com.mongodb.BasicDBList;

/**
 * @author Christoph Strobl
 */
public class BasicDbListBuilder {

	private final BasicDBList dbl;

	public BasicDbListBuilder() {
		this.dbl = new BasicDBList();
	}

	public BasicDbListBuilder add(Object value) {

		this.dbl.add(value);
		return this;
	}

	public BasicDBList get() {
		return this.dbl;
	}

}
