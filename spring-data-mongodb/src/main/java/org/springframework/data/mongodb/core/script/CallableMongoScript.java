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
package org.springframework.data.mongodb.core.script;

import org.bson.types.Code;
import org.springframework.data.annotation.Id;
import org.springframework.util.Assert;

/**
 * A {@link MongoScript} implementation that allows calling the function by its {@literal id} once it has been saved to
 * the {@link com.mongodb.DB} instance.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class CallableMongoScript implements MongoScript {

	private final @Id String name;
	private final MongoScript script;

	/**
	 * Creates new {@link CallableMongoScript} that uses the given {@literal id} to call the function on the server.
	 * 
	 * @param name must not be {@literal null} or {@literal empty}.
	 */
	public CallableMongoScript(String name) {
		this(name, (MongoScript) null);
	}

	/**
	 * Creates new {@link CallableMongoScript} that can be saved to the {@link com.mongodb.DB} instance.
	 * 
	 * @param name must not be {@literal null} or {@literal empty}.
	 * @param rawScript the {@link String} representation of the javascript function. Must not be {@literal null} or
	 *          {@literal empty}.
	 */
	public CallableMongoScript(String name, String rawScript) {
		this(name, new ExecutableMongoScript(rawScript));
	}

	/**
	 * Creates new {@link CallableMongoScript}.
	 * 
	 * @param name must not be {@literal null} or {@literal empty}.
	 * @param script can be {@literal null}.
	 */
	public CallableMongoScript(String name, MongoScript script) {

		Assert.hasText(name, "Name must not be null or empty!");
		this.name = name;
		this.script = script;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.script.MongoScript#getCode()
	 */
	@Override
	public Code getCode() {

		if (script == null) {
			return null;
		}

		return script.getCode();
	}

	/**
	 * Get the id of the callable script.
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}

}
