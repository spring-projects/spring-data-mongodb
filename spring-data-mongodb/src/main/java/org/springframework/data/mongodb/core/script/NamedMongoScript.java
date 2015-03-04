/*
 * Copyright 2014-2015 the original author or authors.
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

import org.springframework.data.annotation.Id;
import org.springframework.util.Assert;

/**
 * An {@link ExecutableMongoScript} assigned to a name that allows calling the function by its {@literal name} once it
 * has been saved to the {@link com.mongodb.DB} instance.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
public class NamedMongoScript {

	private final @Id String name;
	private final ExecutableMongoScript script;

	/**
	 * Creates new {@link NamedMongoScript} that can be saved to the {@link com.mongodb.DB} instance.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @param rawScript the {@link String} representation of the {@literal JavaScript} function. Must not be
	 *          {@literal null} or empty.
	 */
	public NamedMongoScript(String name, String rawScript) {
		this(name, new ExecutableMongoScript(rawScript));
	}

	/**
	 * Creates new {@link NamedMongoScript}.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @param script must not be {@literal null}.
	 */
	public NamedMongoScript(String name, ExecutableMongoScript script) {

		Assert.hasText(name, "Name must not be null or empty!");
		Assert.notNull(script, "ExecutableMongoScript must not be null!");

		this.name = name;
		this.script = script;
	}

	/**
	 * Returns the actual script code.
	 * 
	 * @return will never be {@literal null}.
	 */
	public String getCode() {
		return script.getCode();
	}

	/**
	 * Returns the underlying {@link ExecutableMongoScript}.
	 * 
	 * @return will never be {@literal null}.
	 */
	public ExecutableMongoScript getScript() {
		return script;
	}

	/**
	 * Returns the name of the script.
	 * 
	 * @return will never be {@literal null} or empty.
	 */
	public String getName() {
		return name;
	}
}
