/*
 * Copyright 2014-2019 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Set;

import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.data.mongodb.core.script.NamedMongoScript;
import org.springframework.lang.Nullable;


/**
 * Script operations on {@link com.mongodb.DB} level. Allows interaction with server side JavaScript functions.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 * @deprecated since 2.2. The {@code eval} command has been removed without replacement in MongoDB Server 4.2.0.
 */
@Deprecated
public interface ScriptOperations {

	/**
	 * Store given {@link ExecutableMongoScript} generating a syntheitcal name so that it can be called by it
	 * subsequently.
	 *
	 * @param script must not be {@literal null}.
	 * @return {@link NamedMongoScript} with name under which the {@code JavaScript} function can be called.
	 */
	NamedMongoScript register(ExecutableMongoScript script);

	/**
	 * Registers the given {@link NamedMongoScript} in the database.
	 *
	 * @param script the {@link NamedMongoScript} to be registered.
	 * @return
	 */
	NamedMongoScript register(NamedMongoScript script);

	/**
	 * Executes the {@literal script} by either calling it via its {@literal name} or directly sending it.
	 *
	 * @param script must not be {@literal null}.
	 * @param args arguments to pass on for script execution.
	 * @return the script evaluation result.
	 * @throws org.springframework.dao.DataAccessException
	 */
	@Nullable
	Object execute(ExecutableMongoScript script, Object... args);

	/**
	 * Call the {@literal JavaScript} by its name.
	 *
	 * @param scriptName must not be {@literal null} or empty.
	 * @param args
	 * @return
	 */
	@Nullable
	Object call(String scriptName, Object... args);

	/**
	 * Checks {@link DB} for existence of {@link ServerSideJavaScript} with given name.
	 *
	 * @param scriptName must not be {@literal null} or empty.
	 * @return false if no {@link ServerSideJavaScript} with given name exists.
	 */
	boolean exists(String scriptName);

	/**
	 * Returns names of {@literal JavaScript} functions that can be called.
	 *
	 * @return empty {@link Set} if no scripts found.
	 */
	Set<String> getScriptNames();
}
