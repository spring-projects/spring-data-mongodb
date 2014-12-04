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
package org.springframework.data.mongodb.core;

import java.io.Serializable;

import org.springframework.data.mongodb.core.script.CallableMongoScript;
import org.springframework.data.mongodb.core.script.MongoScript;

/**
 * Script operations on {@link com.mongodb.DB} level.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public interface ScriptOperations {

	/**
	 * Saves given {@literal script} to currently used {@link com.mongodb.DB}.
	 * 
	 * @param script must not be {@literal null}.
	 * @return
	 */
	CallableMongoScript save(MongoScript script);

	/**
	 * Executes the {@literal script} by either calling it via its {@literal _id} or directly sending it.
	 * 
	 * @param script must not be {@literal null}.
	 * @param args arguments to pass on for script execution.
	 * @return the script evaluation result.
	 * @throws org.springframework.dao.DataAccessException
	 */
	Object execute(MongoScript script, Object... args);

	/**
	 * Retrieves the {@link CallableMongoScript} by its {@literal id}.
	 * 
	 * @param name
	 * @return {@literal null} if not found.
	 */
	CallableMongoScript load(Serializable name);

}
