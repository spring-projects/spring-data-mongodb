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
package org.springframework.data.mongodb.core;

import static java.util.UUID.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.data.mongodb.core.script.NamedMongoScript;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.DB;
import com.mongodb.MongoException;

/**
 * Default implementation of {@link ScriptOperations} capable of saving and executing {@link ServerSideJavaScript}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
class DefaultScriptOperations implements ScriptOperations {

	private static final String SCRIPT_COLLECTION_NAME = "system.js";
	private static final String SCRIPT_NAME_PREFIX = "func_";

	private final MongoOperations mongoOperations;

	/**
	 * Creates new {@link DefaultScriptOperations} using given {@link MongoOperations}.
	 * 
	 * @param mongoOperations must not be {@literal null}.
	 */
	public DefaultScriptOperations(MongoOperations mongoOperations) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");

		this.mongoOperations = mongoOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#register(org.springframework.data.mongodb.core.script.ExecutableMongoScript)
	 */
	@Override
	public NamedMongoScript register(ExecutableMongoScript script) {
		return register(new NamedMongoScript(generateScriptName(), script));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#register(org.springframework.data.mongodb.core.script.NamedMongoScript)
	 */
	@Override
	public NamedMongoScript register(NamedMongoScript script) {

		Assert.notNull(script, "Script must not be null!");

		mongoOperations.save(script, SCRIPT_COLLECTION_NAME);
		return script;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#execute(org.springframework.data.mongodb.core.script.ExecutableMongoScript, java.lang.Object[])
	 */
	@Override
	public Object execute(final ExecutableMongoScript script, final Object... args) {

		Assert.notNull(script, "Script must not be null!");

		return mongoOperations.execute(new DbCallback<Object>() {

			@Override
			public Object doInDB(DB db) throws MongoException, DataAccessException {
				return db.eval(script.getCode(), convertScriptArgs(args));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#call(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Object call(final String scriptName, final Object... args) {

		Assert.hasText(scriptName, "ScriptName must not be null or empty!");

		return mongoOperations.execute(new DbCallback<Object>() {

			@Override
			public Object doInDB(DB db) throws MongoException, DataAccessException {
				return db.eval(String.format("%s(%s)", scriptName, convertAndJoinScriptArgs(args)));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#exists(java.lang.String)
	 */
	@Override
	public boolean exists(String scriptName) {

		Assert.hasText(scriptName, "ScriptName must not be null or empty!");

		return mongoOperations.exists(query(where("name").is(scriptName)), NamedMongoScript.class, SCRIPT_COLLECTION_NAME);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#getScriptNames()
	 */
	@Override
	public Set<String> getScriptNames() {

		List<NamedMongoScript> scripts = mongoOperations.findAll(NamedMongoScript.class, SCRIPT_COLLECTION_NAME);

		if (CollectionUtils.isEmpty(scripts)) {
			return Collections.emptySet();
		}

		Set<String> scriptNames = new HashSet<String>();

		for (NamedMongoScript script : scripts) {
			scriptNames.add(script.getName());
		}

		return scriptNames;
	}

	private Object[] convertScriptArgs(Object... args) {

		if (ObjectUtils.isEmpty(args)) {
			return args;
		}

		List<Object> convertedValues = new ArrayList<Object>(args.length);

		for (Object arg : args) {
			convertedValues.add(arg instanceof String ? String.format("'%s'", arg) : this.mongoOperations.getConverter()
					.convertToMongoType(arg));
		}

		return convertedValues.toArray();
	}

	private String convertAndJoinScriptArgs(Object... args) {
		return ObjectUtils.isEmpty(args) ? "" : StringUtils.arrayToCommaDelimitedString(convertScriptArgs(args));
	}

	/**
	 * Generate a valid name for the {@literal JavaScript}. MongoDB requires an id of type String for scripts. Calling
	 * scripts having {@link ObjectId} as id fails. Therefore we create a random UUID without {@code -} (as this won't
	 * work) an prefix the result with {@link #SCRIPT_NAME_PREFIX}.
	 * 
	 * @return
	 */
	private static String generateScriptName() {
		return SCRIPT_NAME_PREFIX + randomUUID().toString().replaceAll("-", "");
	}
}
