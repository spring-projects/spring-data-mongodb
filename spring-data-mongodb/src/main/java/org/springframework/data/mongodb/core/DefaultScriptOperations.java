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

import static java.util.UUID.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.script.CallableMongoScript;
import org.springframework.data.mongodb.core.script.MongoScript;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.DB;
import com.mongodb.MongoException;

/**
 * Default implementation of {@link ScriptOperations} capable of saving and executing simple {@link MongoScript}.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class DefaultScriptOperations implements ScriptOperations {

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
	 * @see org.springframework.data.mongodb.core.ScriptOperations#save(org.springframework.data.mongodb.core.script.MongoScript)
	 */
	@Override
	public CallableMongoScript save(MongoScript script) {

		Assert.notNull(script, "Script must not be null!");
		CallableMongoScript callableScript = null;

		if (script instanceof CallableMongoScript) {
			callableScript = (CallableMongoScript) script;
		} else {
			callableScript = new CallableMongoScript(generateScriptName(), script);
		}

		mongoOperations.save(callableScript, SCRIPT_COLLECTION_NAME);
		return callableScript;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#execute(org.springframework.data.mongodb.core.script.MongoScript, java.lang.Object[])
	 */
	@Override
	public Object execute(final MongoScript script, final Object... args) {

		Assert.notNull(script, "Script must not be null!");
		return mongoOperations.execute(new DbCallback<Object>() {

			@Override
			public Object doInDB(DB db) throws MongoException, DataAccessException {

				if (script instanceof CallableMongoScript) {

					String evalString = ((CallableMongoScript) script).getName() + "(" + convertAndJoinScriptArgs(args) + ")";
					return db.eval(evalString);
				}

				Assert.notNull(script.getCode(), "Script.code must not be null!");
				return db.eval(script.getCode().toString(), convertScriptArgs(args));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ScriptOperations#load(java.io.Serializable)
	 */
	@Override
	public CallableMongoScript load(Serializable name) {

		Assert.notNull(name, "Name must not be null!");
		return mongoOperations.findOne(query(where("name").is(name)), CallableMongoScript.class, SCRIPT_COLLECTION_NAME);
	}

	/**
	 * Generate a valid name for the script. MongoDB requires a script id of type String. Calling scripts having ObjectId
	 * as id fails. Therefore we create a random UUID without {@code -} (as this won't work) an prefix the result with
	 * {@link #SCRIPT_NAME_PREFIX}.
	 * 
	 * @return
	 */
	private String generateScriptName() {
		return SCRIPT_NAME_PREFIX + randomUUID().toString().replaceAll("-", "");
	}

	private Object[] convertScriptArgs(Object... args) {

		if (ObjectUtils.isEmpty(args)) {
			return args;
		}

		List<Object> convertedValues = new ArrayList<Object>(args.length);
		for (Object arg : args) {
			if (arg instanceof String) {
				convertedValues.add("'" + arg + "'");
			} else {
				convertedValues.add(this.mongoOperations.getConverter().convertToMongoType(arg));
			}
		}
		return convertedValues.toArray();
	}

	private String convertAndJoinScriptArgs(Object... args) {

		if (ObjectUtils.isEmpty(args)) {
			return "";
		}

		return StringUtils.arrayToCommaDelimitedString(convertScriptArgs(args));
	}

}
