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
package org.springframework.data.mongodb;

import static org.springframework.data.mongodb.MongoClientVersion.*;
import static org.springframework.util.Assert.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;

import com.mongodb.MapReduceCommand;

/**
 * {@link ReflectiveMapReduceInvoker} provides reflective access to {@link MapReduceCommand} API that is not
 * consistently available for various driver versions.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public final class ReflectiveMapReduceInvoker {

	private static final Method ADD_EXTRA_OPTION_METHOD;

	static {

		ADD_EXTRA_OPTION_METHOD = findMethod(MapReduceCommand.class, "addExtraOption", String.class, Object.class);
	}

	private ReflectiveMapReduceInvoker() {}

	/**
	 * Sets the extra option for mongo-java-driver version 2. Will do nothing for mongo-java-driver version 2.
	 * 
	 * @param cmd can be {@literal null} for mongo-java-driver version 2.
	 * @param key
	 * @param value
	 */
	public static void addExtraOption(MapReduceCommand cmd, String key, Object value) {

		if (isMongo3Driver()) {
			return;
		}

		notNull(cmd, "MapReduceCommand must not be null!");
		invokeMethod(ADD_EXTRA_OPTION_METHOD, cmd, key, value);
	}

}
