/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.springframework.data.mongodb.util.MongoClientVersion.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;
import java.util.Map;

import com.mongodb.MongoException;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteResult;

/**
 * {@link ReflectiveWriteResultInvoker} provides reflective access to {@link WriteResult} API that is not consistently
 * available for various driver versions.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.7
 */
final class ReflectiveWriteResultInvoker {

	private static final Method GET_ERROR_METHOD;
	private static final Method WAS_ACKNOWLEDGED_METHOD;
	private static final Method GET_RESPONSE;
	private static final Method GET_COMMAND_RESULT;

	private ReflectiveWriteResultInvoker() {}

	static {

		GET_ERROR_METHOD = findMethod(WriteResult.class, "getError");
		WAS_ACKNOWLEDGED_METHOD = findMethod(WriteResult.class, "wasAcknowledged");
		GET_RESPONSE = findMethod(WriteConcernException.class, "getResponse");
		GET_COMMAND_RESULT = findMethod(WriteConcernException.class, "getCommandResult");
	}

	/**
	 * @param writeResult can be {@literal null} for MongoDB Java driver version 3.
	 * @return null in case of MongoDB Java driver version 3 since errors are thrown as {@link MongoException}.
	 */
	public static String getError(WriteResult writeResult) {

		if (isMongo3Driver()) {
			return null;
		}

		return (String) invokeMethod(GET_ERROR_METHOD, writeResult);
	}

	/**
	 * @param writeResult
	 * @return return in case of MongoDB Java driver version 2.
	 */
	public static boolean wasAcknowledged(WriteResult writeResult) {
		return isMongo3Driver() ? ((Boolean) invokeMethod(WAS_ACKNOWLEDGED_METHOD, writeResult)).booleanValue() : true;
	}

	/**
	 * @param writeConcernException
	 * @return return {@literal true} if the {@link WriteConcernException} indicates a write concern timeout as reason
	 * @since 1.10
	 */
	@SuppressWarnings("unchecked")
	public static boolean wasTimeout(WriteConcernException writeConcernException) {

		Map<Object, Object> response;
		if (isMongo3Driver()) {
			response = (Map<Object, Object>) invokeMethod(GET_RESPONSE, writeConcernException);
		} else {
			response = (Map<Object, Object>) invokeMethod(GET_COMMAND_RESULT, writeConcernException);
		}

		if (response != null && response.containsKey("wtimeout")) {
			Object wtimeout = response.get("wtimeout");
			if (wtimeout != null && wtimeout.toString().contains("true")) {
				return true;
			}
		}

		return false;
	}
}
