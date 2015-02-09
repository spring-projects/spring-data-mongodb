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
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;

import com.mongodb.MongoException;
import com.mongodb.WriteResult;

/**
 * {@link ReflectiveWriteResultInvoker} provides reflective access to {@link WriteResult} API that is not consistently
 * available for various driver versions.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public final class ReflectiveWriteResultInvoker {

	private static final Method GET_ERROR_METHOD;
	private static final Method WAS_ACKNOWLEDGED_METHOD;

	private ReflectiveWriteResultInvoker() {}

	static {

		GET_ERROR_METHOD = findMethod(WriteResult.class, "getError");
		WAS_ACKNOWLEDGED_METHOD = findMethod(WriteResult.class, "wasAcknowledged");
	}

	/**
	 * @param writeResult can be {@literal null} for mongo-java-driver version 3.
	 * @return null in case of mongo-java-driver version 3 since errors are thrown as {@link MongoException}.
	 */
	public static String getError(WriteResult writeResult) {

		if (isMongo3Driver()) {
			return null;
		}

		return (String) invokeMethod(GET_ERROR_METHOD, writeResult);
	}

	/**
	 * @param writeResult
	 * @return return in case of mongo-java-driver version 2.
	 */
	public static boolean wasAcknowledged(WriteResult writeResult) {
		return isMongo3Driver() ? ((Boolean) invokeMethod(WAS_ACKNOWLEDGED_METHOD, writeResult)).booleanValue() : true;
	}

}
