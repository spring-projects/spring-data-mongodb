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
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.util.MongoClientVersion.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.util.ReflectionUtils;

import com.mongodb.MongoOptions;

/**
 * {@link ReflectiveMongoOptionsInvoker} provides reflective access to {@link MongoOptions} API that is not consistently
 * available for various driver versions.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
@SuppressWarnings("deprecation")
class ReflectiveMongoOptionsInvoker {

	private static final Method GET_AUTO_CONNECT_RETRY_METHOD;
	private static final Method SET_AUTO_CONNECT_RETRY_METHOD;
	private static final Method GET_MAX_AUTO_CONNECT_RETRY_TIME_METHOD;
	private static final Method SET_MAX_AUTO_CONNECT_RETRY_TIME_METHOD;

	static {

		SET_AUTO_CONNECT_RETRY_METHOD = ReflectionUtils
				.findMethod(MongoOptions.class, "setAutoConnectRetry", boolean.class);
		GET_AUTO_CONNECT_RETRY_METHOD = ReflectionUtils.findMethod(MongoOptions.class, "isAutoConnectRetry");
		SET_MAX_AUTO_CONNECT_RETRY_TIME_METHOD = ReflectionUtils.findMethod(MongoOptions.class,
				"setMaxAutoConnectRetryTime", long.class);
		GET_MAX_AUTO_CONNECT_RETRY_TIME_METHOD = ReflectionUtils.findMethod(MongoOptions.class,
				"getMaxAutoConnectRetryTime");
	}

	private ReflectiveMongoOptionsInvoker() {}

	/**
	 * Sets the retry connection flag for MongoDB Java driver version 2. Will do nothing for MongoDB Java driver version 3
	 * since the method has been removed.
	 * 
	 * @param options can be {@literal null} for MongoDB Java driver version 3.
	 * @param autoConnectRetry
	 */
	public static void setAutoConnectRetry(MongoOptions options, boolean autoConnectRetry) {

		if (isMongo3Driver()) {
			return;
		}

		invokeMethod(SET_AUTO_CONNECT_RETRY_METHOD, options, autoConnectRetry);
	}

	/**
	 * Sets the maxAutoConnectRetryTime attribute for MongoDB Java driver version 2. Will do nothing for MongoDB Java
	 * driver version 3 since the method has been removed.
	 * 
	 * @param options can be {@literal null} for MongoDB Java driver version 3.
	 * @param maxAutoConnectRetryTime
	 */
	public static void setMaxAutoConnectRetryTime(MongoOptions options, long maxAutoConnectRetryTime) {

		if (isMongo3Driver()) {
			return;
		}

		invokeMethod(SET_MAX_AUTO_CONNECT_RETRY_TIME_METHOD, options, maxAutoConnectRetryTime);
	}

	/**
	 * Sets the slaveOk attribute for MongoDB Java driver version 2. Will do nothing for MongoDB Java driver version 3
	 * since the method has been removed.
	 * 
	 * @param options can be {@literal null} for MongoDB Java driver version 3.
	 * @param slaveOk
	 */
	public static void setSlaveOk(MongoOptions options, boolean slaveOk) {

		if (isMongo3Driver()) {
			return;
		}

		new DirectFieldAccessor(options).setPropertyValue("slaveOk", slaveOk);
	}

	/**
	 * Gets the slaveOk attribute for MongoDB Java driver version 2. Throws {@link UnsupportedOperationException} for
	 * MongoDB Java driver version 3 since the method has been removed.
	 * 
	 * @param options can be {@literal null} for MongoDB Java driver version 3.
	 * @return
	 * @throws UnsupportedOperationException
	 */
	public static boolean getSlaveOk(MongoOptions options) {

		if (isMongo3Driver()) {
			throw new UnsupportedOperationException(
					"Cannot get value for autoConnectRetry which has been removed in MongoDB Java driver version 3.");
		}

		return ((Boolean) new DirectFieldAccessor(options).getPropertyValue("slaveOk")).booleanValue();
	}

	/**
	 * Gets the autoConnectRetry attribute for MongoDB Java driver version 2. Throws {@link UnsupportedOperationException}
	 * for MongoDB Java driver version 3 since the method has been removed.
	 * 
	 * @param options can be {@literal null} for MongoDB Java driver version 3.
	 * @return
	 * @throws UnsupportedOperationException
	 */
	public static boolean getAutoConnectRetry(MongoOptions options) {

		if (isMongo3Driver()) {
			throw new UnsupportedOperationException(
					"Cannot get value for autoConnectRetry which has been removed in MongoDB Java driver version 3.");
		}

		return ((Boolean) invokeMethod(GET_AUTO_CONNECT_RETRY_METHOD, options)).booleanValue();
	}

	/**
	 * Gets the maxAutoConnectRetryTime attribute for MongoDB Java driver version 2. Throws
	 * {@link UnsupportedOperationException} for MongoDB Java driver version 3 since the method has been removed.
	 * 
	 * @param options can be {@literal null} for MongoDB Java driver version 3.
	 * @return
	 * @throws UnsupportedOperationException
	 */
	public static long getMaxAutoConnectRetryTime(MongoOptions options) {

		if (isMongo3Driver()) {
			throw new UnsupportedOperationException(
					"Cannot get value for maxAutoConnectRetryTime which has been removed in MongoDB Java driver version 3.");
		}

		return ((Long) invokeMethod(GET_MAX_AUTO_CONNECT_RETRY_TIME_METHOD, options)).longValue();
	}
}
