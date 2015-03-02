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

import org.springframework.beans.DirectFieldAccessor;

import com.mongodb.WriteConcern;

/**
 * {@link ReflectiveWriteConcernInvoker} provides reflective access to {@link WriteConcern} API that is not consistently
 * available for various driver versions.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
class ReflectiveWriteConcernInvoker {

	private static final WriteConcern NONE_OR_UNACKNOWLEDGED;

	static {

		NONE_OR_UNACKNOWLEDGED = isMongo3Driver() ? WriteConcern.UNACKNOWLEDGED : (WriteConcern) new DirectFieldAccessor(
				new WriteConcern()).getPropertyValue("NONE");
	}

	/**
	 * @return {@link WriteConcern#NONE} for MongoDB Java driver version 2, otherwise {@link WriteConcern#UNACKNOWLEDGED}.
	 */
	public static WriteConcern noneOrUnacknowledged() {
		return NONE_OR_UNACKNOWLEDGED;
	}
}
