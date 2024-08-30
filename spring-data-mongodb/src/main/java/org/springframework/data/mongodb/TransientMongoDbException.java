/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb;

import org.springframework.dao.TransientDataAccessException;
import org.springframework.lang.Nullable;

/**
 * Root of the hierarchy of MongoDB specific data access exceptions that are considered transient such as
 * {@link com.mongodb.MongoException MongoExceptions} carrying {@link com.mongodb.MongoException#hasErrorLabel(String)
 * specific labels}.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public class TransientMongoDbException extends TransientDataAccessException {

	/**
	 * Constructor for {@link TransientMongoDbException}.
	 *
	 * @param msg the detail message. Can be {@literal null}.
	 * @param cause the root cause. Can be {@literal null}.
	 */
	public TransientMongoDbException(String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}
