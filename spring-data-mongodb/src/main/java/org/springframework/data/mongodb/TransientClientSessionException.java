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
 * {@link TransientDataAccessException} specific to MongoDB {@link com.mongodb.session.ClientSession} related data
 * access failures such as reading data using an already closed session.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public class TransientClientSessionException extends TransientMongoDbException {

	/**
	 * Constructor for {@link TransientClientSessionException}.
	 *
	 * @param msg the detail message. Can be {@literal null}.
	 * @param cause the root cause. Can be {@literal null}.
	 */
	public TransientClientSessionException(@Nullable String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}
