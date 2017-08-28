/*
 * Copyright 2014-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver.LazyLoadingInterceptor;
import org.springframework.lang.Nullable;

import com.mongodb.DBRef;

/**
 * Allows direct interaction with the underlying {@link LazyLoadingInterceptor}.
 *
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.5
 */
public interface LazyLoadingProxy {

	/**
	 * Initializes the proxy and returns the wrapped value.
	 *
	 * @return
	 * @since 1.5
	 */
	Object getTarget();

	/**
	 * Returns the {@link DBRef} represented by this {@link LazyLoadingProxy}, may be null.
	 *
	 * @return
	 * @since 1.5
	 */
	@Nullable
	DBRef toDBRef();
}
