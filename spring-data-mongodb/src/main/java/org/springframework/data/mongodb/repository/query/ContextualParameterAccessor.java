/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import reactor.util.context.Context;

import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.util.Assert;

/**
 * Parameter accessor extension to {@link ConvertingParameterAccessor} that provides a Reactor subscriber
 * {@link Context}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class ContextualParameterAccessor extends ConvertingParameterAccessor {

	private final Context context;

	/**
	 * Creates a new {@link ConvertingParameterAccessor} with the given {@link MongoWriter} and delegate.
	 *
	 * @param writer must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 */
	public ContextualParameterAccessor(MongoWriter<?> writer, MongoParameterAccessor delegate, Context context) {

		super(writer, delegate);

		Assert.notNull(context, "Context must not be null!");
		this.context = context;
	}

	/**
	 * @return the context.
	 */
	public Context getContext() {
		return context;
	}
}
