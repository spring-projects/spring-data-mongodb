/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.mongodb.ServerApi;
import com.mongodb.ServerApi.Builder;
import com.mongodb.ServerApiVersion;

/**
 * {@link FactoryBean} for creating {@link ServerApi} using the {@link ServerApi.Builder}.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public class MongoServerApiFactoryBean implements FactoryBean<ServerApi> {

	private String version;
	private @Nullable Boolean deprecationErrors;
	private @Nullable Boolean strict;

	/**
	 * @param version the version string either as the enum name or the server version value.
	 * @see ServerApiVersion
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * @param deprecationErrors
	 * @see ServerApi.Builder#deprecationErrors(boolean)
	 */
	public void setDeprecationErrors(@Nullable Boolean deprecationErrors) {
		this.deprecationErrors = deprecationErrors;
	}

	/**
	 * @param strict
	 * @see ServerApi.Builder#strict(boolean)
	 */
	public void setStrict(@Nullable Boolean strict) {
		this.strict = strict;
	}

	@Nullable
	@Override
	public ServerApi getObject() throws Exception {

		Builder builder = ServerApi.builder().version(version());

		if (deprecationErrors != null) {
			builder = builder.deprecationErrors(deprecationErrors);
		}
		if (strict != null) {
			builder = builder.strict(strict);
		}
		return builder.build();
	}

	@Nullable
	@Override
	public Class<?> getObjectType() {
		return ServerApi.class;
	}

	private ServerApiVersion version() {
		try {
			// lookup by name eg. 'V1'
			return ObjectUtils.caseInsensitiveValueOf(ServerApiVersion.values(), version);
		} catch (IllegalArgumentException e) {
			// or just the version number, eg. just '1'
			return ServerApiVersion.findByValue(version);
		}
	}
}
