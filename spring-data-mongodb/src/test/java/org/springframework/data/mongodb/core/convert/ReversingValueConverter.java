/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
class ReversingValueConverter implements MongoValueConverter<String, String> {

	@Nullable
	@Override
	public String read(@Nullable String value, MongoConversionContext context) {
		return reverse(value);
	}

	@Nullable
	@Override
	public String write(@Nullable String value, MongoConversionContext context) {
		return reverse(value);
	}

	private String reverse(String source) {

		if (source == null) {
			return null;
		}

		return new StringBuilder(source).reverse().toString();
	}
}
