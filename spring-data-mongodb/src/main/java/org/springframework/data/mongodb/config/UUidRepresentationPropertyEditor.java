/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.mongodb.config;

import java.beans.PropertyEditorSupport;

import org.bson.UuidRepresentation;
import org.jspecify.annotations.Nullable;
import org.springframework.util.StringUtils;

/**
 * Parse a {@link String} to a {@link UuidRepresentation}.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
public class UUidRepresentationPropertyEditor extends PropertyEditorSupport {

	@Override
	public void setAsText(@Nullable String value) {

		if (!StringUtils.hasText(value)) {
			return;
		}

		setValue(UuidRepresentation.valueOf(value));
	}
}
