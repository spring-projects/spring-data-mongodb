/*
 * Copyright 2019 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.ConnectionString;

/**
 * Parse a {@link String} to a {@link com.mongodb.ConnectionString}.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
public class ConnectionStringPropertyEditor extends PropertyEditorSupport {

	/*
	 * (non-Javadoc)
	 * @see java.beans.PropertyEditorSupport#setAsText(java.lang.String)
	 */
	@Override
	public void setAsText(@Nullable String connectionString) {

		if (!StringUtils.hasText(connectionString)) {
			return;
		}

		setValue(new ConnectionString(connectionString));
	}
}
