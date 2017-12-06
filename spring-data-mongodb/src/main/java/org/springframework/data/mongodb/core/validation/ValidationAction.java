/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core.validation;

import lombok.Getter;

/**
 * Determines whether to error on invalid documents or just warn about the violations but allow invalid documents to be
 * inserted.
 * 
 * @author Andreas Zink
 * @since 2.1
 * @see <a href="https://docs.mongodb.com/manual/reference/method/db.createCollection/">MongoDB Collection Options</a>
 */
public enum ValidationAction {

	/**
	 * Documents must pass validation before the write occurs. Otherwise, the write operation fails. (MongoDB default)
	 */
	ERROR("error"),

	/**
	 * Documents do not have to pass validation. If the document fails validation, the write operation logs the validation
	 * failure.
	 */
	WARN("warn");

	@Getter private String value;

	private ValidationAction(String value) {
		this.value = value;
	}
}
