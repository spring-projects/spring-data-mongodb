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
 * Determines how strictly MongoDB applies the validation rules to existing documents during an update.
 * 
 * @author Andreas Zink
 * @since 2.1
 * @see <a href="https://docs.mongodb.com/manual/reference/method/db.createCollection/">MongoDB Collection Options</a>
 */
public enum ValidationLevel {

	/**
	 * No validation for inserts or updates.
	 */
	OFF("off"),

	/**
	 * Apply validation rules to all inserts and all updates. (MongoDB default)
	 */
	STRICT("strict"),

	/**
	 * Apply validation rules to inserts and to updates on existing valid documents. Do not apply rules to updates on
	 * existing invalid documents.
	 */
	MODERATE("moderate");

	@Getter private String value;

	private ValidationLevel(String value) {
		this.value = value;
	}
}
