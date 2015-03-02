/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.config;

import java.beans.PropertyEditorSupport;

import com.mongodb.ReadPreference;

/**
 * Parse a {@link String} to a {@link ReadPreference}.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class ReadPreferencePropertyEditor extends PropertyEditorSupport {

	@Override
	public void setAsText(String readPreferenceString) throws IllegalArgumentException {

		if (readPreferenceString == null) {
			return;
		}

		ReadPreference preference = null;
		try {
			preference = ReadPreference.valueOf(readPreferenceString);
		} catch (IllegalArgumentException ex) {
			// ignore this one and try to map it differently
		}

		if (preference != null) {
			setValue(preference);
		} else if ("PRIMARY".equalsIgnoreCase(readPreferenceString)) {
			setValue(ReadPreference.primary());
		} else if ("PRIMARY_PREFERRED".equalsIgnoreCase(readPreferenceString)) {
			setValue(ReadPreference.primaryPreferred());
		} else if ("SECONDARY".equalsIgnoreCase(readPreferenceString)) {
			setValue(ReadPreference.secondary());
		} else if ("SECONDARY_PREFERRED".equalsIgnoreCase(readPreferenceString)) {
			setValue(ReadPreference.secondaryPreferred());
		} else if ("NEAREST".equalsIgnoreCase(readPreferenceString)) {
			setValue(ReadPreference.nearest());
		} else {
			throw new IllegalArgumentException(String.format("Cannot find matching ReadPreference for %s",
					readPreferenceString));
		}
	}
}
