/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

/**
 * Lookup policy for aggregation fields. Allows strict lookups that fail if the field is absent or lenient ones that
 * pass-thru the requested field even if we have to assume that the field isn't present because of the limited scope of
 * our input.
 *
 * @author Mark Paluch
 * @since xxx
 */
public abstract class FieldLookupPolicy {

	private static final FieldLookupPolicy STRICT = new FieldLookupPolicy() {
		@Override
		boolean isStrict() {
			return true;
		}
	};

	private static final FieldLookupPolicy LENIENT = new FieldLookupPolicy() {
		@Override
		boolean isStrict() {
			return false;
		}
	};

	private FieldLookupPolicy() {}

	/**
	 * @return a lenient lookup policy.
	 */
	public static FieldLookupPolicy lenient() {
		return LENIENT;
	}

	/**
	 * @return a strict lookup policy.
	 */
	public static FieldLookupPolicy strict() {
		return STRICT;
	}

	/**
	 * @return {@code true} if the policy uses a strict lookup; {@code false} to allow references to fields that cannot be
	 *         determined to be exactly present.
	 */
	abstract boolean isStrict();

}
