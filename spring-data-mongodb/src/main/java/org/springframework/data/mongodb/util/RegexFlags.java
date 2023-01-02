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
package org.springframework.data.mongodb.util;

import java.util.regex.Pattern;

import org.springframework.lang.Nullable;

/**
 * Utility to translate {@link Pattern#flags() regex flags} to MongoDB regex options and vice versa.
 *
 * @author Mark Paluch
 * @since 3.3
 */
public abstract class RegexFlags {

	private static final int[] FLAG_LOOKUP = new int[Character.MAX_VALUE];

	static {
		FLAG_LOOKUP['g'] = 256;
		FLAG_LOOKUP['i'] = Pattern.CASE_INSENSITIVE;
		FLAG_LOOKUP['m'] = Pattern.MULTILINE;
		FLAG_LOOKUP['s'] = Pattern.DOTALL;
		FLAG_LOOKUP['c'] = Pattern.CANON_EQ;
		FLAG_LOOKUP['x'] = Pattern.COMMENTS;
		FLAG_LOOKUP['d'] = Pattern.UNIX_LINES;
		FLAG_LOOKUP['t'] = Pattern.LITERAL;
		FLAG_LOOKUP['u'] = Pattern.UNICODE_CASE;
	}

	private RegexFlags() {

	}

	/**
	 * Lookup the MongoDB specific options from given {@link Pattern#flags() flags}.
	 *
	 * @param flags the Regex flags to look up.
	 * @return the options string. May be empty.
	 */
	public static String toRegexOptions(int flags) {

		if (flags == 0) {
			return "";
		}

		StringBuilder buf = new StringBuilder();

		for (int i = 'a'; i < 'z'; i++) {

			if (FLAG_LOOKUP[i] == 0) {
				continue;
			}

			if ((flags & FLAG_LOOKUP[i]) > 0) {
				buf.append((char) i);
			}
		}

		return buf.toString();
	}

	/**
	 * Lookup the MongoDB specific flags for a given regex option string.
	 *
	 * @param s the Regex option/flag to look up. Can be {@literal null}.
	 * @return zero if given {@link String} is {@literal null} or empty.
	 * @since 2.2
	 */
	public static int toRegexFlags(@Nullable String s) {

		int flags = 0;

		if (s == null) {
			return flags;
		}

		for (char f : s.toLowerCase().toCharArray()) {
			flags |= toRegexFlag(f);
		}

		return flags;
	}

	/**
	 * Lookup the MongoDB specific flags for a given character.
	 *
	 * @param c the Regex option/flag to look up.
	 * @return
	 * @throws IllegalArgumentException for unknown flags
	 * @since 2.2
	 */
	public static int toRegexFlag(char c) {

		int flag = FLAG_LOOKUP[c];

		if (flag == 0) {
			throw new IllegalArgumentException(String.format("Unrecognized flag [%c]", c));
		}

		return flag;
	}
}
