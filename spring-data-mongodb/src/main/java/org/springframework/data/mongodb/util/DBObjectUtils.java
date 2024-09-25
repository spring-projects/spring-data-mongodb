/*
 * Copyright 2013-2024 the original author or authors.
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

import java.util.Arrays;

import com.mongodb.BasicDBList;

/**
 * @author Thomas Darimont
 * @deprecated since 4.2.0
 */
@Deprecated(since = "4.2.0", forRemoval = true)
public class DBObjectUtils {

	public static BasicDBList dbList(Object... items) {

		BasicDBList list = new BasicDBList();
		list.addAll(Arrays.asList(items));
		return list;
	}
}
