/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@Data
@NoArgsConstructor
public class UserWithLikes {

	String id;
	Date joined;
	Set<String> likes = new HashSet<String>();

	public UserWithLikes(String id, Date joined, String... likes) {

		this.id = id;
		this.joined = joined;
		this.likes = new HashSet<String>(Arrays.asList(likes));
	}
}
