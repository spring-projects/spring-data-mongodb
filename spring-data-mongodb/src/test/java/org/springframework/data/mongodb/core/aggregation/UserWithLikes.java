/*
 * Copyright 2013-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class UserWithLikes {

	String id;
	Date joined;
	Set<String> likes = new HashSet<String>();

	public UserWithLikes() {}

	public UserWithLikes(String id, Date joined, String... likes) {

		this.id = id;
		this.joined = joined;
		this.likes = new HashSet<String>(Arrays.asList(likes));
	}

	public String getId() {
		return this.id;
	}

	public Date getJoined() {
		return this.joined;
	}

	public Set<String> getLikes() {
		return this.likes;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setJoined(Date joined) {
		this.joined = joined;
	}

	public void setLikes(Set<String> likes) {
		this.likes = likes;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		UserWithLikes that = (UserWithLikes) o;
		return Objects.equals(id, that.id) && Objects.equals(joined, that.joined) && Objects.equals(likes, that.likes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, joined, likes);
	}

	public String toString() {
		return "UserWithLikes(id=" + this.getId() + ", joined=" + this.getJoined() + ", likes=" + this.getLikes() + ")";
	}
}
