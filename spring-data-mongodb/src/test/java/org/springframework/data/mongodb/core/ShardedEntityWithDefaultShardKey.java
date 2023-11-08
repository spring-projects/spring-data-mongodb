/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Objects;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.Sharded;

/**
 * @author Christoph Strobl
 */
@Sharded
public class ShardedEntityWithDefaultShardKey {

	private @Id String id;

	private String country;

	@Field("userid") //
	private Integer userId;

	public ShardedEntityWithDefaultShardKey(String id, String country, Integer userId) {

		this.id = id;
		this.country = country;
		this.userId = userId;
	}

	public String getId() {
		return this.id;
	}

	public String getCountry() {
		return this.country;
	}

	public Integer getUserId() {
		return this.userId;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ShardedEntityWithDefaultShardKey that = (ShardedEntityWithDefaultShardKey) o;
		return Objects.equals(id, that.id) && Objects.equals(country, that.country) && Objects.equals(userId, that.userId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, country, userId);
	}

	public String toString() {
		return "ShardedEntityWithDefaultShardKey(id=" + this.getId() + ", country=" + this.getCountry() + ", userId="
				+ this.getUserId() + ")";
	}
}
