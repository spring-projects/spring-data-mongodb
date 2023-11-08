/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import java.util.Objects;

/**
 * @author Christoph Strobl
 */
final class SumAge {

	private final Long total;

	public SumAge(Long total) {
		this.total = total;
	}

	public Long getTotal() {
		return this.total;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SumAge sumAge = (SumAge) o;
		return Objects.equals(total, sumAge.total);
	}

	@Override
	public int hashCode() {
		return Objects.hash(total);
	}

	public String toString() {
		return "SumAge(total=" + this.getTotal() + ")";
	}
}
