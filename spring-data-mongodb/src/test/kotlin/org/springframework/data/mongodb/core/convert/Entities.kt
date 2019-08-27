/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.data.mongodb.core.convert

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document(collection = "domain_events")
data class StoredEvent<AGGREGATE, ID>(@Id val sequenceNo: Long = 0, val event: DomainEvent<AGGREGATE, ID>? = null)


abstract class DomainEvent<AGGREGATE, ID>(val aggregateId: ID, val root: AGGREGATE) {

	override fun toString(): String {
		return "DomainEvent(aggregateId=$aggregateId, root=$root)"
	}
}


class OfferCreated(aggregateId: Long, root: OfferDetails)
	: DomainEvent<OfferDetails, Long>(aggregateId, root) {

	override fun equals(other: Any?): Boolean {
		if (this === other) return true
		if (javaClass != other?.javaClass) return false

		other as OfferCreated

		if (aggregateId != other.aggregateId) return false
		if (root != other.root) return false

		return true
	}

	override fun hashCode(): Int {
		var result = aggregateId.hashCode()
		result = 31 * result + root.hashCode()
		return result
	}

	override fun toString(): String {
		return "OfferCreated(aggregateId=$aggregateId, root=$root)"
	}
}

data class OfferDetails(val name: String)
