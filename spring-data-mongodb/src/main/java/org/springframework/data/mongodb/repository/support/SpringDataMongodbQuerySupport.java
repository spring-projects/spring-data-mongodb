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
package org.springframework.data.mongodb.repository.support;

import java.util.List;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.mongodb.MongoClientSettings;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.mongodb.document.AbstractMongodbQuery;
import com.querydsl.mongodb.document.MongodbDocumentSerializer;

/**
 * Support query type to augment Spring Data-specific {@link #toString} representations and
 * {@link org.springframework.data.domain.Sort} creation.
 *
 * @author Mark Paluch
 * @since 3.3
 */
abstract class SpringDataMongodbQuerySupport<Q extends SpringDataMongodbQuerySupport<Q>>
		extends AbstractMongodbQuery<Q> {

	private final QueryMixin<Q> superQueryMixin;

	private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder().outputMode(JsonMode.SHELL)
			.build();

	private final MongodbDocumentSerializer serializer;

	@SuppressWarnings("unchecked")
	SpringDataMongodbQuerySupport(MongodbDocumentSerializer serializer) {

		super(serializer);
		this.serializer = serializer;
		this.superQueryMixin = super.getQueryMixin();
	}

	/**
	 * Returns the {@literal Mongo Shell} representation of the query. <br />
	 * The following query
	 *
	 * <pre class="code">
	 *
	 * where(p.lastname.eq("Matthews")).orderBy(p.firstname.asc()).offset(1).limit(5);
	 * </pre>
	 *
	 * results in
	 *
	 * <pre class="code">
	 *
	 * find({"lastname" : "Matthews"}).sort({"firstname" : 1}).skip(1).limit(5)
	 * </pre>
	 *
	 * Note that encoding to {@link String} may fail when using data types that cannot be encoded or DBRef's without an
	 * identifier.
	 *
	 * @return never {@literal null}.
	 */
	@Override
	public String toString() {

		Document projection = createProjection(getQueryMixin().getMetadata().getProjection());
		Document sort = createSort(getQueryMixin().getMetadata().getOrderBy());
		DocumentCodec codec = new DocumentCodec(MongoClientSettings.getDefaultCodecRegistry());

		StringBuilder sb = new StringBuilder("find(" + asDocument().toJson(JSON_WRITER_SETTINGS, codec));
		if (projection != null && projection.isEmpty()) {
			sb.append(", ").append(projection.toJson(JSON_WRITER_SETTINGS, codec));
		}
		sb.append(")");
		if (!sort.isEmpty()) {
			sb.append(".sort(").append(sort.toJson(JSON_WRITER_SETTINGS, codec)).append(")");
		}
		if (getQueryMixin().getMetadata().getModifiers().getOffset() != null) {
			sb.append(".skip(").append(getQueryMixin().getMetadata().getModifiers().getOffset()).append(")");
		}
		if (getQueryMixin().getMetadata().getModifiers().getLimit() != null) {
			sb.append(".limit(").append(getQueryMixin().getMetadata().getModifiers().getLimit()).append(")");
		}
		return sb.toString();
	}

	/**
	 * Get the where definition as a Document instance
	 *
	 * @return
	 */
	public Document asDocument() {
		return createQuery(getQueryMixin().getMetadata().getWhere());
	}

	/**
	 * Obtain the {@literal Mongo Shell} json query representation.
	 *
	 * @return never {@literal null}.
	 */
	public String toJson() {
		return toJson(JSON_WRITER_SETTINGS);
	}

	/**
	 * Obtain the json query representation applying given {@link JsonWriterSettings settings}.
	 *
	 * @param settings must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	public String toJson(JsonWriterSettings settings) {
		return asDocument().toJson(settings);
	}

	/**
	 * Compute the sort {@link Document} from the given list of {@link OrderSpecifier order specifiers}.
	 *
	 * @param orderSpecifiers can be {@literal null}.
	 * @return an empty {@link Document} if predicate is {@literal null}.
	 * @see MongodbDocumentSerializer#toSort(List)
	 */
	protected Document createSort(List<OrderSpecifier<?>> orderSpecifiers) {
		return serializer.toSort(orderSpecifiers);
	}
}
