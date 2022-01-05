/*
 * Copyright 2018-2021 the original author or authors.
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

import org.springframework.lang.Nullable;

import com.mongodb.MongoClientSettings;
import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.FactoryExpression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;
import com.querydsl.mongodb.document.AbstractMongodbQuery;
import com.querydsl.mongodb.document.MongodbDocumentSerializer;

/**
 * {@code QuerydslAbstractMongodbQuery} provides a base class for general Querydsl query implementation.
 * <p>
 * Original implementation source {@link com.querydsl.mongodb.AbstractMongodbQuery} by {@literal The Querydsl Team}
 * (<a href="http://www.querydsl.com/team">http://www.querydsl.com/team</a>) licensed under the Apache License, Version
 * 2.0.
 * </p>
 * Modified for usage with {@link MongodbDocumentSerializer}.
 *
 * @param <Q> concrete subtype
 * @author laimw
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 * @deprecated since 3.3, use Querydsl's {@link AbstractMongodbQuery} directly. This class is deprecated for removal
 *             with the next major release.
 */
@Deprecated
public abstract class QuerydslAbstractMongodbQuery<K, Q extends QuerydslAbstractMongodbQuery<K, Q>>
		extends AbstractMongodbQuery<Q>
		implements SimpleQuery<Q> {

	private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder().outputMode(JsonMode.SHELL)
			.build();

	private final MongodbDocumentSerializer serializer;
	private final QueryMixin<Q> queryMixin;

	/**
	 * Create a new MongodbQuery instance
	 *
	 * @param serializer serializer
	 */
	@SuppressWarnings("unchecked")
	QuerydslAbstractMongodbQuery(MongodbDocumentSerializer serializer) {

		super(serializer);

		this.queryMixin = new QueryMixin<>((Q) this, new DefaultQueryMetadata(), false);
		this.serializer = serializer;
	}

	@Override
	public Q distinct() {
		return queryMixin.distinct();
	}

	@Override
	public Q where(Predicate... e) {
		return queryMixin.where(e);
	}

	@Override
	public Q limit(long limit) {
		return queryMixin.limit(limit);
	}

	@Override
	public Q offset(long offset) {
		return queryMixin.offset(offset);
	}

	@Override
	public Q restrict(QueryModifiers modifiers) {
		return queryMixin.restrict(modifiers);
	}

	@Override
	public Q orderBy(OrderSpecifier<?>... o) {
		return queryMixin.orderBy(o);
	}

	@Override
	public <T> Q set(ParamExpression<T> param, T value) {
		return queryMixin.set(param, value);
	}

	/**
	 * Compute the actual projection {@link Document} from a given projectionExpression by serializing the contained
	 * {@link Expression expressions} individually.
	 *
	 * @param projectionExpression the computed projection {@link Document}.
	 * @return never {@literal null}. An empty {@link Document} by default.
	 * @see MongodbDocumentSerializer#handle(Expression)
	 */
	protected Document createProjection(@Nullable Expression<?> projectionExpression) {

		if (!(projectionExpression instanceof FactoryExpression)) {
			return new Document();
		}

		Document projection = new Document();
		((FactoryExpression<?>) projectionExpression).getArgs().stream() //
				.filter(Expression.class::isInstance) //
				.map(Expression.class::cast) //
				.map(serializer::handle) //
				.forEach(it -> projection.append(it.toString(), 1));

		return projection;
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

		Document projection = createProjection(queryMixin.getMetadata().getProjection());
		Document sort = createSort(queryMixin.getMetadata().getOrderBy());
		DocumentCodec codec = new DocumentCodec(MongoClientSettings.getDefaultCodecRegistry());

		StringBuilder sb = new StringBuilder("find(" + asDocument().toJson(JSON_WRITER_SETTINGS, codec));
		if (!projection.isEmpty()) {
			sb.append(", ").append(projection.toJson(JSON_WRITER_SETTINGS, codec));
		}
		sb.append(")");
		if (!sort.isEmpty()) {
			sb.append(".sort(").append(sort.toJson(JSON_WRITER_SETTINGS, codec)).append(")");
		}
		if (queryMixin.getMetadata().getModifiers().getOffset() != null) {
			sb.append(".skip(").append(queryMixin.getMetadata().getModifiers().getOffset()).append(")");
		}
		if (queryMixin.getMetadata().getModifiers().getLimit() != null) {
			sb.append(".limit(").append(queryMixin.getMetadata().getModifiers().getLimit()).append(")");
		}
		return sb.toString();
	}

	/**
	 * Obtain the {@literal Mongo Shell} json query representation.
	 *
	 * @return never {@literal null}.
	 * @since 2.2
	 */
	public String toJson() {
		return toJson(JSON_WRITER_SETTINGS);
	}

	/**
	 * Obtain the json query representation applying given {@link JsonWriterSettings settings}.
	 *
	 * @param settings must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.2
	 */
	public String toJson(JsonWriterSettings settings) {
		return asDocument().toJson(settings);
	}
}
