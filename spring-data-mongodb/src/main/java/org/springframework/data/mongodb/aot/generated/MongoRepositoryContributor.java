/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.aot.generated;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.bson.conversions.Bson;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition.Placeholder;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoQueryCreator;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;
import org.springframework.javapoet.MethodSpec.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.lang.Nullable;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class MongoRepositoryContributor extends RepositoryContributor {

	public MongoRepositoryContributor(AotRepositoryContext repositoryContext) {
		super(repositoryContext);
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {
		constructorBuilder.addParameter("operations", TypeName.get(MongoOperations.class));
	}

	@Override
	protected AotRepositoryMethodBuilder contributeRepositoryMethod(
			AotRepositoryMethodGenerationContext generationContext) {

		return new AotRepositoryMethodBuilder(generationContext).customize((context, body) -> {
			Query query = AnnotatedElementUtils.findMergedAnnotation(context.getMethod(), Query.class);
			if (query != null) {
				userAnnotatedQuery(context, body, query);
			} else {

				MongoMappingContext mongoMappingContext = new MongoMappingContext();
				mongoMappingContext.setSimpleTypeHolder(
						MongoCustomConversions.create((cfg) -> cfg.useNativeDriverJavaTimeCodecs()).getSimpleTypeHolder());
				mongoMappingContext.setAutoIndexCreation(false);
				mongoMappingContext.afterPropertiesSet();

				PartTree partTree = new PartTree(context.getMethod().getName(),
						context.getRepositoryInformation().getDomainType());
				MongoQueryCreator queryCreator = new MongoQueryCreator(partTree,
						new ConvertingParameterAccessor(new MongoWriter<Object>() {
							@Nullable
							@Override
							public Object convertToMongoType(@Nullable Object obj, @Nullable TypeInformation<?> typeInformation) {
								return "?0";
							}

							@Override
							public DBRef toDBRef(Object object, @Nullable MongoPersistentProperty referringProperty) {
								return null;
							}

							@Override
							public void write(Object source, Bson sink) {

							}
						}, new MongoParameterAccessor() {
							@Override
							public Range<Distance> getDistanceRange() {
								return null;
							}

							@Nullable
							@Override
							public Point getGeoNearLocation() {
								return null;
							}

							@Nullable
							@Override
							public TextCriteria getFullText() {
								return null;
							}

							@Nullable
							@Override
							public Collation getCollation() {
								return null;
							}

							@Override
							public Object[] getValues() {

								if (context.getMethod().getParameterCount() == 0) {
									return new Object[] {};
								}
								return IntStream.range(0, context.getMethod().getParameterCount())
										.mapToObj(it -> new Placeholder("?" + it)).toArray();
							}

							@Nullable
							@Override
							public UpdateDefinition getUpdate() {
								return null;
							}

							@Nullable
							@Override
							public ScrollPosition getScrollPosition() {
								return null;
							}

							@Override
							public Pageable getPageable() {
								return null;
							}

							@Override
							public Sort getSort() {
								return null;
							}

							@Nullable
							@Override
							public Class<?> findDynamicProjection() {
								return null;
							}

							@Nullable
							@Override
							public Object getBindableValue(int index) {
								return "?" + index;
							}

							@Override
							public boolean hasBindableNullValue() {
								return false;
							}

							@Override
							public Iterator<Object> iterator() {
								return Arrays.stream(getValues()).iterator();
							}
						}), mongoMappingContext);

				org.springframework.data.mongodb.core.query.Query partTreeQuery = queryCreator.createQuery();
				StringBuffer buffer = new StringBuffer();
				BsonUtils.writeJson(partTreeQuery.getQueryObject()).to(buffer);
				writeStringQuery(context, body, buffer.toString());
			}
		});
	}

	private static void writeStringQuery(AotRepositoryMethodGenerationContext context, Builder body, String query) {

		body.addCode(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
		body.addCode(MongoBlocks.queryBlockBuilder(context).filter(query).build("query"));
		body.addCode(MongoBlocks.queryExecutionBlockBuilder(context).build("query"));
	}

	private static void userAnnotatedQuery(AotRepositoryMethodGenerationContext context, Builder body, Query query) {
		writeStringQuery(context, body, query.value());
	}
}
