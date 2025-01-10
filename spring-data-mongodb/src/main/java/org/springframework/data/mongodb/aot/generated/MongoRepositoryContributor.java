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

import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.bson.conversions.Bson;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition.Placeholder;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoQueryCreator;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodBuilder.MethodGenerationMetadata;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;
import org.springframework.javapoet.MethodSpec.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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
	protected void customizeDerivedMethod(AotRepositoryMethodBuilder methodBuilder) {

		methodBuilder.customize((repositoryInformation, metadata, body) -> {
			Query query = AnnotatedElementUtils.findMergedAnnotation(metadata.getRepositoryMethod(), Query.class);
			if (query != null) {
				userAnnotatedQuery(repositoryInformation, metadata, body, query);
			} else {


				;

				MongoMappingContext mongoMappingContext = new MongoMappingContext();
				mongoMappingContext.setSimpleTypeHolder(MongoCustomConversions.create((cfg) ->
					cfg.useNativeDriverJavaTimeCodecs()).getSimpleTypeHolder());
				mongoMappingContext.setAutoIndexCreation(false);
				mongoMappingContext.afterPropertiesSet();

				PartTree partTree = new PartTree(metadata.getRepositoryMethod().getName(),
						repositoryInformation.getDomainType());
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

								if(metadata.getRepositoryMethod().getParameterCount() == 0) {
									return new Object[]{};
								}
								return IntStream.range(0, metadata.getRepositoryMethod().getParameterCount()).mapToObj(it -> new Placeholder("?"+it)).toArray();
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

				writeStringQuery(repositoryInformation, metadata, body, queryCreator.createQuery().toJson());
			}
		});
	}

	private static void writeStringQuery(RepositoryInformation repositoryInformation, MethodGenerationMetadata metadata,
		Builder body, String query) {

		String mongoOpsRef = metadata.fieldNameOf(MongoOperations.class);
		String arguments = StringUtils.collectionToCommaDelimitedString(Arrays
			.stream(metadata.getRepositoryMethod().getParameters()).map(Parameter::getName).collect(Collectors.toList()));

		body.beginControlFlow("if($L.isDebugEnabled())", metadata.fieldNameOf(Log.class));
		body.addStatement("$L.debug(\"invoking generated [$L] method\")", metadata.fieldNameOf(Log.class),
			metadata.getRepositoryMethod().getName());
		body.endControlFlow();

		body.addStatement("$T filter = new $T($S, $L.getConverter(), new $T[]{ $L })", BindableMongoExpression.class,
			BindableMongoExpression.class, query, mongoOpsRef, Object.class, arguments);
		body.addStatement("$T query = new $T(filter.toDocument())", org.springframework.data.mongodb.core.query.Query.class,
			BasicQuery.class);

		boolean isCollectionType = TypeInformation.fromReturnTypeOf(metadata.getRepositoryMethod()).isCollectionLike();
		String terminatingMethod = isCollectionType ? "all()" : "oneValue()";

		if (metadata.getActualReturnType() != null && ObjectUtils
			.nullSafeEquals(TypeName.get(repositoryInformation.getDomainType()), metadata.getActualReturnType())) {
			body.addStatement("""
					return $L.query($T.class)
						.matching(query)
						.$L""", mongoOpsRef, repositoryInformation.getDomainType(), terminatingMethod);
		} else {

			body.addStatement("""
					return $L.query($T.class)
						.as($T.class)
						.matching(query)
						.$L""", mongoOpsRef, repositoryInformation.getDomainType(),
				metadata.getActualReturnType() != null ? metadata.getActualReturnType()
					: repositoryInformation.getDomainType(), terminatingMethod);
		}
	}

	private static void userAnnotatedQuery(RepositoryInformation repositoryInformation, MethodGenerationMetadata metadata,
			Builder body, Query query) {
		writeStringQuery(repositoryInformation, metadata, body, query.value());
	}
}
