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
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodBuilder;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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

				String mongoOpsRef = metadata.fieldNameOf(MongoOperations.class);
				String arguments = StringUtils
						.collectionToCommaDelimitedString(Arrays.stream(metadata.getRepositoryMethod().getParameters())
								.map(Parameter::getName).collect(Collectors.toList()));

				body.beginControlFlow("if($L.isDebugEnabled())", metadata.fieldNameOf(Log.class));
				body.addStatement("$L.debug(\"invoking generated [$L] method\")", metadata.fieldNameOf(Log.class),
						metadata.getRepositoryMethod().getName());
				body.endControlFlow();

				body.addStatement("$T filter = new $T($S, $L.getConverter(), new $T[]{ $L })", BindableMongoExpression.class,
						BindableMongoExpression.class, query.value(), mongoOpsRef, Object.class, arguments);
				body.addStatement("$T query = new $T(filter.toDocument())",
						org.springframework.data.mongodb.core.query.Query.class, BasicQuery.class);

				if (metadata.getActualReturnType() != null && ObjectUtils
						.nullSafeEquals(TypeName.get(repositoryInformation.getDomainType()), metadata.getActualReturnType())) {
					body.addStatement("""
							return $L.query($T.class)
							    .matching(query)
							    .all()""", mongoOpsRef, repositoryInformation.getDomainType());
				} else {

					body.addStatement("""
							return $L.query($T.class)
							    .as($T.class)
							    .matching(query)
							    .all()""", mongoOpsRef, repositoryInformation.getDomainType(),
							metadata.getActualReturnType() != null ? metadata.getActualReturnType()
									: repositoryInformation.getDomainType());
				}
			}
		});
	}
}
