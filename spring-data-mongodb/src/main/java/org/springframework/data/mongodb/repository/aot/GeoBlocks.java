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
package org.springframework.data.mongodb.repository.aot;

import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.javapoet.CodeBlock;
import org.springframework.util.ClassUtils;

/**
 * Code blocks for generating code related to geo-near queries in MongoDB repositories.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class GeoBlocks {

	@NullUnmarked
	static class GeoNearCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private String variableName;

		GeoNearCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add("\n");

			VariableSnippet query = Snippet.declare(builder).variable(NearQuery.class, variableName).as("$T.near($L)",
					NearQuery.class, context.getParameterName(queryMethod.getParameters().getNearIndex()));

			if (queryMethod.getParameters().getRangeIndex() != -1) {

				String rangeParameter = context.getParameterName(queryMethod.getParameters().getRangeIndex());

				builder.beginControlFlow("if($L.getLowerBound().isBounded())", rangeParameter);
				VariableSnippet min = Snippet.declare(builder).variable(Distance.class, context.localVariable("min"))
						.as("$L.getLowerBound().getValue().get()", rangeParameter);
				builder.addStatement("$1L.minDistance($2L).in($2L.getMetric())", query.getVariableName(),
						min.getVariableName());
				builder.endControlFlow();

				builder.beginControlFlow("if($L.getUpperBound().isBounded())", rangeParameter);
				VariableSnippet max = Snippet.declare(builder).variable(Distance.class, context.localVariable("max"))
						.as("$L.getUpperBound().getValue().get()", rangeParameter);
				builder.addStatement("$1L.maxDistance($2L).in($2L.getMetric())", query.getVariableName(),
						max.getVariableName());
				builder.endControlFlow();
			} else {

				String distanceParameter = context.getParameterName(queryMethod.getParameters().getMaxDistanceIndex());
				builder.addStatement("$1L.maxDistance($2L).in($2L.getMetric())", query.code(), distanceParameter);
			}

			if (context.getPageableParameterName() != null) {
				builder.addStatement("$L.with($L)", query.code(), context.getPageableParameterName());
			}

			MongoCodeBlocks.appendReadPreference(context, builder, query.getVariableName());

			return builder.build();
		}

		public GeoNearCodeBlockBuilder usingQueryVariableName(String variableName) {
			this.variableName = variableName;
			return this;
		}
	}

	@NullUnmarked
	static class GeoNearExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private String queryVariableName;

		GeoNearExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context) {

			this.context = context;
		}

		GeoNearExecutionCodeBlockBuilder referencing(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add("\n");

			VariableSnippet queryExecutor = Snippet.declare(builder).variable(context.localVariable("nearFinder")).as(
					"$L.query($T.class).near($L)", context.fieldNameOf(MongoOperations.class),
					context.getRepositoryInformation().getDomainType(), queryVariableName);

			if (ClassUtils.isAssignable(GeoPage.class, context.getReturnType().getRawClass())) {

				VariableSnippet geoResult = Snippet.declare(builder).variable(context.localVariable("geoResult")).as("$L.all()",
						queryExecutor.getVariableName());

				builder.beginControlFlow("if($L.isUnpaged())", context.getPageableParameterName());
				builder.addStatement("return new $T<>($L)", GeoPage.class, geoResult.getVariableName());
				builder.endControlFlow();

				VariableSnippet resultPage = Snippet.declare(builder).variable(context.localVariable("resultPage")).as(
						"$T.getPage($L.getContent(), $L, () -> $L.count())", PageableExecutionUtils.class,
						geoResult.getVariableName(), context.getPageableParameterName(), queryExecutor.getVariableName());

				builder.addStatement("return new $T<>($L, $L, $L.getTotalElements())", GeoPage.class,
						geoResult.getVariableName(), context.getPageableParameterName(), resultPage.getVariableName());
			} else if (ClassUtils.isAssignable(GeoResults.class, context.getReturnType().getRawClass())) {
				builder.addStatement("return $L.all()", queryExecutor.getVariableName());
			} else {
				builder.addStatement("return $L.all().getContent()", queryExecutor.getVariableName());
			}
			return builder.build();
		}
	}
}
