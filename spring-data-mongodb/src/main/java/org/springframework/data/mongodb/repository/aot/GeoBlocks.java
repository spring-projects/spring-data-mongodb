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
 * @author Christoph Strobl
 * @since 5.0
 */
class GeoBlocks {

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

			String locationParameterName = context.getParameterName(queryMethod.getParameters().getNearIndex());

			builder.addStatement("$1T $2L = $1T.near($3L)", NearQuery.class, variableName, locationParameterName);

			if (queryMethod.getParameters().getRangeIndex() != -1) {

				String rangeParametername = context.getParameterName(queryMethod.getParameters().getRangeIndex());
				String minVarName = context.localVariable("min");
				String maxVarName = context.localVariable("max");

				builder.beginControlFlow("if($L.getLowerBound().isBounded())", rangeParametername);
				builder.addStatement("$1T $2L = $3L.getLowerBound().getValue().get()", Distance.class, minVarName,
						rangeParametername);
				builder.addStatement("$1L.minDistance($2L).in($2L.getMetric())", variableName, minVarName);
				builder.endControlFlow();

				builder.beginControlFlow("if($L.getUpperBound().isBounded())", rangeParametername);
				builder.addStatement("$1T $2L = $3L.getUpperBound().getValue().get()", Distance.class, maxVarName,
						rangeParametername);
				builder.addStatement("$1L.maxDistance($2L).in($2L.getMetric())", variableName, maxVarName);
				builder.endControlFlow();
			} else {

				String distanceParametername = context.getParameterName(queryMethod.getParameters().getMaxDistanceIndex());
				builder.addStatement("$1L.maxDistance($2L).in($2L.getMetric())", variableName, distanceParametername);
			}

			if (context.getPageableParameterName() != null) {
				builder.addStatement("$L.with($L)", variableName, context.getPageableParameterName());
			}

			MongoCodeBlocks.appendReadPreference(context, builder, variableName);

			return builder.build();
		}

		public GeoNearCodeBlockBuilder usingQueryVariableName(String variableName) {
			this.variableName = variableName;
			return this;
		}
	}

	static class GeoNearExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;

		GeoNearExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		GeoNearExecutionCodeBlockBuilder referencing(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add("\n");

			String executorVar = context.localVariable("nearFinder");
			builder.addStatement("var $L = $L.query($T.class).near($L)", executorVar,
					context.fieldNameOf(MongoOperations.class), context.getRepositoryInformation().getDomainType(),
					queryVariableName);

			if (ClassUtils.isAssignable(GeoPage.class, context.getReturnType().getRawClass())) {

				String geoResultVar = context.localVariable("geoResult");
				builder.addStatement("var $L = $L.all()", geoResultVar, executorVar);

				builder.beginControlFlow("if($L.isUnpaged())", context.getPageableParameterName());
				builder.addStatement("return new $T<>($L)", GeoPage.class, geoResultVar);
				builder.endControlFlow();

				String pageVar = context.localVariable("resultPage");
				builder.addStatement("var $L = $T.getPage($L.getContent(), $L, () -> $L.count())", pageVar,
						PageableExecutionUtils.class, geoResultVar, context.getPageableParameterName(), executorVar);
				builder.addStatement("return new $T<>($L, $L, $L.getTotalElements())", GeoPage.class, geoResultVar,
						context.getPageableParameterName(), pageVar);
			} else if (ClassUtils.isAssignable(GeoResults.class, context.getReturnType().getRawClass())) {
				builder.addStatement("return $L.all()", executorVar);
			} else {
				builder.addStatement("return $L.all().getContent()", executorVar);
			}
			return builder.build();
		}
	}
}
