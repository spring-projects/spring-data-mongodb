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

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.NullUnmarked;
import org.springframework.core.ResolvableType;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.ExecutableUpdate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;

/**
 * @author Christoph Strobl
 */
class UpdateBlocks {

	@NullUnmarked
	static class UpdateExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;
		private String updateVariableName;

		UpdateExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		UpdateExecutionCodeBlockBuilder withFilter(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		UpdateExecutionCodeBlockBuilder referencingUpdate(String updateVariableName) {

			this.updateVariableName = updateVariableName;
			return this;
		}

		CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
			Builder builder = CodeBlock.builder();

			builder.add("\n");

			String updateReference = updateVariableName;
			Class<?> domainType = context.getRepositoryInformation().getDomainType();
			VariableSnippet updater = Snippet.declare(builder)
					.variable(ResolvableType.forClassWithGenerics(ExecutableUpdate.class, domainType),
							context.localVariable("updater"))
					.as("$L.update($T.class)", mongoOpsRef, domainType);

			Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());
			if (ReflectionUtils.isVoid(returnType)) {
				builder.addStatement("$L.matching($L).apply($L).all()", updater.getVariableName(), queryVariableName,
						updateReference);
			} else if (ClassUtils.isAssignable(Long.class, returnType)) {
				builder.addStatement("return $L.matching($L).apply($L).all().getModifiedCount()", updater.getVariableName(),
						queryVariableName, updateReference);
			} else {

				VariableSnippet modifiedCount = Snippet.declare(builder)
						.variable(Long.class, context.localVariable("modifiedCount"))
						.as("$L.matching($L).apply($L).all().getModifiedCount()", updater.getVariableName(), queryVariableName,
								updateReference);
				builder.addStatement("return $T.convertNumberToTargetClass($L, $T.class)", NumberUtils.class,
						modifiedCount.getVariableName(), returnType);
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class UpdateCodeBlockBuilder {

		private UpdateInteraction source;
		private Map<String, CodeBlock> arguments;
		private String updateVariableName;

		public UpdateCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.arguments = new LinkedHashMap<>();
			context.getBindableParameterNames().forEach(it -> arguments.put(it, CodeBlock.of(it)));
		}

		public UpdateCodeBlockBuilder update(UpdateInteraction update) {
			this.source = update;
			return this;
		}

		public UpdateCodeBlockBuilder usingUpdateVariableName(String updateVariableName) {
			this.updateVariableName = updateVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();

			builder.add("\n");
			String tmpVariableName = updateVariableName + "Document";
			builder.add(
					MongoCodeBlocks.renderExpressionToDocument(source.getUpdate().getUpdateString(), tmpVariableName, arguments));
			builder.addStatement("$1T $2L = new $1T($3L)", BasicUpdate.class, updateVariableName, tmpVariableName);

			return builder.build();
		}
	}
}
