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

import java.util.Optional;

import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.ExecutableRemove;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class DeleteBlocks {

    @NullUnmarked
    static class DeleteExecutionCodeBlockBuilder {

        private final AotQueryMethodGenerationContext context;
        private final MongoQueryMethod queryMethod;
        private String queryVariableName;

        DeleteExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

            this.context = context;
            this.queryMethod = queryMethod;
        }

        DeleteExecutionCodeBlockBuilder referencing(String queryVariableName) {

            this.queryVariableName = queryVariableName;
            return this;
        }

        CodeBlock build() {

            String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
            Builder builder = CodeBlock.builder();

            Class<?> domainType = context.getRepositoryInformation().getDomainType();
            boolean isProjecting = context.getActualReturnType() != null
                    && !ObjectUtils.nullSafeEquals(TypeName.get(domainType), context.getActualReturnType());

            Object actualReturnType = isProjecting ? context.getActualReturnType().getType() : domainType;

            builder.add("\n");
            builder.addStatement("$1T<$2T> $3L = $4L.remove($2T.class)", ExecutableRemove.class, domainType,
                    context.localVariable("remover"), mongoOpsRef);

            DeleteExecution.Type type = DeleteExecution.Type.FIND_AND_REMOVE_ALL;
            if (!queryMethod.isCollectionQuery()) {
                if (!ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())) {
                    type = DeleteExecution.Type.FIND_AND_REMOVE_ONE;
                } else {
                    type = DeleteExecution.Type.ALL;
                }
            }

            actualReturnType = ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())
                    ? TypeName.get(context.getMethod().getReturnType())
                    : queryMethod.isCollectionQuery() ? context.getReturnTypeName() : actualReturnType;

            if (ClassUtils.isVoidType(context.getMethod().getReturnType())) {
                builder.addStatement("new $T($L, $T.$L).execute($L)", DeleteExecution.class, context.localVariable("remover"),
                        DeleteExecution.Type.class, type.name(), queryVariableName);
            } else if (context.getMethod().getReturnType() == Optional.class) {
                builder.addStatement("return $T.ofNullable(($T) new $T($L, $T.$L).execute($L))", Optional.class,
                        actualReturnType, DeleteExecution.class, context.localVariable("remover"), DeleteExecution.Type.class,
                        type.name(), queryVariableName);
            } else {
                builder.addStatement("return ($T) new $T($L, $T.$L).execute($L)", actualReturnType, DeleteExecution.class,
                        context.localVariable("remover"), DeleteExecution.Type.class, type.name(), queryVariableName);
            }

            return builder.build();
        }
    }
}
