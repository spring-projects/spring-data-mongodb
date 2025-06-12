/*
 * Copyright 2025. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.aot;

import java.util.List;
import java.util.stream.Collectors;

import org.jspecify.annotations.NullUnmarked;
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
 * @since 2025/06
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
            builder.addStatement("$1T<$2T> $3L = $4L.update($2T.class)", ExecutableUpdate.class, domainType,
                    context.localVariable("updater"), mongoOpsRef);

            Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());
            if (ReflectionUtils.isVoid(returnType)) {
                builder.addStatement("$L.matching($L).apply($L).all()", context.localVariable("updater"), queryVariableName,
                        updateReference);
            } else if (ClassUtils.isAssignable(Long.class, returnType)) {
                builder.addStatement("return $L.matching($L).apply($L).all().getModifiedCount()",
                        context.localVariable("updater"), queryVariableName, updateReference);
            } else {
                builder.addStatement("$T $L = $L.matching($L).apply($L).all().getModifiedCount()", Long.class,
                        context.localVariable("modifiedCount"), context.localVariable("updater"), queryVariableName,
                        updateReference);
                builder.addStatement("return $T.convertNumberToTargetClass($L, $T.class)", NumberUtils.class,
                        context.localVariable("modifiedCount"), returnType);
            }

            return builder.build();
        }
    }

    @NullUnmarked
    static class UpdateCodeBlockBuilder {

        private UpdateInteraction source;
        private List<CodeBlock> arguments;
        private String updateVariableName;

        public UpdateCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
            this.arguments = context.getBindableParameterNames().stream().map(CodeBlock::of).collect(Collectors.toList());
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
            builder.add(MongoCodeBlocks.renderExpressionToDocument(source.getUpdate().getUpdateString(), tmpVariableName, arguments));
            builder.addStatement("$1T $2L = new $1T($3L)", BasicUpdate.class, updateVariableName, tmpVariableName);

            return builder.build();
        }
    }
}
