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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class AggregationBlocks {

    @NullUnmarked
    static class AggregationExecutionCodeBlockBuilder {

        private final AotQueryMethodGenerationContext context;
        private final MongoQueryMethod queryMethod;
        private String aggregationVariableName;

        AggregationExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

            this.context = context;
            this.queryMethod = queryMethod;
        }

        AggregationExecutionCodeBlockBuilder referencing(String aggregationVariableName) {

            this.aggregationVariableName = aggregationVariableName;
            return this;
        }

        CodeBlock build() {

            String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
            Builder builder = CodeBlock.builder();

            builder.add("\n");

            Class<?> outputType = queryMethod.getReturnedObjectType();
            if (MongoSimpleTypes.HOLDER.isSimpleType(outputType)) {
                outputType = Document.class;
            } else if (ClassUtils.isAssignable(AggregationResults.class, outputType)) {
                outputType = queryMethod.getReturnType().getComponentType().getType();
            }

            if (ReflectionUtils.isVoid(queryMethod.getReturnedObjectType())) {
                builder.addStatement("$L.aggregate($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);
                return builder.build();
            }

            if (ClassUtils.isAssignable(AggregationResults.class, context.getMethod().getReturnType())) {
                builder.addStatement("return $L.aggregate($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);
                return builder.build();
            }

            if (outputType == Document.class) {

                Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());

                if (queryMethod.isStreamQuery()) {

                    builder.addStatement("$T<$T> $L = $L.aggregateStream($L, $T.class)", Stream.class, Document.class,
                            context.localVariable("results"), mongoOpsRef, aggregationVariableName, outputType);

                    builder.addStatement("return $1L.map(it -> ($2T) convertSimpleRawResult($2T.class, it))",
                            context.localVariable("results"), returnType);
                } else {

                    builder.addStatement("$T $L = $L.aggregate($L, $T.class)", AggregationResults.class,
                            context.localVariable("results"), mongoOpsRef, aggregationVariableName, outputType);

                    if (!queryMethod.isCollectionQuery()) {
                        builder.addStatement(
                                "return $1T.<$2T>firstElement(convertSimpleRawResults($2T.class, $3L.getMappedResults()))",
                                CollectionUtils.class, returnType, context.localVariable("results"));
                    } else {
                        builder.addStatement("return convertSimpleRawResults($T.class, $L.getMappedResults())", returnType,
                                context.localVariable("results"));
                    }
                }
            } else {
                if (queryMethod.isSliceQuery()) {
                    builder.addStatement("$T $L = $L.aggregate($L, $T.class)", AggregationResults.class,
                            context.localVariable("results"), mongoOpsRef, aggregationVariableName, outputType);
                    builder.addStatement("boolean $L = $L.getMappedResults().size() > $L.getPageSize()",
                            context.localVariable("hasNext"), context.localVariable("results"), context.getPageableParameterName());
                    builder.addStatement(
                            "return new $1T<>($2L ? $3L.getMappedResults().subList(0, $4L.getPageSize()) : $3L.getMappedResults(), $4L, $2L)",
                            SliceImpl.class, context.localVariable("hasNext"), context.localVariable("results"),
                            context.getPageableParameterName());
                } else {

                    if (queryMethod.isStreamQuery()) {
                        builder.addStatement("return $L.aggregateStream($L, $T.class)", mongoOpsRef, aggregationVariableName,
                                outputType);
                    } else {

                        builder.addStatement("return $L.aggregate($L, $T.class).getMappedResults()", mongoOpsRef,
                                aggregationVariableName, outputType);
                    }
                }
            }

            return builder.build();
        }
    }

    @NullUnmarked
    static class AggregationCodeBlockBuilder {

        private final AotQueryMethodGenerationContext context;
        private final MongoQueryMethod queryMethod;
        private final List<CodeBlock> arguments;

        private AggregationInteraction source;

        private String aggregationVariableName;
        private boolean pipelineOnly;

        AggregationCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

            this.context = context;
            this.arguments = context.getBindableParameterNames().stream().map(CodeBlock::of).collect(Collectors.toList());
            this.queryMethod = queryMethod;
        }

        AggregationCodeBlockBuilder stages(AggregationInteraction aggregation) {

            this.source = aggregation;
            return this;
        }

        AggregationCodeBlockBuilder usingAggregationVariableName(String aggregationVariableName) {

            this.aggregationVariableName = aggregationVariableName;
            return this;
        }

        AggregationCodeBlockBuilder pipelineOnly(boolean pipelineOnly) {

            this.pipelineOnly = pipelineOnly;
            return this;
        }

        CodeBlock build() {

            Builder builder = CodeBlock.builder();
            builder.add("\n");

            String pipelineName = context.localVariable(aggregationVariableName + (pipelineOnly ? "" : "Pipeline"));
            builder.add(pipeline(pipelineName));

            if (!pipelineOnly) {

                builder.addStatement("$1T<$2T> $3L = $4T.newAggregation($2T.class, $5L.getOperations())",
                        TypedAggregation.class, context.getRepositoryInformation().getDomainType(), aggregationVariableName,
                        Aggregation.class, pipelineName);

                builder.add(aggregationOptions(aggregationVariableName));
            }

            return builder.build();
        }

        private CodeBlock pipeline(String pipelineVariableName) {

            String sortParameter = context.getSortParameterName();
            String limitParameter = context.getLimitParameterName();
            String pageableParameter = context.getPageableParameterName();

            boolean mightBeSorted = StringUtils.hasText(sortParameter);
            boolean mightBeLimited = StringUtils.hasText(limitParameter);
            boolean mightBePaged = StringUtils.hasText(pageableParameter);

            int stageCount = source.stages().size();
            if (mightBeSorted) {
                stageCount++;
            }
            if (mightBeLimited) {
                stageCount++;
            }
            if (mightBePaged) {
                stageCount += 3;
            }

            Builder builder = CodeBlock.builder();
            builder.add(aggregationStages(context.localVariable("stages"), source.stages(), stageCount, arguments));

            if (mightBeSorted) {
                builder.add(sortingStage(sortParameter));
            }

            if (mightBeLimited) {
                builder.add(limitingStage(limitParameter));
            }

            if (mightBePaged) {
                builder.add(pagingStage(pageableParameter, queryMethod.isSliceQuery()));
            }

            builder.addStatement("$T $L = createPipeline($L)", AggregationPipeline.class, pipelineVariableName,
                    context.localVariable("stages"));
            return builder.build();
        }

        private CodeBlock aggregationOptions(String aggregationVariableName) {

            Builder builder = CodeBlock.builder();
            List<CodeBlock> options = new ArrayList<>(5);
            if (ReflectionUtils.isVoid(queryMethod.getReturnedObjectType())) {
                options.add(CodeBlock.of(".skipOutput()"));
            }

            MergedAnnotation<Hint> hintAnnotation = context.getAnnotation(Hint.class);
            String hint = hintAnnotation.isPresent() ? hintAnnotation.getString("value") : null;
            if (StringUtils.hasText(hint)) {
                options.add(CodeBlock.of(".hint($S)", hint));
            }

            MergedAnnotation<ReadPreference> readPreferenceAnnotation = context.getAnnotation(ReadPreference.class);
            String readPreference = readPreferenceAnnotation.isPresent() ? readPreferenceAnnotation.getString("value") : null;
            if (StringUtils.hasText(readPreference)) {
                options.add(CodeBlock.of(".readPreference($T.valueOf($S))", com.mongodb.ReadPreference.class, readPreference));
            }

            if (queryMethod.hasAnnotatedCollation()) {
                options.add(CodeBlock.of(".collation($T.parse($S))", Collation.class, queryMethod.getAnnotatedCollation()));
            }

            if (!options.isEmpty()) {

                Builder optionsBuilder = CodeBlock.builder();
                optionsBuilder.add("$1T $2L = $1T.builder()\n", AggregationOptions.class,
                        context.localVariable("aggregationOptions"));
                optionsBuilder.indent();
                for (CodeBlock optionBlock : options) {
                    optionsBuilder.add(optionBlock);
                    optionsBuilder.add("\n");
                }
                optionsBuilder.add(".build();\n");
                optionsBuilder.unindent();
                builder.add(optionsBuilder.build());

                builder.addStatement("$1L = $1L.withOptions($2L)", aggregationVariableName,
                        context.localVariable("aggregationOptions"));
            }
            return builder.build();
        }

        private CodeBlock aggregationStages(String stageListVariableName, Iterable<String> stages, int stageCount,
                List<CodeBlock> arguments) {

            Builder builder = CodeBlock.builder();
            builder.addStatement("$T<$T> $L = new $T($L)", List.class, Object.class, stageListVariableName, ArrayList.class,
                    stageCount);
            int stageCounter = 0;

            for (String stage : stages) {
                String stageName = context.localVariable("stage_%s".formatted(stageCounter++));
                builder.add(MongoCodeBlocks.renderExpressionToDocument(stage, stageName, arguments));
                builder.addStatement("$L.add($L)", context.localVariable("stages"), stageName);
            }

            return builder.build();
        }

        private CodeBlock sortingStage(String sortProvider) {

            Builder builder = CodeBlock.builder();

            builder.beginControlFlow("if ($L.isSorted())", sortProvider);
            builder.addStatement("$1T $2L = new $1T()", Document.class, context.localVariable("sortDocument"));
            builder.beginControlFlow("for ($T $L : $L)", Order.class, context.localVariable("order"), sortProvider);
            builder.addStatement("$1L.append($2L.getProperty(), $2L.isAscending() ? 1 : -1);",
                    context.localVariable("sortDocument"), context.localVariable("order"));
            builder.endControlFlow();
            builder.addStatement("stages.add(new $T($S, $L))", Document.class, "$sort",
                    context.localVariable("sortDocument"));
            builder.endControlFlow();

            return builder.build();
        }

        private CodeBlock pagingStage(String pageableProvider, boolean slice) {

            Builder builder = CodeBlock.builder();

            builder.add(sortingStage(pageableProvider + ".getSort()"));

            builder.beginControlFlow("if ($L.isPaged())", pageableProvider);
            builder.beginControlFlow("if ($L.getOffset() > 0)", pageableProvider);
            builder.addStatement("$L.add($T.skip($L.getOffset()))", context.localVariable("stages"), Aggregation.class,
                    pageableProvider);
            builder.endControlFlow();
            if (slice) {
                builder.addStatement("$L.add($T.limit($L.getPageSize() + 1))", context.localVariable("stages"),
                        Aggregation.class, pageableProvider);
            } else {
                builder.addStatement("$L.add($T.limit($L.getPageSize()))", context.localVariable("stages"), Aggregation.class,
                        pageableProvider);
            }
            builder.endControlFlow();

            return builder.build();
        }

        private CodeBlock limitingStage(String limitProvider) {

            Builder builder = CodeBlock.builder();

            builder.beginControlFlow("if ($L.isLimited())", limitProvider);
            builder.addStatement("$L.add($T.limit($L.max()))", context.localVariable("stages"), Aggregation.class,
                    limitProvider);
            builder.endControlFlow();

            return builder.build();
        }

    }
}
