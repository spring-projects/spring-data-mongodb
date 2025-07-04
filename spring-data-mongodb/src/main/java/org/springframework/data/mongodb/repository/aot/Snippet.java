/*
 * Copyright 2025-present the original author or authors.
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

import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.springframework.core.ResolvableType;
import org.springframework.data.mongodb.repository.aot.Snippet.BuilderStyleBuilder.BuilderStyleMethodArgumentBuilder;
import org.springframework.data.mongodb.repository.aot.Snippet.BuilderStyleVariableBuilder.BuilderStyleVariableBuilderImpl;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
interface Snippet {

	CodeBlock code();

	default boolean isEmpty() {
		return code().isEmpty();
	}

	default void appendTo(CodeBlock.Builder builder) {
		if (!isEmpty()) {
			builder.add(code());
		}
	}

	default <T> T as(Function<? super Snippet, T> transformer) {
		return transformer.apply(this);
	}

	default Snippet wrap(String prefix, String suffix) {
		return wrap("%s$L%s".formatted(prefix, suffix));
	}

	default Snippet wrap(CodeBlock prefix, CodeBlock suffix) {
		return new Snippet() {

			@Override
			public CodeBlock code() {
				return CodeBlock.builder().add(prefix).add(Snippet.this.code()).add(suffix).build();
			}
		};
	}

	default Snippet wrap(String statement) {
		return new Snippet() {

			@Override
			public CodeBlock code() {
				return CodeBlock.of(statement, Snippet.this.code());
			}
		};
	}

	static Snippet just(CodeBlock codeBlock) {
		return new Snippet() {
			@Override
			public CodeBlock code() {
				return codeBlock;
			}
		};
	}

	static ContextualSnippetBuilder declare(CodeBlock.Builder builder) {

		return new ContextualSnippetBuilder() {

			@Override
			public VariableBuilder variable(String variableName) {
				return VariableSnippet.variable(variableName).targeting(builder);
			}

			@Override
			public VariableBuilder variable(Class<?> type, String variableName) {
				return VariableSnippet.variable(type, variableName).targeting(builder);
			}

			@Override
			public VariableBuilder variable(ResolvableType resolvableType, String variableName) {
				return VariableSnippet.variable(resolvableType, variableName).targeting(builder);
			}

			@Override
			public BuilderStyleVariableBuilder variableBuilder(String variableName) {
				return new BuilderStyleVariableBuilderImpl(builder, null, variableName);
			}

			@Override
			public BuilderStyleVariableBuilder variableBuilder(Class<?> type, String variableName) {
				return variableBuilder(ResolvableType.forClass(type), variableName);
			}

			@Override
			public BuilderStyleVariableBuilder variableBuilder(ResolvableType resolvableType, String variableName) {
				return new BuilderStyleVariableBuilderImpl(builder, resolvableType, variableName);
			}
		};
	}

	interface ContextualSnippetBuilder {

		VariableBuilder variable(String variableName);

		VariableBuilder variable(Class<?> type, String variableName);

		VariableBuilder variable(ResolvableType resolvableType, String variableName);

		BuilderStyleVariableBuilder variableBuilder(String variableName);

		BuilderStyleVariableBuilder variableBuilder(Class<?> type, String variableName);

		BuilderStyleVariableBuilder variableBuilder(ResolvableType resolvableType, String variableName);
	}

	interface VariableBuilder {

		default VariableSnippet as(String declaration, Object... args) {
			return of(CodeBlock.of(declaration, args));
		}

		VariableSnippet of(CodeBlock codeBlock);
	}

	interface BuilderStyleVariableBuilder {

		default BuilderStyleBuilder as(String declaration, Object... args) {
			return of(CodeBlock.of(declaration, args));
		}

		BuilderStyleBuilder of(CodeBlock codeBlock);

		class BuilderStyleVariableBuilderImpl
				implements BuilderStyleVariableBuilder, BuilderStyleBuilder, BuilderStyleMethodArgumentBuilder {

			Builder targetBuilder;
			@Nullable ResolvableType type;
			String targetVariableName;
			@Nullable String targetMethodName;
			@Nullable VariableSnippet variableSnippet;

			public BuilderStyleVariableBuilderImpl(Builder targetBuilder, @Nullable ResolvableType type,
					String targetVariableName) {
				this.targetBuilder = targetBuilder;
				this.type = type;
				this.targetVariableName = targetVariableName;
			}

			@Override
			public BuilderStyleBuilder as(String declaration, Object... args) {

				if (type != null) {
					this.variableSnippet = Snippet.declare(targetBuilder).variable(type, targetVariableName).as(declaration, args);
				} else {
					this.variableSnippet = Snippet.declare(targetBuilder).variable(targetVariableName).as(declaration, args);
				}
				return this;
			}

			@Override
			public BuilderStyleBuilder of(CodeBlock codeBlock) {
				if (type != null) {
					this.variableSnippet = Snippet.declare(targetBuilder).variable(type, targetVariableName).of(codeBlock);
				} else {
					this.variableSnippet = Snippet.declare(targetBuilder).variable(targetVariableName).of(codeBlock);
				}
				return this;
			}

			@Override
			public BuilderStyleMethodArgumentBuilder call(String methodName) {
				this.targetMethodName = methodName;
				return this;
			}

			@Override
			public BuilderStyleBuilder with(Snippet snippet) {
				new BuilderStyleSnippet(targetVariableName, targetMethodName, snippet).appendTo(targetBuilder);
				return this;
			}

			@Override
			public VariableSnippet variable() {
				return this.variableSnippet;
			}
		}
	}

	interface BuilderStyleBuilder {

		BuilderStyleMethodArgumentBuilder call(String methodName);
		VariableSnippet variable();

		interface BuilderStyleMethodArgumentBuilder {
			default BuilderStyleBuilder with(String statement, Object... args) {
				return with(CodeBlock.of(statement, args));
			}

			default BuilderStyleBuilder with(CodeBlock codeBlock) {
				return with(Snippet.just(codeBlock));
			}

			BuilderStyleBuilder with(Snippet snippet);
		}
	}

}
