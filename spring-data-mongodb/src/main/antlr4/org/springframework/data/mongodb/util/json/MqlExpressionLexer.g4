/*
 * Copyright 2026-present the original author or authors.
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
lexer grammar MqlExpressionLexer;

@header {
/**
 * Locates parameter placeholders ({@code ?0}, {@code '?0'}) within the source of a value expression.
 * <p>
 * Applied to the source of {@code #{…}} expressions within {@code EXPRESSION} tokens produced by {@code Mql.g4} or
 * {@code MqlStringLexer.g4}; {@code ${…}} is not inspected. A separate lexer because the main grammar is a combined
 * grammar and ANTLR allows lexical modes only in lexer grammars.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
}

/*
    Lexer rules
 */

QUOTED_PLACEHOLDER : '\'?' [0-9]+ '\'' ;

PLACEHOLDER : '?' [0-9]+ ;

// doubled quotes escape a quote within the literal.
STRING
    : '\'' ( '\'\'' | ~'\'' )* '\''
    | '"' ( '""' | ~'"' )* '"'
    ;

TEXT : ~['"?]+ ;

ANY : . ;
