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
grammar MqlTemplate;

@header {
/**
 * Structure of JSON query templates: relaxed JSON, Extended JSON, shell constructors, parameter placeholders and value
 * expressions.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
}

// content following the first value is ignored for compatibility with the JsonScanner based reader.
template
    : value .*? EOF
    ;

value
    : object                    # objectValue
    | array                     # arrayValue
    | STRING                    # stringValue
    | REGEX                     # regexValue
    | NUMBER                    # numberValue
    | PLACEHOLDER               # placeholderValue
    | EXPRESSION                # expressionValue
    | NEW NAME arguments?       # newConstructorValue
    | NAME arguments            # constructorValue
    | NAME                      # nameValue
    ;

object
    : LBRACE (entry COMMA?)* RBRACE
    ;

entry
    : key COLON value
    ;

key
    : STRING
    | NAME
    | NEW
    | PLACEHOLDER
    | EXPRESSION
    ;

array
    : LBRACKET (value COMMA?)* RBRACKET
    ;

arguments
    : LPAREN (value (COMMA value)*)? RPAREN
    ;

LBRACE : '{' ;
RBRACE : '}' ;
LBRACKET : '[' ;
RBRACKET : ']' ;
LPAREN : '(' ;
RPAREN : ')' ;
COLON : ':' ;
COMMA : ',' ;

NEW : 'new' ;

// ?#{…} and :#{…} with nested braces and quoted expression strings, ?${…} and :${…} with nested braces.
EXPRESSION
    : [?:] '#' '{' EXPRESSION_PART* '}'
    | [?:] '$' '{' PROPERTY_PART* '}'
    ;

// ?0. Trailing name characters are retained for compatibility and ignored when binding.
PLACEHOLDER : '?' [$_\p{L}\p{Nd}]* ;

STRING
    : '"' ( '\\' . | ~["\\] )* '"'
    | '\'' ( '\\' . | ~['\\] )* '\''
    ;

REGEX : REGEX_PATTERN [imsx]* ;
INVALID_REGEX : REGEX_PATTERN [imsx]* [$_\p{L}\p{Nd}]+ ;

NUMBER
    : '-'? [0-9]+ ( '.' [0-9]+ )? ( [eE] [+-]? [0-9]+ )?
    | '-Infinity'
    ;
INVALID_NUMBER : '-'? [0-9] [$_\p{L}\p{Nd}.+-]* ;

NAME : [$_\p{L}] [$_\p{L}\p{Nd}]* ;

WS : [\p{White_Space}]+ -> skip ;

ANY : . ;

fragment REGEX_PATTERN
    : '/' ( '\\' . | [?:] '#' '{' EXPRESSION_PART* '}' | [?:] '$' '{' PROPERTY_PART* '}' | ~[/\\] )* '/'
    ;

fragment EXPRESSION_PART
    : '\'' ~'\''* '\''
    | '"' ~'"'* '"'
    | '{' EXPRESSION_PART* '}'
    | ~['"{}]
    ;

fragment PROPERTY_PART
    : '{' PROPERTY_PART* '}'
    | ~[{}]
    ;
