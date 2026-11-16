/**
 * Parsing and binding of MongoDB field paths against the mapping model.
 * <ul>
 * <li>{@link org.springframework.data.mongodb.core.mapping.path.MongoPath} is parsed from a string for a
 * {@link org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType}.</li>
 * <li>{@link org.springframework.data.mongodb.core.mapping.path.MongoPathBinder} binds it against a
 * {@link org.springframework.data.mongodb.core.mapping.path.Scope}, resulting in a
 * {@link org.springframework.data.mongodb.core.mapping.path.BoundMongoPath}.</li>
 * <li>{@link org.springframework.data.mongodb.core.mapping.path.ExpressionPathMapper} maps the paths within raw
 * aggregation expressions.</li>
 * </ul>
 */
@org.jspecify.annotations.NullMarked
package org.springframework.data.mongodb.core.mapping.path;
