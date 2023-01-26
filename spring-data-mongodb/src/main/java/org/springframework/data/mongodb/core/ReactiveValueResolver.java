/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.ReactiveDbRefResolver;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
class ReactiveValueResolver {

	static Mono<Document> prepareDbRefResolution(Mono<Document> root, ReactiveDbRefResolver dbRefResolver) {
		return root.flatMap(source -> {
			for (Entry<String, Object> entry : source.entrySet()) {
				Object value = entry.getValue();
				if (value instanceof DBRef dbRef) {
					return prepareDbRefResolution(dbRefResolver.initFetch(dbRef).defaultIfEmpty(new Document())
							.flatMap(it -> prepareDbRefResolution(Mono.just(it), dbRefResolver)).map(resolved -> {
								source.put(entry.getKey(), resolved.isEmpty() ? null : resolved);
								return source;
							}), dbRefResolver);
				}
				if (value instanceof Document nested) {
					return prepareDbRefResolution(Mono.just(nested), dbRefResolver).map(it -> {
						source.put(entry.getKey(), it);
						return source;
					});
				}
				if (value instanceof List<?> list) {
					return Flux.fromIterable(list).concatMap(it -> {
						if (it instanceof DBRef dbRef) {
							return prepareDbRefResolution(dbRefResolver.initFetch(dbRef), dbRefResolver);
						}
						if (it instanceof Document document) {
							return prepareDbRefResolution(Mono.just(document), dbRefResolver);
						}
						return Mono.just(it);
					}).collectList().map(resolved -> {
						source.put(entry.getKey(), resolved.isEmpty() ? null : resolved);
						return source;
					});
				}
			}
			return Mono.just(source);
		});
	}

	public Mono<Document> resolveValues(Mono<Document> document) {

		return document.flatMap(source -> {
			for (Entry<String, Object> entry : source.entrySet()) {
				Object val = entry.getValue();
				if (val instanceof Mono<?> valueMono) {
					return valueMono.flatMap(value -> {
						source.put(entry.getKey(), value);
						return resolveValues(Mono.just(source));
					});
				}
				if (entry.getValue()instanceof Document nested) {
					return resolveValues(Mono.just(nested)).map(it -> {
						source.put(entry.getKey(), it);
						return source;
					});
				}
				if (entry.getValue() instanceof List<?>) {
					// do traverse list
				}
			}
			return Mono.just(source);
		});
	}
}
