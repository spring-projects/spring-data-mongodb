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
package org.springframework.data.mongodb.core.mapping;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPath.MappedSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.RawMongoPath.Segment;
import org.springframework.data.mongodb.core.mapping.MongoPath.RawMongoPath.TargetType;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ConcurrentLruCache;

/**
 * @author Christoph Strobl
 * @since 2025/09
 */
public class MongoPaths {

	private final ConcurrentLruCache<PathAndType, MongoPath.MappedMongoPath> CACHE = new ConcurrentLruCache<>(128, this::mapFieldNames);
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	public MongoPaths(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	public MongoPath create(String path) {
		return MongoPath.RawMongoPath.parse(path);
	}

	public MongoPath mappedPath(MongoPath path, TypeInformation<?> type) {

		if (!(path instanceof MongoPath.RawMongoPath rawMongoPath)) {
			return path;
		}

		if (!mappingContext.hasPersistentEntityFor(type.getType())) {
			return path;
		}
		return CACHE.get(new PathAndType(rawMongoPath, type));
	}

	record PathAndType(MongoPath.RawMongoPath path, TypeInformation<?> type) {
	}

	MongoPath.MappedMongoPath mapFieldNames(PathAndType cacheKey) {

		MongoPath.RawMongoPath mongoPath = cacheKey.path();
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(cacheKey.type());

		List<MappedSegment> segments = new ArrayList<>(mongoPath.getSegments().size());

		for (Segment segment : mongoPath.getSegments()) {

			if (persistentEntity != null && !segment.keyword()
					&& (segment.targetType() == TargetType.ANY || segment.targetType() == TargetType.PROPERTY)) {

				MongoPersistentProperty persistentProperty = persistentEntity.getPersistentProperty(segment.toString());

				String name = segment.segment();

				if (persistentProperty != null) {

					if (persistentProperty.isEntity()) {
						persistentEntity = mappingContext.getPersistentEntity(persistentProperty);
					}

					if (persistentProperty.isUnwrapped()) {
						continue;
					}

					name = persistentProperty.getFieldName();
				}

				segments.add(new MappedSegment(segment, name));
			} else {
				segments.add(new MappedSegment(segment, segment.segment()));
			}
		}

		return new MongoPath.MappedMongoPath(mongoPath, segments);
	}
}
