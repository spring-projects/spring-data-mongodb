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
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPathImpl.AssociationSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPathImpl.MappedPropertySegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPathImpl.WrappedSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment.PropertySegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.RawMongoPath;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ConcurrentLruCache;

/**
 * @author Christoph Strobl
 * @since 2025/09
 */
public class MongoPaths {

	private final ConcurrentLruCache<PathAndType, MongoPath.MappedMongoPath> CACHE = new ConcurrentLruCache<>(128,
			this::mapFieldNames);
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	public MongoPaths(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	public MongoPath create(String path) {
		return MongoPath.RawMongoPath.parse(path);
	}

	public MappedMongoPath mappedPath(MongoPath path, Class<?> type) {
		return mappedPath(path, TypeInformation.of(type));
	}

	public MappedMongoPath mappedPath(MongoPath path, TypeInformation<?> type) {

		if (path instanceof MappedMongoPath mappedPath) {
			return mappedPath;
		}

		MongoPath.RawMongoPath rawMongoPath = (RawMongoPath) path;

		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(type);
		if (persistentEntity == null) {
			return MappedMongoPath.just(rawMongoPath);
		}

		return CACHE.get(new PathAndType(rawMongoPath, type));
	}

	record PathAndType(MongoPath.RawMongoPath path, TypeInformation<?> type) {
	}

	MongoPath.MappedMongoPath mapFieldNames(PathAndType cacheKey) {

		MongoPath.RawMongoPath mongoPath = cacheKey.path();
		MongoPersistentEntity<?> root = mappingContext.getPersistentEntity(cacheKey.type());
		MongoPersistentEntity<?> persistentEntity = root;

		List<PathSegment> segments = new ArrayList<>(mongoPath.getSegments().size());

		for (int i = 0; i < mongoPath.getSegments().size(); i++) {

			EntityIndexSegment eis = segment(i, mongoPath.getSegments(), persistentEntity);
			segments.add(eis.segment());
			persistentEntity = eis.entity();
			i = eis.index();
		}

		return new MongoPath.MappedMongoPathImpl(mongoPath, root.getTypeInformation(), segments);
	}

	EntityIndexSegment segment(int index, List<PathSegment> segments, MongoPersistentEntity<?> currentEntity) {

		PathSegment segment = segments.get(index);
		MongoPersistentEntity<?> entity = currentEntity;

		if (entity != null && segment instanceof PropertySegment) {

			MongoPersistentProperty persistentProperty = entity.getPersistentProperty(segment.segment());

			if (persistentProperty != null) {

				entity = mappingContext.getPersistentEntity(persistentProperty);

				if (persistentProperty.isUnwrapped()) {

					if (segments.size() > index + 1) {
						EntityIndexSegment inner = segment(index + 1, segments, entity);
						if (inner.segment() instanceof MappedPropertySegment mappedInnerSegment) {
							return new EntityIndexSegment(inner.entity(), inner.index(),
									new WrappedSegment(mappedInnerSegment.getMappedName(),
											new MappedPropertySegment(persistentProperty.findAnnotation(Unwrapped.class).prefix(), segment,
													persistentProperty),
											mappedInnerSegment));
						}
					} else {
						return new EntityIndexSegment(entity, index, new WrappedSegment("", new MappedPropertySegment(
								persistentProperty.findAnnotation(Unwrapped.class).prefix(), segment, persistentProperty), null));
					}
				} else if (persistentProperty.isAssociation()) {
					return new EntityIndexSegment(entity, index, new AssociationSegment(
							new MappedPropertySegment(persistentProperty.getFieldName(), segment, persistentProperty)));
				}

				return new EntityIndexSegment(entity, index,
						new MappedPropertySegment(persistentProperty.getFieldName(), segment, persistentProperty));
			}
		}
		return new EntityIndexSegment(entity, index, segment);
	}

	record EntityIndexSegment(MongoPersistentEntity<?> entity, int index, PathSegment segment) {
	}
}
