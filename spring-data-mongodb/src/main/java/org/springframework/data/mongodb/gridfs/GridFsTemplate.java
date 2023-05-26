/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

/**
 * {@link GridFsOperations} implementation to store content into MongoDB GridFS.
 *
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Thomas Darimont
 * @author Martin Baumgartner
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Hartmut Lang
 * @author Niklas Helge Hanft
 * @author Denis Zavedeev
 */
public class GridFsTemplate extends GridFsOperationsSupport implements GridFsOperations, ResourcePatternResolver {

	private final Supplier<GridFSBucket> bucketSupplier;

	/**
	 * Creates a new {@link GridFsTemplate} using the given {@link MongoDatabaseFactory} and {@link MongoConverter}.
	 * <p>
	 * Note that the {@link GridFSBucket} is obtained only once from {@link MongoDatabaseFactory#getMongoDatabase()
	 * MongoDatabase}. Use {@link #GridFsTemplate(MongoConverter, Supplier)} if you want to use different buckets from the
	 * same Template instance.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public GridFsTemplate(MongoDatabaseFactory dbFactory, MongoConverter converter) {
		this(dbFactory, converter, null);
	}

	/**
	 * Creates a new {@link GridFsTemplate} using the given {@link MongoDatabaseFactory} and {@link MongoConverter}.
	 * <p>
	 * Note that the {@link GridFSBucket} is obtained only once from {@link MongoDatabaseFactory#getMongoDatabase()
	 * MongoDatabase}. Use {@link #GridFsTemplate(MongoConverter, Supplier)} if you want to use different buckets from the
	 * same Template instance.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket can be {@literal null}.
	 */
	public GridFsTemplate(MongoDatabaseFactory dbFactory, MongoConverter converter, @Nullable String bucket) {
		this(converter, Lazy.of(() -> getGridFs(dbFactory, bucket)));
	}

	/**
	 * Creates a new {@link GridFsTemplate} using the given {@link MongoConverter} and {@link Supplier} providing the
	 * required {@link GridFSBucket}.
	 *
	 * @param converter must not be {@literal null}.
	 * @param gridFSBucket must not be {@literal null}.
	 * @since 4.2
	 */
	public GridFsTemplate(MongoConverter converter, Supplier<GridFSBucket> gridFSBucket) {

		super(converter);

		Assert.notNull(gridFSBucket, "GridFSBucket supplier must not be null");

		this.bucketSupplier = gridFSBucket;
	}

	@Override
	public ObjectId store(InputStream content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata) {
		return store(content, filename, contentType, toDocument(metadata));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T store(GridFsObject<T, InputStream> upload) {

		GridFSUploadOptions uploadOptions = computeUploadOptionsFor(upload.getOptions().getContentType(),
				upload.getOptions().getMetadata());

		if (upload.getOptions().getChunkSize() > 0) {
			uploadOptions.chunkSizeBytes(upload.getOptions().getChunkSize());
		}

		if (upload.getFileId() == null) {
			return (T) getGridFs().uploadFromStream(upload.getFilename(), upload.getContent(), uploadOptions);
		}

		getGridFs().uploadFromStream(BsonUtils.simpleToBsonValue(upload.getFileId()), upload.getFilename(),
				upload.getContent(), uploadOptions);
		return upload.getFileId();
	}

	@Override
	public GridFSFindIterable find(Query query) {

		Assert.notNull(query, "Query must not be null");

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		GridFSFindIterable iterable = getGridFs().find(queryObject).sort(sortObject);

		if (query.getSkip() > 0) {
			iterable = iterable.skip(Math.toIntExact(query.getSkip()));
		}

		if (query.getLimit() > 0) {
			iterable = iterable.limit(query.getLimit());
		}

		return iterable;
	}

	@Override
	public GridFSFile findOne(Query query) {
		return find(query).first();
	}

	@Override
	public void delete(Query query) {

		for (GridFSFile gridFSFile : find(query)) {
			getGridFs().delete(gridFSFile.getId());
		}
	}

	@Override
	public ClassLoader getClassLoader() {
		return null;
	}

	@Override
	public GridFsResource getResource(String location) {

		return Optional.ofNullable(findOne(query(whereFilename().is(location)))) //
				.map(this::getResource) //
				.orElseGet(() -> GridFsResource.absent(location));
	}

	@Override
	public GridFsResource getResource(GridFSFile file) {

		Assert.notNull(file, "GridFSFile must not be null");

		return new GridFsResource(file, getGridFs().openDownloadStream(file.getId()));
	}

	@Override
	public GridFsResource[] getResources(String locationPattern) {

		if (!StringUtils.hasText(locationPattern)) {
			return new GridFsResource[0];
		}

		AntPath path = new AntPath(locationPattern);

		if (path.isPattern()) {

			GridFSFindIterable files = find(query(whereFilename().regex(path.toRegex())));
			List<GridFsResource> resources = new ArrayList<>();

			for (GridFSFile file : files) {
				resources.add(getResource(file));
			}

			return resources.toArray(new GridFsResource[0]);
		}

		return new GridFsResource[] { getResource(locationPattern) };
	}

	private GridFSBucket getGridFs() {
		return this.bucketSupplier.get();
	}

	private static GridFSBucket getGridFs(MongoDatabaseFactory dbFactory, @Nullable String bucket) {

		Assert.notNull(dbFactory, "MongoDatabaseFactory must not be null");

		MongoDatabase db = dbFactory.getMongoDatabase();
		return bucket == null ? GridFSBuckets.create(db) : GridFSBuckets.create(db, bucket);
	}
}
