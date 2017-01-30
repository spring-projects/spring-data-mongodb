/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
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
 */
public class GridFsTemplate implements GridFsOperations, ResourcePatternResolver {

	private final MongoDbFactory dbFactory;
	private final String bucket;
	private final MongoConverter converter;
	private final QueryMapper queryMapper;

	/**
	 * Creates a new {@link GridFsTemplate} using the given {@link MongoDbFactory} and {@link MongoConverter}.
	 * 
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public GridFsTemplate(MongoDbFactory dbFactory, MongoConverter converter) {
		this(dbFactory, converter, null);
	}

	/**
	 * Creates a new {@link GridFsTemplate} using the given {@link MongoDbFactory} and {@link MongoConverter}.
	 * 
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket
	 */
	public GridFsTemplate(MongoDbFactory dbFactory, MongoConverter converter, String bucket) {

		Assert.notNull(dbFactory, "MongoDbFactory must not be null!");
		Assert.notNull(converter, "MongoConverter must not be null!");

		this.dbFactory = dbFactory;
		this.converter = converter;
		this.bucket = bucket;

		this.queryMapper = new QueryMapper(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.String)
	 */
	public ObjectId store(InputStream content, String filename) {
		return store(content, filename, (Object) null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.Object)
	 */

	@Override
	public ObjectId store(InputStream content, Object metadata) {
		return store(content, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, com.mongodb.Document)
	 */
	@Override
	public ObjectId store(InputStream content, Document metadata) {
		return store(content, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.String, java.lang.String)
	 */
	public ObjectId store(InputStream content, String filename, String contentType) {
		return store(content, filename, contentType, (Object) null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.String, java.lang.Object)
	 */
	public ObjectId store(InputStream content, String filename, Object metadata) {
		return store(content, filename, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.String, java.lang.String, java.lang.Object)
	 */
	public ObjectId store(InputStream content, String filename, String contentType, Object metadata) {

		Document document = null;

		if (metadata != null) {
			document = new Document();
			converter.write(metadata, document);
		}

		return store(content, filename, contentType, document);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.String, com.mongodb.Document)
	 */
	public ObjectId store(InputStream content, String filename, Document metadata) {
		return this.store(content, filename, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#store(java.io.InputStream, java.lang.String, com.mongodb.Document)
	 */
	public ObjectId store(InputStream content, String filename, String contentType, Document metadata) {

		Assert.notNull(content, "InputStream must not be null!");

		GridFSUploadOptions opts = new GridFSUploadOptions();

		Document mData = new Document();
		if (StringUtils.hasText(contentType)) {
			mData.put("type", contentType);
		}

		if (metadata != null) {
			mData.putAll(metadata);
		}

		opts.metadata(mData);

		return getGridFs().uploadFromStream(filename, content, opts);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#find(com.mongodb.Document)
	 */
	public GridFSFindIterable find(Query query) {

		if (query == null) {
			return getGridFs().find(new Document());
		}

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		return getGridFs().find(queryObject).sort(sortObject);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#findOne(com.mongodb.Document)
	 */
	public GridFSFile findOne(Query query) {
		return find(query).first();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsOperations#delete(org.springframework.data.mongodb.core.query.Query)
	 */
	public void delete(Query query) {

		for (GridFSFile x : find(query)) {
			getGridFs().delete(((BsonObjectId) x.getId()).getValue());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.ResourceLoader#getClassLoader()
	 */
	public ClassLoader getClassLoader() {
		return dbFactory.getClass().getClassLoader();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.ResourceLoader#getResource(java.lang.String)
	 */
	public GridFsResource getResource(String location) {

		GridFSFile file = findOne(query(whereFilename().is(location)));
		return file != null ? new GridFsResource(file, getGridFs().openDownloadStreamByName(location)) : null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.support.ResourcePatternResolver#getResources(java.lang.String)
	 */
	public GridFsResource[] getResources(String locationPattern) {

		if (!StringUtils.hasText(locationPattern)) {
			return new GridFsResource[0];
		}

		AntPath path = new AntPath(locationPattern);

		if (path.isPattern()) {

			GridFSFindIterable files = find(query(whereFilename().regex(path.toRegex())));
			List<GridFsResource> resources = new ArrayList<GridFsResource>();

			for (GridFSFile file : files) {
				resources.add(new GridFsResource(file, getGridFs().openDownloadStreamByName(file.getFilename())));
			}

			return resources.toArray(new GridFsResource[resources.size()]);
		}

		return new GridFsResource[] { getResource(locationPattern) };
	}

	private Document getMappedQuery(Query query) {
		return query == null ? new Query().getQueryObject() : getMappedQuery(query.getQueryObject());
	}

	private Document getMappedQuery(Document query) {
		return query == null ? null : queryMapper.getMappedObject(query, Optional.empty());
	}

	private GridFSBucket getGridFs() {

		MongoDatabase db = dbFactory.getDb();
		return bucket == null ? GridFSBuckets.create(db) : GridFSBuckets.create(db, bucket);
	}
}
