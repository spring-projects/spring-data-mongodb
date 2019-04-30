/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperationsProvider;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.repository.query.MongoEntityMetadata;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.repository.core.support.QueryCreationListener;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import com.mongodb.MongoException;

/**
 * {@link QueryCreationListener} inspecting {@link PartTreeMongoQuery}s and creating an index for the properties it
 * refers to.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class IndexEnsuringQueryCreationListener implements QueryCreationListener<PartTreeMongoQuery> {

	private static final Set<Type> GEOSPATIAL_TYPES = new HashSet<Type>(Arrays.asList(Type.NEAR, Type.WITHIN));
	private static final Logger LOG = LoggerFactory.getLogger(IndexEnsuringQueryCreationListener.class);

	private final IndexOperationsProvider indexOperationsProvider;

	/**
	 * Creates a new {@link IndexEnsuringQueryCreationListener} using the given {@link MongoOperations}.
	 *
	 * @param indexOperationsProvider must not be {@literal null}.
	 */
	public IndexEnsuringQueryCreationListener(IndexOperationsProvider indexOperationsProvider) {

		Assert.notNull(indexOperationsProvider, "IndexOperationsProvider must not be null!");
		this.indexOperationsProvider = indexOperationsProvider;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.QueryCreationListener#onCreation(org.springframework.data.repository.query.RepositoryQuery)
	 */
	public void onCreation(PartTreeMongoQuery query) {

		PartTree tree = query.getTree();

		if (!tree.hasPredicate()) {
			return;
		}

		Index index = new Index();
		index.named(query.getQueryMethod().getName());
		Sort sort = tree.getSort();

		for (Part part : tree.getParts()) {
			if (GEOSPATIAL_TYPES.contains(part.getType())) {
				return;
			}
			String property = part.getProperty().toDotPath();
			Direction order = toDirection(sort, property);
			index.on(property, order);
		}

		// Add fixed sorting criteria to index
		if (sort.isSorted()) {
			for (Order order : sort) {
				index.on(order.getProperty(), order.getDirection());
			}
		}

		if (query.getQueryMethod().hasAnnotatedCollation()) {

			String collation = query.getQueryMethod().getAnnotatedCollation();
			if (!collation.contains("?")) {
				index = index.collation(Collation.parse(collation));
			}
		}

		MongoEntityMetadata<?> metadata = query.getQueryMethod().getEntityInformation();
		try {
			indexOperationsProvider.indexOps(metadata.getCollectionName()).ensureIndex(index);
		} catch (UncategorizedMongoDbException e) {

			if (e.getCause() instanceof MongoException) {

				/*
				 * As of MongoDB 4.2 index creation raises an error when creating an index for the very same keys with
				 * different name, whereas previous versions silently ignored this.
				 * Because an index is by default named after the repository finder method it is not uncommon that an index
				 * for the very same property combination might already exist with a different name.
				 * So you see, that's why we need to ignore the error here.
				 *
				 * For details please see: https://docs.mongodb.com/master/release-notes/4.2-compatibility/#indexes
				 */
				if (((MongoException) e.getCause()).getCode() != 85) {
					throw e;
				}
			}
		}
		LOG.debug(String.format("Created %s!", index));
	}

	private static Direction toDirection(Sort sort, String property) {

		if (sort.isUnsorted()) {
			return Direction.DESC;
		}

		Order order = sort.getOrderFor(property);
		return order == null ? Direction.DESC : order.isAscending() ? Direction.ASC : Direction.DESC;
	}
}
