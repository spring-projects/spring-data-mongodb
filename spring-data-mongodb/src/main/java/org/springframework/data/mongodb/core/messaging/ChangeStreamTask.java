/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.PrefixingDelegatingAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.Message.MessageProperties;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ErrorHandler;
import org.springframework.util.StringUtils;

import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * {@link Task} implementation for obtaining {@link ChangeStreamDocument ChangeStreamDocuments} from MongoDB.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
class ChangeStreamTask extends CursorReadingTask<ChangeStreamDocument<Document>, Object> {

	private final Set<String> blacklist = new HashSet<>(
			Arrays.asList("operationType", "fullDocument", "documentKey", "updateDescription", "ns"));

	private final QueryMapper queryMapper;
	private final MongoConverter mongoConverter;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	ChangeStreamTask(MongoTemplate template, ChangeStreamRequest<?> request, Class<?> targetType,
			ErrorHandler errorHandler) {
		super(template, (ChangeStreamRequest) request, (Class) targetType, errorHandler);

		queryMapper = new QueryMapper(template.getConverter());
		mongoConverter = template.getConverter();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.CursorReadingTask#initCursor(org.springframework.data.mongodb.core.MongoTemplate, org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions, java.lang.Class)
	 */
	@Override
	protected MongoCursor<ChangeStreamDocument<Document>> initCursor(MongoTemplate template, RequestOptions options,
			Class<?> targetType) {

		List<Document> filter = Collections.emptyList();
		BsonDocument resumeToken = new BsonDocument();
		Collation collation = null;
		FullDocument fullDocument = ClassUtils.isAssignable(Document.class, targetType) ? FullDocument.DEFAULT
				: FullDocument.UPDATE_LOOKUP;
		BsonTimestamp startAt = null;

		if (options instanceof ChangeStreamRequest.ChangeStreamRequestOptions) {

			ChangeStreamOptions changeStreamOptions = ((ChangeStreamRequestOptions) options).getChangeStreamOptions();
			filter = prepareFilter(template, changeStreamOptions);

			if (changeStreamOptions.getFilter().isPresent()) {

				Object val = changeStreamOptions.getFilter().get();
				if (val instanceof Aggregation) {
					collation = ((Aggregation) val).getOptions().getCollation()
							.map(org.springframework.data.mongodb.core.query.Collation::toMongoCollation).orElse(null);
				}
			}

			if (changeStreamOptions.getResumeToken().isPresent()) {
				resumeToken = changeStreamOptions.getResumeToken().get().asDocument();
			}

			fullDocument = changeStreamOptions.getFullDocumentLookup()
					.orElseGet(() -> ClassUtils.isAssignable(Document.class, targetType) ? FullDocument.DEFAULT
							: FullDocument.UPDATE_LOOKUP);

			startAt = changeStreamOptions.getResumeTimestamp().map(Instant::toEpochMilli).map(BsonTimestamp::new)
					.orElse(null);
		}

		MongoDatabase db = StringUtils.hasText(options.getDatabaseName())
				? template.getMongoDbFactory().getDb(options.getDatabaseName()) : template.getDb();

		ChangeStreamIterable<Document> iterable;

		if (StringUtils.hasText(options.getCollectionName())) {
			iterable = filter.isEmpty() ? db.getCollection(options.getCollectionName()).watch(Document.class)
					: db.getCollection(options.getCollectionName()).watch(filter, Document.class);

		} else {
			iterable = filter.isEmpty() ? db.watch(Document.class) : db.watch(filter, Document.class);
		}

		if (!resumeToken.isEmpty()) {
			iterable = iterable.resumeAfter(resumeToken);
		}

		if (startAt != null) {
			iterable.startAtOperationTime(startAt);
		}

		if (collation != null) {
			iterable = iterable.collation(collation);
		}

		iterable = iterable.fullDocument(fullDocument);

		return iterable.iterator();
	}

	@SuppressWarnings("unchecked")
	List<Document> prepareFilter(MongoTemplate template, ChangeStreamOptions options) {

		if (!options.getFilter().isPresent()) {
			return Collections.emptyList();
		}

		Object filter = options.getFilter().orElse(null);

		if (filter instanceof Aggregation) {
			Aggregation agg = (Aggregation) filter;
			AggregationOperationContext context = agg instanceof TypedAggregation
					? new TypeBasedAggregationOperationContext(((TypedAggregation<?>) agg).getInputType(),
							template.getConverter().getMappingContext(), queryMapper)
					: Aggregation.DEFAULT_CONTEXT;

			return agg.toPipeline(new PrefixingDelegatingAggregationOperationContext(context, "fullDocument", blacklist));
		}

		if (filter instanceof List) {
			return (List<Document>) filter;
		}

		throw new IllegalArgumentException(
				"ChangeStreamRequestOptions.filter mut be either an Aggregation or a plain list of Documents");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.CursorReadingTask#createMessage(java.lang.Object, java.lang.Class, org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions)
	 */
	@Override
	protected Message<ChangeStreamDocument<Document>, Object> createMessage(ChangeStreamDocument<Document> source,
			Class<Object> targetType, RequestOptions options) {

		MongoNamespace namespace = source.getNamespace() != null ? source.getNamespace()
				: createNamespaceFromOptions(options);

		return new ChangeStreamEventMessage<>(new ChangeStreamEvent<>(source, targetType, mongoConverter), MessageProperties
				.builder().databaseName(namespace.getDatabaseName()).collectionName(namespace.getCollectionName()).build());
	}

	MongoNamespace createNamespaceFromOptions(RequestOptions options) {

		String collectionName = StringUtils.hasText(options.getCollectionName()) ? options.getCollectionName() : "unknown";
		String databaseName = StringUtils.hasText(options.getDatabaseName()) ? options.getDatabaseName() : "unknown";

		return new MongoNamespace(databaseName, collectionName);
	}

	/**
	 * {@link Message} implementation for ChangeStreams
	 *
	 * @since 2.1
	 */
	@AllArgsConstructor
	static class ChangeStreamEventMessage<T> implements Message<ChangeStreamDocument<Document>, T> {

		private final ChangeStreamEvent<T> delegate;
		private final MessageProperties messageProperties;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.messaging.Message#getRaw()
		 */
		@Nullable
		@Override
		public ChangeStreamDocument<Document> getRaw() {
			return delegate.getRaw();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.messaging.Message#getBody()
		 */
		@Nullable
		@Override
		public T getBody() {
			return delegate.getBody();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.messaging.Message#getProperties()
		 */
		@Override
		public MessageProperties getProperties() {
			return this.messageProperties;
		}

		/**
		 * @return the resume token or {@literal null} if not set.
		 * @see ChangeStreamEvent#getResumeToken()
		 */
		@Nullable
		BsonValue getResumeToken() {
			return delegate.getResumeToken();
		}

		/**
		 * @return the cluster time of the event or {@literal null}.
		 * @see ChangeStreamEvent#getTimestamp()
		 */
		@Nullable
		Instant getTimestamp() {
			return delegate.getTimestamp();
		}

		/**
		 * Get the {@link ChangeStreamEvent} from the message.
		 *
		 * @return never {@literal null}.
		 */
		ChangeStreamEvent<T> getChangeStreamEvent() {
			return delegate;
		}
	}
}
