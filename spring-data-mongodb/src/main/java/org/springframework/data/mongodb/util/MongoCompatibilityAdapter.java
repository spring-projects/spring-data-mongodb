/*
 * Copyright 2024-2025 the original author or authors.
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
package org.springframework.data.mongodb.util;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MapReducePublisher;

/**
 * Compatibility adapter to bridge functionality across different MongoDB driver versions.
 * <p>
 * This class is for internal use within the framework and should not be used by applications.
 *
 * @author Christoph Strobl
 * @since 4.3
 */
public class MongoCompatibilityAdapter {

	private static final String NO_LONGER_SUPPORTED = "%s is no longer supported on Mongo Client 5 or newer";

	private static final @Nullable Method getStreamFactoryFactory = ReflectionUtils.findMethod(MongoClientSettings.class,
			"getStreamFactoryFactory");

	private static final @Nullable Method setBucketSize = ReflectionUtils.findMethod(IndexOptions.class, "bucketSize",
			Double.class);

	/**
	 * Return a compatibility adapter for {@link MongoClientSettings.Builder}.
	 *
	 * @param builder
	 * @return
	 */
	public static ClientSettingsBuilderAdapter clientSettingsBuilderAdapter(MongoClientSettings.Builder builder) {
		return new MongoStreamFactoryFactorySettingsConfigurer(builder)::setStreamFactory;
	}

	/**
	 * Return a compatibility adapter for {@link MongoClientSettings}.
	 *
	 * @param clientSettings
	 * @return
	 */
	public static ClientSettingsAdapter clientSettingsAdapter(MongoClientSettings clientSettings) {
		return new ClientSettingsAdapter() {
			@Override
			public <T> T getStreamFactoryFactory() {

				if (MongoClientVersion.isVersion5orNewer() || getStreamFactoryFactory == null) {
					return null;
				}

				return (T) ReflectionUtils.invokeMethod(getStreamFactoryFactory, clientSettings);
			}
		};
	}

	/**
	 * Return a compatibility adapter for {@link IndexOptions}.
	 *
	 * @param options
	 * @return
	 */
	public static IndexOptionsAdapter indexOptionsAdapter(IndexOptions options) {
		return bucketSize -> {

			if (MongoClientVersion.isVersion5orNewer() || setBucketSize == null) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("IndexOptions.bucketSize"));
			}

			ReflectionUtils.invokeMethod(setBucketSize, options, bucketSize);
		};
	}

	/**
	 * Return a compatibility adapter for {@code MapReduceIterable}.
	 *
	 * @param iterable
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static MapReduceIterableAdapter mapReduceIterableAdapter(Object iterable) {
		return sharded -> {

			if (MongoClientVersion.isVersion5orNewer()) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("sharded"));
			}

			// Use MapReduceIterable to avoid package-protected access violations to
			// com.mongodb.client.internal.MapReduceIterableImpl
			Method shardedMethod = ReflectionUtils.findMethod(MapReduceIterable.class, "sharded", boolean.class);
			ReflectionUtils.invokeMethod(shardedMethod, iterable, sharded);
		};
	}

	/**
	 * Return a compatibility adapter for {@code MapReducePublisher}.
	 *
	 * @param publisher
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static MapReducePublisherAdapter mapReducePublisherAdapter(Object publisher) {
		return sharded -> {

			if (MongoClientVersion.isVersion5orNewer()) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("sharded"));
			}

			// Use MapReducePublisher to avoid package-protected access violations to MapReducePublisherImpl
			Method shardedMethod = ReflectionUtils.findMethod(MapReducePublisher.class, "sharded", boolean.class);
			ReflectionUtils.invokeMethod(shardedMethod, publisher, sharded);
		};
	}

	/**
	 * Return a compatibility adapter for {@link ServerAddress}.
	 *
	 * @param serverAddress
	 * @return
	 */
	public static ServerAddressAdapter serverAddressAdapter(ServerAddress serverAddress) {
		return () -> {

			if (MongoClientVersion.isVersion5orNewer()) {
				return null;
			}

			Method serverAddressMethod = ReflectionUtils.findMethod(ServerAddress.class, "getSocketAddress");
			Object value = ReflectionUtils.invokeMethod(serverAddressMethod, serverAddress);
			return value != null ? InetSocketAddress.class.cast(value) : null;
		};
	}

	public static MongoDatabaseAdapterBuilder mongoDatabaseAdapter() {
		return MongoDatabaseAdapter::new;
	}

	public static ReactiveMongoDatabaseAdapterBuilder reactiveMongoDatabaseAdapter() {
		return ReactiveMongoDatabaseAdapter::new;
	}

	public interface IndexOptionsAdapter {
		void setBucketSize(double bucketSize);
	}

	public interface ClientSettingsAdapter {
		@Nullable
		<T> T getStreamFactoryFactory();
	}

	public interface ClientSettingsBuilderAdapter {
		<T> void setStreamFactoryFactory(T streamFactory);
	}

	public interface MapReduceIterableAdapter {
		void sharded(boolean sharded);
	}

	public interface MapReducePublisherAdapter {
		void sharded(boolean sharded);
	}

	public interface ServerAddressAdapter {
		@Nullable
		InetSocketAddress getSocketAddress();
	}

	public interface MongoDatabaseAdapterBuilder {
		MongoDatabaseAdapter forDb(com.mongodb.client.MongoDatabase db);
	}

	@SuppressWarnings({ "unchecked", "DataFlowIssue" })
	public static class MongoDatabaseAdapter {

		@Nullable //
		private static final Method LIST_COLLECTION_NAMES_METHOD;

		@Nullable //
		private static final Method LIST_COLLECTION_NAMES_METHOD_SESSION;

		private static final Class<?> collectionNamesReturnType;

		private final MongoDatabase db;

		static {

			if (MongoClientVersion.isSyncClientPresent()) {

				LIST_COLLECTION_NAMES_METHOD = ReflectionUtils.findMethod(MongoDatabase.class, "listCollectionNames");
				LIST_COLLECTION_NAMES_METHOD_SESSION = ReflectionUtils.findMethod(MongoDatabase.class, "listCollectionNames",
						ClientSession.class);

				if (MongoClientVersion.isVersion5orNewer()) {
					try {
						collectionNamesReturnType = ClassUtils.forName("com.mongodb.client.ListCollectionNamesIterable",
								MongoDatabaseAdapter.class.getClassLoader());
					} catch (ClassNotFoundException e) {
						throw new IllegalStateException("Unable to load com.mongodb.client.ListCollectionNamesIterable", e);
					}
				} else {
					try {
						collectionNamesReturnType = ClassUtils.forName("com.mongodb.client.MongoIterable",
								MongoDatabaseAdapter.class.getClassLoader());
					} catch (ClassNotFoundException e) {
						throw new IllegalStateException("Unable to load com.mongodb.client.ListCollectionNamesIterable", e);
					}
				}
			} else {
				LIST_COLLECTION_NAMES_METHOD = null;
				LIST_COLLECTION_NAMES_METHOD_SESSION = null;
				collectionNamesReturnType = Object.class;
			}
		}

		public MongoDatabaseAdapter(MongoDatabase db) {
			this.db = db;
		}

		public Class<? extends MongoIterable<String>> collectionNameIterableType() {
			return (Class<? extends MongoIterable<String>>) collectionNamesReturnType;
		}

		public MongoIterable<String> listCollectionNames() {

			Assert.state(LIST_COLLECTION_NAMES_METHOD != null, "No method listCollectionNames present for %s".formatted(db));
			return (MongoIterable<String>) ReflectionUtils.invokeMethod(LIST_COLLECTION_NAMES_METHOD, db);
		}

		public MongoIterable<String> listCollectionNames(ClientSession clientSession) {
			Assert.state(LIST_COLLECTION_NAMES_METHOD != null,
					"No method listCollectionNames(ClientSession) present for %s".formatted(db));
			return (MongoIterable<String>) ReflectionUtils.invokeMethod(LIST_COLLECTION_NAMES_METHOD_SESSION, db,
					clientSession);
		}
	}

	public interface ReactiveMongoDatabaseAdapterBuilder {
		ReactiveMongoDatabaseAdapter forDb(com.mongodb.reactivestreams.client.MongoDatabase db);
	}

	@SuppressWarnings({ "unchecked", "DataFlowIssue" })
	public static class ReactiveMongoDatabaseAdapter {

		@Nullable //
		private static final Method LIST_COLLECTION_NAMES_METHOD;

		@Nullable //
		private static final Method LIST_COLLECTION_NAMES_METHOD_SESSION;

		private static final Class<?> collectionNamesReturnType;

		private final com.mongodb.reactivestreams.client.MongoDatabase db;

		static {

			if (MongoClientVersion.isReactiveClientPresent()) {

				LIST_COLLECTION_NAMES_METHOD = ReflectionUtils
						.findMethod(com.mongodb.reactivestreams.client.MongoDatabase.class, "listCollectionNames");
				LIST_COLLECTION_NAMES_METHOD_SESSION = ReflectionUtils.findMethod(
						com.mongodb.reactivestreams.client.MongoDatabase.class, "listCollectionNames",
						com.mongodb.reactivestreams.client.ClientSession.class);

				if (MongoClientVersion.isVersion5orNewer()) {
					try {
						collectionNamesReturnType = ClassUtils.forName(
								"com.mongodb.reactivestreams.client.ListCollectionNamesPublisher",
								ReactiveMongoDatabaseAdapter.class.getClassLoader());
					} catch (ClassNotFoundException e) {
						throw new IllegalStateException("com.mongodb.reactivestreams.client.ListCollectionNamesPublisher", e);
					}
				} else {
					try {
						collectionNamesReturnType = ClassUtils.forName("org.reactivestreams.Publisher",
								ReactiveMongoDatabaseAdapter.class.getClassLoader());
					} catch (ClassNotFoundException e) {
						throw new IllegalStateException("org.reactivestreams.Publisher", e);
					}
				}
			} else {
				LIST_COLLECTION_NAMES_METHOD = null;
				LIST_COLLECTION_NAMES_METHOD_SESSION = null;
				collectionNamesReturnType = Object.class;
			}
		}

		ReactiveMongoDatabaseAdapter(com.mongodb.reactivestreams.client.MongoDatabase db) {
			this.db = db;
		}

		public Class<? extends Publisher<String>> collectionNamePublisherType() {
			return (Class<? extends Publisher<String>>) collectionNamesReturnType;

		}

		public Publisher<String> listCollectionNames() {
			Assert.state(LIST_COLLECTION_NAMES_METHOD != null, "No method listCollectionNames present for %s".formatted(db));
			return (Publisher<String>) ReflectionUtils.invokeMethod(LIST_COLLECTION_NAMES_METHOD, db);
		}

		public Publisher<String> listCollectionNames(com.mongodb.reactivestreams.client.ClientSession clientSession) {
			Assert.state(LIST_COLLECTION_NAMES_METHOD != null,
					"No method listCollectionNames(ClientSession) present for %s".formatted(db));
			return (Publisher<String>) ReflectionUtils.invokeMethod(LIST_COLLECTION_NAMES_METHOD_SESSION, db, clientSession);
		}
	}

	static class MongoStreamFactoryFactorySettingsConfigurer {

		private static final Log logger = LogFactory.getLog(MongoStreamFactoryFactorySettingsConfigurer.class);

		private static final String STREAM_FACTORY_NAME = "com.mongodb.connection.StreamFactoryFactory";
		private static final boolean STREAM_FACTORY_PRESENT = ClassUtils.isPresent(STREAM_FACTORY_NAME,
				MongoCompatibilityAdapter.class.getClassLoader());
		private final MongoClientSettings.Builder settingsBuilder;

		static boolean isStreamFactoryPresent() {
			return STREAM_FACTORY_PRESENT;
		}

		public MongoStreamFactoryFactorySettingsConfigurer(Builder settingsBuilder) {
			this.settingsBuilder = settingsBuilder;
		}

		void setStreamFactory(Object streamFactory) {

			if (MongoClientVersion.isVersion5orNewer() && isStreamFactoryPresent()) {
				logger.warn("StreamFactoryFactory is no longer available. Use TransportSettings instead.");
				return;
			}

			try {
				Class<?> streamFactoryType = ClassUtils.forName(STREAM_FACTORY_NAME, streamFactory.getClass().getClassLoader());

				if (!ClassUtils.isAssignable(streamFactoryType, streamFactory.getClass())) {
					throw new IllegalArgumentException("Expected %s but found %s".formatted(streamFactoryType, streamFactory));
				}

				Method setter = ReflectionUtils.findMethod(settingsBuilder.getClass(), "streamFactoryFactory",
						streamFactoryType);
				if (setter != null) {
					ReflectionUtils.invokeMethod(setter, settingsBuilder, streamFactoryType.cast(streamFactory));
				}
			} catch (ReflectiveOperationException e) {
				throw new IllegalArgumentException("Cannot set StreamFactoryFactory for %s".formatted(settingsBuilder), e);
			}
		}
	}

}
