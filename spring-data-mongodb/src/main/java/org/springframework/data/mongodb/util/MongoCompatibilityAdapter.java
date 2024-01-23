/*
 * Copyright 2024 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.IndexOptions;

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

				if (MongoClientVersion.isVersion5OrNewer() || getStreamFactoryFactory == null) {
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

			if (MongoClientVersion.isVersion5OrNewer() || setBucketSize == null) {
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
	public static MapReduceIterableAdapter mapReduceIterableAdapter(Object iterable) {
		return sharded -> {

			if (MongoClientVersion.isVersion5OrNewer()) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("sharded"));
			}

			Method shardedMethod = ReflectionUtils.findMethod(iterable.getClass(), "sharded", boolean.class);
			ReflectionUtils.invokeMethod(shardedMethod, iterable, sharded);
		};
	}

	/**
	 * Return a compatibility adapter for {@code MapReducePublisher}.
	 *
	 * @param publisher
	 * @return
	 */
	public static MapReducePublisherAdapter mapReducePublisherAdapter(Object publisher) {
		return sharded -> {

			if (MongoClientVersion.isVersion5OrNewer()) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("sharded"));
			}

			Method shardedMethod = ReflectionUtils.findMethod(publisher.getClass(), "sharded", boolean.class);
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

			if (MongoClientVersion.isVersion5OrNewer()) {
				return null;
			}

			Method serverAddressMethod = ReflectionUtils.findMethod(serverAddress.getClass(), "getSocketAddress");
			Object value = ReflectionUtils.invokeMethod(serverAddressMethod, serverAddress);
			return value != null ? InetSocketAddress.class.cast(value) : null;
		};
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

			if (MongoClientVersion.isVersion5OrNewer() && isStreamFactoryPresent()) {
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
