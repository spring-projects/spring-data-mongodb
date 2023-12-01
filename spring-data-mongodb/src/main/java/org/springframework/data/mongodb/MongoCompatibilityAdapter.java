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
package org.springframework.data.mongodb;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.mongodb.ServerAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mongodb.core.MongoClientSettingsFactoryBean;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MapReducePublisher;

/**
 * @author Christoph Strobl
 * @since 2023/12
 */
public class MongoCompatibilityAdapter {

	private static final String NO_LONGER_SUPPORTED = "%s is no longer supported on Mongo Client 5+";

	public static ClientSettingsBuilderAdapter clientSettingsBuilderAdapter(MongoClientSettings.Builder builder) {
		return new MongoStreamFactoryFactorySettingsConfigurer(builder)::setStreamFactory;
	}

	public static ClientSettingsAdapter clientSettingsAdapter(MongoClientSettings clientSettings) {
		return new ClientSettingsAdapter() {
			@Override
			public <T> T getStreamFactoryFactory() {
				if (MongoClientVersion.is5PlusClient()) {
					return null;
				}

				Method getStreamFactoryFactory = ReflectionUtils.findMethod(MongoClientSettings.class,
						"getStreamFactoryFactory");
				return getStreamFactoryFactory != null
						? (T) ReflectionUtils.invokeMethod(getStreamFactoryFactory, clientSettings)
						: null;
			}
		};
	}

	public static IndexOptionsAdapter indexOptionsAdapter(IndexOptions options) {
		return new IndexOptionsAdapter() {
			@Override
			public void setBucketSize(Double bucketSize) {

				if (MongoClientVersion.is5PlusClient()) {
					throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("IndexOptions.bucketSize"));
				}

				Method setBucketSize = ReflectionUtils.findMethod(IndexOptions.class, "bucketSize", Double.class);
				ReflectionUtils.invokeMethod(setBucketSize, options, bucketSize);
			}
		};
	}

	@SuppressWarnings({ "deprecation" })
	public static MapReduceIterableAdapter mapReduceIterableAdapter(MapReduceIterable<?> iterable) {
		return sharded -> {
			if (MongoClientVersion.is5PlusClient()) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("sharded"));
			}

			Method shardedMethod = ReflectionUtils.findMethod(iterable.getClass(), "MapReduceIterable.sharded",
					boolean.class);
			ReflectionUtils.invokeMethod(shardedMethod, iterable, shardedMethod);
		};
	}

	public static MapReducePublisherAdapter mapReducePublisherAdapter(MapReducePublisher<?> publisher) {
		return sharded -> {
			if (MongoClientVersion.is5PlusClient()) {
				throw new UnsupportedOperationException(NO_LONGER_SUPPORTED.formatted("sharded"));
			}

			Method shardedMethod = ReflectionUtils.findMethod(publisher.getClass(), "MapReduceIterable.sharded",
					boolean.class);
			ReflectionUtils.invokeMethod(shardedMethod, publisher, shardedMethod);
		};
	}

	public static ServerAddressAdapter serverAddressAdapter(ServerAddress serverAddress) {
		return new ServerAddressAdapter() {
			@Override
			public InetSocketAddress getSocketAddress() {

				if(MongoClientVersion.is5PlusClient()) {
					return null;
				}

				Method serverAddressMethod = ReflectionUtils.findMethod(serverAddress.getClass(), "getSocketAddress");
				Object value = ReflectionUtils.invokeMethod(serverAddressMethod, serverAddress);
				return value != null ? InetSocketAddress.class.cast(value) : null;
			}
		};
	}

	public interface IndexOptionsAdapter {
		void setBucketSize(Double bucketSize);
	}

	public interface ClientSettingsAdapter {
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
		InetSocketAddress getSocketAddress();
	}

	static class MongoStreamFactoryFactorySettingsConfigurer {

		private static final Log logger = LogFactory.getLog(MongoClientSettingsFactoryBean.class);

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

			if (MongoClientVersion.is5PlusClient()) {
				logger.warn("StreamFactoryFactory is no longer available. Use TransportSettings instead.");
			}

			if (isStreamFactoryPresent()) { //
				try {
					Class<?> streamFactoryType = ClassUtils.forName(STREAM_FACTORY_NAME,
							streamFactory.getClass().getClassLoader());
					if (!ClassUtils.isAssignable(streamFactoryType, streamFactory.getClass())) {
						throw new IllegalArgumentException("Expected %s but found %s".formatted(streamFactoryType, streamFactory));
					}

					Method setter = ReflectionUtils.findMethod(settingsBuilder.getClass(), "streamFactoryFactory",
							streamFactoryType);
					if (setter != null) {
						ReflectionUtils.invokeMethod(setter, settingsBuilder, streamFactoryType.cast(streamFactory));
					}
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Cannot set StreamFactoryFactory for %s".formatted(settingsBuilder), e);
				}
			}
		}
	}

}
