/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import static org.springframework.data.mongodb.repository.aot.AotPlaceholders.*;

import java.io.StringWriter;
import java.lang.reflect.Field;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.bson.json.StrictJsonWriter;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.mongodb.core.query.GeoCommand;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.MongoClientSettings;

/**
 * Utility to serialize {@link Document} instances to JSON. Serialization considers {@link Placeholder placeholders}.
 *
 * @author Mark Paluch
 * @since 5.0
 */
class DocumentSerializer {

	private static final CodecRegistry JSON_CODEC_REGISTRY = CodecRegistries.fromRegistries(
			CodecRegistries.fromProviders(PlaceholderCodecProvider.INSTACE), MongoClientSettings.getDefaultCodecRegistry());
	private static final Codec<Document> DOCUMENT_CODEC = JSON_CODEC_REGISTRY.get(Document.class);

	/**
	 * Obtain a preconfigured {@link JsonWriter} allowing to render the given {@link Document} using a
	 * {@link CodecRegistry} containing a {@link PlaceholderCodec}.
	 *
	 * @param document the source document. Must not be {@literal null}.
	 * @return new instance of {@link JsonWriter}.
	 * @since 5.0
	 */
	public static String toJson(Document document) {

		PlaceholderJsonWriter writer = new PlaceholderJsonWriter();
		DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().build());

		return writer.getWriter().getBuffer().toString();
	}

	/**
	 * Internal {@link BsonWriter} implementation that allows to render {@link #writePlaceholder(String) placeholders} as
	 * {@code ?0}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 5.0
	 */
	@NullUnmarked
	static class PlaceholderJsonWriter extends JsonWriter implements BsonWriter {

		private static final Field JSON_WRITER = ReflectionUtils.findField(JsonWriter.class, "strictJsonWriter");

		private final StrictJsonWriter jsonWriter;

		public PlaceholderJsonWriter() {

			super(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());

			if (JSON_WRITER == null) {
				throw new IllegalStateException("Cannot resolve 'JsonWriter.strictJsonWriter' field");
			}

			ReflectionUtils.makeAccessible(JSON_WRITER);
			jsonWriter = (StrictJsonWriter) ReflectionUtils.getField(JSON_WRITER, this);

		}

		@Override
		public StringWriter getWriter() {
			return (StringWriter) super.getWriter();
		}

		/**
		 * @param placeholder
		 */
		public void writePlaceholder(String placeholder) {
			checkPreconditions("writePlaceholder", State.VALUE);
			jsonWriter.writeRaw(placeholder);
			setState(getNextState());
		}

	}

	@NullUnmarked
	enum PlaceholderCodecProvider implements CodecProvider {

		INSTACE;

		@Override
		@SuppressWarnings("unchecked")
		public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {

			if (ClassUtils.isAssignable(Placeholder.class, clazz)) {
				return (Codec<T>) PlaceholderCodec.INSTANCE;
			}

			if (ClassUtils.isAssignable(GeoCommand.class, clazz)) {
				return (Codec<T>) GeoCommandCodec.INSTANCE;
			}

			return null;

		}

	}

	/**
	 * Internal {@link Codec} implementation to write {@link Placeholder placeholders}.
	 *
	 * @since 5.0
	 * @author Christoph Strobl
	 */
	@NullUnmarked
	enum PlaceholderCodec implements Codec<Placeholder> {

		INSTANCE;

		@Override
		public Placeholder decode(BsonReader reader, DecoderContext decoderContext) {
			throw new UnsupportedOperationException("decode is not supported for PlaceholderCodec");
		}

		@Override
		public void encode(BsonWriter writer, Placeholder value, EncoderContext encoderContext) {
			if (writer instanceof PlaceholderJsonWriter sjw) {
				sjw.writePlaceholder(value.toString());
			} else {
				writer.writeString(value.toString());
			}
		}

		@Override
		public Class<Placeholder> getEncoderClass() {
			return Placeholder.class;
		}

	}

	enum GeoCommandCodec implements Codec<GeoCommand> {

		INSTANCE;

		@Override
		public GeoCommand decode(BsonReader reader, DecoderContext decoderContext) {
			throw new UnsupportedOperationException("decode is not supported for GeoCommandCodec");
		}

		@Override
		public void encode(BsonWriter writer, GeoCommand value, EncoderContext encoderContext) {

			if (writer instanceof PlaceholderJsonWriter sjw) {
				if (!value.getCommand().equals("$geometry")) {
					writer.writeStartDocument();
					writer.writeName(value.getCommand());
					if (value.getShape() instanceof Placeholder p) { // maybe we should wrap input to use geo command object
						sjw.writePlaceholder(p.toString());
					}
					writer.writeEndDocument();
				} else {
					if (value.getShape() instanceof Placeholder p) { // maybe we should wrap input to use geo command object
						sjw.writePlaceholder(p.toString());
					}
				}
			} else {
				writer.writeString(value.getCommand(), value.getShape().toString());
			}
		}

		@Override
		public Class<GeoCommand> getEncoderClass() {
			return GeoCommand.class;
		}

	}

}
