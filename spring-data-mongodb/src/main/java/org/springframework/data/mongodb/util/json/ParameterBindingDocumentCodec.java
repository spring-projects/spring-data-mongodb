/*
 * Copyright 2008-2020 the original author or authors.
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
package org.springframework.data.mongodb.util.json;

import static java.util.Arrays.*;
import static org.bson.assertions.Assertions.*;
import static org.bson.codecs.configuration.CodecRegistries.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.bson.AbstractBsonReader.State;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.Transformer;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonParseException;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Codec} implementation that allows binding parameters to placeholders or SpEL expressions when decoding a
 * JSON String. <br />
 * Modified version of <a href=
 * "https://github.com/mongodb/mongo-java-driver/blob/master/bson/src/main/org/bson/codecs/DocumentCodec.java">MongoDB
 * Inc. DocumentCodec</a> licensed under the Apache License, Version 2.0. <br />
 *
 * @author Jeff Yemin
 * @author Ross Lawley
 * @author Ralph Schaer
 * @author Christoph Strobl
 * @since 2.2
 */
public class ParameterBindingDocumentCodec implements CollectibleCodec<Document> {

	private static final String ID_FIELD_NAME = "_id";
	private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(
			asList(new ValueCodecProvider(), new BsonValueCodecProvider(), new DocumentCodecProvider()));
	private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();

	private final BsonTypeCodecMap bsonTypeCodecMap;
	private final CodecRegistry registry;
	private final IdGenerator idGenerator;
	private final Transformer valueTransformer;

	/**
	 * Construct a new instance with a default {@code CodecRegistry}.
	 */
	public ParameterBindingDocumentCodec() {
		this(DEFAULT_REGISTRY);
	}

	/**
	 * Construct a new instance with the given registry.
	 *
	 * @param registry the registry
	 */
	public ParameterBindingDocumentCodec(final CodecRegistry registry) {
		this(registry, DEFAULT_BSON_TYPE_CLASS_MAP);
	}

	/**
	 * Construct a new instance with the given registry and BSON type class map.
	 *
	 * @param registry the registry
	 * @param bsonTypeClassMap the BSON type class map
	 */
	public ParameterBindingDocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
		this(registry, bsonTypeClassMap, null);
	}

	/**
	 * Construct a new instance with the given registry and BSON type class map. The transformer is applied as a last step
	 * when decoding values, which allows users of this codec to control the decoding process. For example, a user of this
	 * class could substitute a value decoded as a Document with an instance of a special purpose class (e.g., one
	 * representing a DBRef in MongoDB).
	 *
	 * @param registry the registry
	 * @param bsonTypeClassMap the BSON type class map
	 * @param valueTransformer the value transformer to use as a final step when decoding the value of any field in the
	 *          document
	 */
	public ParameterBindingDocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap,
			final Transformer valueTransformer) {
		this.registry = notNull("registry", registry);
		this.bsonTypeCodecMap = new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry);
		this.idGenerator = new ObjectIdGenerator();
		this.valueTransformer = valueTransformer != null ? valueTransformer : new Transformer() {
			@Override
			public Object transform(final Object value) {
				return value;
			}
		};
	}

	@Override
	public boolean documentHasId(final Document document) {
		return document.containsKey(ID_FIELD_NAME);
	}

	@Override
	public BsonValue getDocumentId(final Document document) {
		if (!documentHasId(document)) {
			throw new IllegalStateException("The document does not contain an _id");
		}

		Object id = document.get(ID_FIELD_NAME);
		if (id instanceof BsonValue) {
			return (BsonValue) id;
		}

		BsonDocument idHoldingDocument = new BsonDocument();
		BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
		writer.writeStartDocument();
		writer.writeName(ID_FIELD_NAME);
		writeValue(writer, EncoderContext.builder().build(), id);
		writer.writeEndDocument();
		return idHoldingDocument.get(ID_FIELD_NAME);
	}

	@Override
	public Document generateIdIfAbsentFromDocument(final Document document) {
		if (!documentHasId(document)) {
			document.put(ID_FIELD_NAME, idGenerator.generate());
		}
		return document;
	}

	@Override
	public void encode(final BsonWriter writer, final Document document, final EncoderContext encoderContext) {
		writeMap(writer, document, encoderContext);
	}

	// Spring Data Customization START
	public Document decode(@Nullable String json, Object[] values) {

		return decode(json, new ParameterBindingContext((index) -> values[index], new SpelExpressionParser(),
				EvaluationContextProvider.DEFAULT.getEvaluationContext(values)));
	}

	public Document decode(@Nullable String json, ParameterBindingContext bindingContext) {

		if (StringUtils.isEmpty(json)) {
			return new Document();
		}

		ParameterBindingJsonReader reader = new ParameterBindingJsonReader(json, bindingContext);
		return this.decode(reader, DecoderContext.builder().build());
	}

	/**
	 * Determine {@link ExpressionDependencies} from Expressions that are nested in the {@code json} content. Returns
	 * {@link Optional#empty()} if {@code json} is empty or of it does not contain any SpEL expressions.
	 *
	 * @param json
	 * @param expressionParser
	 * @return a {@link Optional} containing merged {@link ExpressionDependencies} object if expressions were found,
	 *         otherwise {@link Optional#empty()}.
	 * @since 3.1
	 */
	public Optional<ExpressionDependencies> getExpressionDependencies(@Nullable String json, ValueProvider valueProvider,
			ExpressionParser expressionParser) {

		if (StringUtils.isEmpty(json)) {
			return Optional.empty();
		}

		List<ExpressionDependencies> dependencies = new ArrayList<>();

		ParameterBindingContext context = new ParameterBindingContext(valueProvider, new SpELExpressionEvaluator() {
			@Override
			public <T> T evaluate(String expression) {

				dependencies.add(ExpressionDependencies.discover(expressionParser.parseExpression(expression)));

				return (T) new Object();
			}
		});

		ParameterBindingJsonReader reader = new ParameterBindingJsonReader(json, context);
		this.decode(reader, DecoderContext.builder().build());

		if (dependencies.isEmpty()) {
			return Optional.empty();
		}

		ExpressionDependencies result = ExpressionDependencies.empty();

		for (ExpressionDependencies dependency : dependencies) {
			result = result.mergeWith(dependency);
		}

		return Optional.of(result);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Document decode(final BsonReader reader, final DecoderContext decoderContext) {

		if (reader instanceof ParameterBindingJsonReader) {
			ParameterBindingJsonReader bindingReader = (ParameterBindingJsonReader) reader;

			// check if the reader has actually found something to replace on top level and did so.
			// binds just placeholder queries like: `@Query(?0)`
			if (bindingReader.currentValue instanceof org.bson.Document) {
				return (Document) bindingReader.currentValue;
			}
		}

		Document document = new Document();
		reader.readStartDocument();

		try {

			while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
				String fieldName = reader.readName();
				Object value = readValue(reader, decoderContext);
				document.put(fieldName, value);
			}
		} catch (JsonParseException e) {
			try {

				Object value = readValue(reader, decoderContext);
				if (value instanceof Map<?, ?>) {
					if (!((Map) value).isEmpty()) {
						return new Document((Map<String, Object>) value);
					}
				}
			} catch (Exception ex) {
				e.addSuppressed(ex);
				throw e;
			}
		}

		reader.readEndDocument();

		return document;
	}

	// Spring Data Customization END

	@Override
	public Class<Document> getEncoderClass() {
		return Document.class;
	}

	private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext,
			final Map<String, Object> document) {
		if (encoderContext.isEncodingCollectibleDocument() && document.containsKey(ID_FIELD_NAME)) {
			bsonWriter.writeName(ID_FIELD_NAME);
			writeValue(bsonWriter, encoderContext, document.get(ID_FIELD_NAME));
		}
	}

	private boolean skipField(final EncoderContext encoderContext, final String key) {
		return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void writeValue(final BsonWriter writer, final EncoderContext encoderContext, final Object value) {
		if (value == null) {
			writer.writeNull();
		} else if (value instanceof Iterable) {
			writeIterable(writer, (Iterable<Object>) value, encoderContext.getChildContext());
		} else if (value instanceof Map) {
			writeMap(writer, (Map<String, Object>) value, encoderContext.getChildContext());
		} else {
			Codec codec = registry.get(value.getClass());
			encoderContext.encodeWithChildContext(codec, writer, value);
		}
	}

	private void writeMap(final BsonWriter writer, final Map<String, Object> map, final EncoderContext encoderContext) {
		writer.writeStartDocument();

		beforeFields(writer, encoderContext, map);

		for (final Map.Entry<String, Object> entry : map.entrySet()) {
			if (skipField(encoderContext, entry.getKey())) {
				continue;
			}
			writer.writeName(entry.getKey());
			writeValue(writer, encoderContext, entry.getValue());
		}
		writer.writeEndDocument();
	}

	private void writeIterable(final BsonWriter writer, final Iterable<Object> list,
			final EncoderContext encoderContext) {
		writer.writeStartArray();
		for (final Object value : list) {
			writeValue(writer, encoderContext, value);
		}
		writer.writeEndArray();
	}

	private Object readValue(final BsonReader reader, final DecoderContext decoderContext) {

		// Spring Data Customization START
		if (reader instanceof ParameterBindingJsonReader) {

			ParameterBindingJsonReader bindingReader = (ParameterBindingJsonReader) reader;

			// check if the reader has actually found something to replace and did so.
			// resets the reader state to move on after the actual value
			// returns the replacement value
			if (bindingReader.currentValue != null) {

				Object value = bindingReader.currentValue;

				if (ObjectUtils.nullSafeEquals(BsonType.DATE_TIME, bindingReader.getCurrentBsonType())
						&& !(value instanceof Date)) {

					if (value instanceof Number) {
						value = new Date(NumberUtils.convertNumberToTargetClass((Number) value, Long.class));
					} else if (value instanceof String) {
						value = new Date(DateTimeFormatter.parse((String) value));
					}
				}

				bindingReader.setState(State.TYPE);
				bindingReader.currentValue = null;
				return value;
			}
		}

		// Spring Data Customization END

		BsonType bsonType = reader.getCurrentBsonType();
		if (bsonType == BsonType.NULL) {
			reader.readNull();
			return null;
		} else if (bsonType == BsonType.ARRAY) {
			return readList(reader, decoderContext);
		} else if (bsonType == BsonType.BINARY && BsonBinarySubType.isUuid(reader.peekBinarySubType())
				&& reader.peekBinarySize() == 16) {
			return registry.get(UUID.class).decode(reader, decoderContext);
		}

		// Spring Data Customization START
		// By default the registry uses DocumentCodec for parsing.
		// We need to reroute that to our very own implementation or we'll end up only mapping half the placeholders.
		Codec<?> codecToUse = bsonTypeCodecMap.get(bsonType);
		if (codecToUse instanceof org.bson.codecs.DocumentCodec) {
			codecToUse = this;
		}

		return valueTransformer.transform(codecToUse.decode(reader, decoderContext));
		// Spring Data Customization END
	}

	private List<Object> readList(final BsonReader reader, final DecoderContext decoderContext) {
		reader.readStartArray();
		List<Object> list = new ArrayList<>();
		while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {

			// Spring Data Customization START
			Object listValue = readValue(reader, decoderContext);
			if (listValue instanceof Collection) {
				list.addAll((Collection) listValue);
				break;
			}
			list.add(listValue);
			// Spring Data Customization END
		}
		reader.readEndArray();
		return list;
	}

}
