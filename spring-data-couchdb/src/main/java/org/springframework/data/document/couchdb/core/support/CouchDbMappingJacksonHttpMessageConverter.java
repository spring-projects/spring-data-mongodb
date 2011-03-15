/*
 * Copyright 2011 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.couchdb.core.support;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.util.Assert;

public class CouchDbMappingJacksonHttpMessageConverter extends
    AbstractHttpMessageConverter<Object> {

  public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  private static final String ROWS_FIELD_NAME = "rows";
  private static final String VALUE_FIELD_NAME = "value";
  private static final String INCLUDED_DOC_FIELD_NAME = "doc";
  private static final String TOTAL_ROWS_FIELD_NAME = "total_rows";

  private ObjectMapper objectMapper = new ObjectMapper();

  private boolean prefixJson = false;

  /**
   * Construct a new {@code BindingJacksonHttpMessageConverter}.
   */
  public CouchDbMappingJacksonHttpMessageConverter() {
    super(new MediaType("application", "json", DEFAULT_CHARSET));
  }

  /**
   * Sets the {@code ObjectMapper} for this view. If not set, a default
   * {@link ObjectMapper#ObjectMapper() ObjectMapper} is used.
   * <p/>
   * Setting a custom-configured {@code ObjectMapper} is one way to take
   * further control of the JSON serialization process. For example, an
   * extended {@link org.codehaus.jackson.map.SerializerFactory} can be
   * configured that provides custom serializers for specific types. The other
   * option for refining the serialization process is to use Jackson's
   * provided annotations on the types to be serialized, in which case a
   * custom-configured ObjectMapper is unnecessary.
   */
  public void setObjectMapper(ObjectMapper objectMapper) {
    Assert.notNull(objectMapper, "'objectMapper' must not be null");
    this.objectMapper = objectMapper;
  }

  /**
   * Indicates whether the JSON output by this view should be prefixed with
   * "{} &&". Default is false.
   * <p/>
   * Prefixing the JSON string in this manner is used to help prevent JSON
   * Hijacking. The prefix renders the string syntactically invalid as a
   * script so that it cannot be hijacked. This prefix does not affect the
   * evaluation of JSON, but if JSON validation is performed on the string,
   * the prefix would need to be ignored.
   */
  public void setPrefixJson(boolean prefixJson) {
    this.prefixJson = prefixJson;
  }

  @Override
  public boolean canRead(Class<?> clazz, MediaType mediaType) {
    JavaType javaType = getJavaType(clazz);
    return this.objectMapper.canDeserialize(javaType) && canRead(mediaType);
  }

  /**
   * Returns the Jackson {@link JavaType} for the specific class.
   * <p/>
   * <p/>
   * Default implementation returns
   * {@link TypeFactory#type(java.lang.reflect.Type)}, but this can be
   * overridden in subclasses, to allow for custom generic collection
   * handling. For instance:
   * <p/>
   * <pre class="code">
   * protected JavaType getJavaType(Class&lt;?&gt; clazz) {
   * if (List.class.isAssignableFrom(clazz)) {
   * return TypeFactory.collectionType(ArrayList.class, MyBean.class);
   * } else {
   * return super.getJavaType(clazz);
   * }
   * }
   * </pre>
   *
   * @param clazz the class to return the java type for
   * @return the java type
   */
  protected JavaType getJavaType(Class<?> clazz) {
    return TypeFactory.type(clazz);
  }

  @Override
  public boolean canWrite(Class<?> clazz, MediaType mediaType) {
    return this.objectMapper.canSerialize(clazz) && canWrite(mediaType);
  }

  @Override
  protected boolean supports(Class<?> clazz) {
    // should not be called, since we override canRead/Write instead
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object readInternal(Class<?> clazz, HttpInputMessage inputMessage)
      throws IOException, HttpMessageNotReadableException {
    JavaType javaType = getJavaType(clazz);
    try {
      return success(clazz, inputMessage);

      // return this.objectMapper.readValue(inputMessage.getBody(),
      // javaType);
    } catch (Exception ex) {
      throw new HttpMessageNotReadableException("Could not read JSON: "
          + ex.getMessage(), ex);
    }
  }

  private Object success(Class<?> clazz, HttpInputMessage inputMessage)
      throws JsonParseException, IOException {

    //Note, parsing code used from ektorp project
    JsonParser jp = objectMapper.getJsonFactory().createJsonParser(
        inputMessage.getBody());
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      throw new RuntimeException("Expected data to start with an Object");
    }
    Map<String, Integer> fields = readHeaderFields(jp);

    List result;
    if (fields.containsKey(TOTAL_ROWS_FIELD_NAME)) {
      int totalRows = fields.get(TOTAL_ROWS_FIELD_NAME);
      if (totalRows == 0) {
        return Collections.emptyList();
      }
      result = new ArrayList(totalRows);
    } else {
      result = new ArrayList();
    }

    ParseState state = new ParseState();

    Object first = parseFirstRow(jp, state, clazz);
    if (first == null) {
      return Collections.emptyList();
    } else {
      result.add(first);
    }

    while (jp.getCurrentToken() != null) {
      skipToField(jp, state.docFieldName, state);
      if (atEndOfRows(jp)) {
        return result;
      }
      result.add(jp.readValueAs(clazz));
      endRow(jp, state);
    }
    return result;
  }

  private Object parseFirstRow(JsonParser jp, ParseState state, Class clazz)
      throws JsonParseException, IOException, JsonProcessingException,
      JsonMappingException {
    skipToField(jp, VALUE_FIELD_NAME, state);
    JsonNode value = null;
    if (atObjectStart(jp)) {
      value = jp.readValueAsTree();
      jp.nextToken();
      if (isEndOfRow(jp)) {
        state.docFieldName = VALUE_FIELD_NAME;
        Object doc = objectMapper.readValue(value, clazz);
        endRow(jp, state);
        return doc;
      }
    }
    skipToField(jp, INCLUDED_DOC_FIELD_NAME, state);
    if (atObjectStart(jp)) {
      state.docFieldName = INCLUDED_DOC_FIELD_NAME;
      Object doc = jp.readValueAs(clazz);
      endRow(jp, state);
      return doc;
    }
    return null;
  }


  private boolean isEndOfRow(JsonParser jp) {
    return jp.getCurrentToken() == JsonToken.END_OBJECT;
  }

  private void endRow(JsonParser jp, ParseState state) throws IOException, JsonParseException {
    state.inRow = false;
    jp.nextToken();
  }

  private boolean atObjectStart(JsonParser jp) {
    return jp.getCurrentToken() == JsonToken.START_OBJECT;
  }

  private boolean atEndOfRows(JsonParser jp) {
    return jp.getCurrentToken() != JsonToken.START_OBJECT;
  }

  private void skipToField(JsonParser jp, String fieldName, ParseState state) throws JsonParseException, IOException {
    String lastFieldName = null;
    while (jp.getCurrentToken() != null) {
      switch (jp.getCurrentToken()) {
        case FIELD_NAME:
          lastFieldName = jp.getCurrentName();
          jp.nextToken();
          break;
        case START_OBJECT:
          if (!state.inRow) {
            state.inRow = true;
            jp.nextToken();
          } else {
            if (isInField(fieldName, lastFieldName)) {
              return;
            } else {
              jp.skipChildren();
            }
          }
          break;
        default:
          if (isInField(fieldName, lastFieldName)) {
            jp.nextToken();
            return;
          }
          jp.nextToken();
          break;
      }
    }
  }

  private boolean isInField(String fieldName, String lastFieldName) {
    return lastFieldName != null && lastFieldName.equals(fieldName);
  }


  private Map<String, Integer> readHeaderFields(JsonParser jp)
      throws JsonParseException, IOException {
    Map<String, Integer> map = new HashMap<String, Integer>();
    jp.nextToken();
    String nextFieldName = jp.getCurrentName();
    while (!nextFieldName.equals(ROWS_FIELD_NAME)) {
      jp.nextToken();
      map.put(nextFieldName, Integer.valueOf(jp.getIntValue()));
      jp.nextToken();
      nextFieldName = jp.getCurrentName();
    }
    return map;
  }

  @Override
  protected void writeInternal(Object o, HttpOutputMessage outputMessage)
      throws IOException, HttpMessageNotWritableException {

    JsonEncoding encoding = getEncoding(outputMessage.getHeaders()
        .getContentType());
    JsonGenerator jsonGenerator = this.objectMapper.getJsonFactory()
        .createJsonGenerator(outputMessage.getBody(), encoding);
    try {
      if (this.prefixJson) {
        jsonGenerator.writeRaw("{} && ");
      }
      this.objectMapper.writeValue(jsonGenerator, o);
    } catch (JsonGenerationException ex) {
      throw new HttpMessageNotWritableException("Could not write JSON: "
          + ex.getMessage(), ex);
    }
  }

  private JsonEncoding getEncoding(MediaType contentType) {
    if (contentType != null && contentType.getCharSet() != null) {
      Charset charset = contentType.getCharSet();
      for (JsonEncoding encoding : JsonEncoding.values()) {
        if (charset.name().equals(encoding.getJavaName())) {
          return encoding;
        }
      }
    }
    return JsonEncoding.UTF8;
  }

  private static class ParseState {
    boolean inRow;
    String docFieldName = "";
  }
}
