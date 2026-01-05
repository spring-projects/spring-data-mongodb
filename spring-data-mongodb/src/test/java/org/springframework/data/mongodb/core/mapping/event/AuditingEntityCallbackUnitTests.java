/*
 * Copyright 2019-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.Ordered;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.Unwrapped;

/**
 * Unit tests for {@link AuditingEntityCallback}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class AuditingEntityCallbackUnitTests {

	private final MongoMappingContext mappingContext = new MongoMappingContext();

	private IsNewAwareAuditingHandler handler;
	private AuditingEntityCallback callback;

	@BeforeEach
	void setUp() {

		mappingContext.getPersistentEntity(Sample.class);

		handler = spy(new IsNewAwareAuditingHandler(new PersistentEntities(Arrays.asList(mappingContext))));

		callback = new AuditingEntityCallback(() -> handler);
	}

	@Test // DATAMONGO-2261
	void rejectsNullAuditingHandler() {
		assertThatIllegalArgumentException().isThrownBy(() -> new AuditingEntityCallback(null));
	}

	@Test // DATAMONGO-2261
	void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		callback.onBeforeConvert(sample, "foo");

		verify(handler, times(1)).markCreated(sample);
		verify(handler, times(0)).markModified(any());
	}

	@Test // DATAMONGO-2261
	void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		callback.onBeforeConvert(sample, "foo");

		verify(handler, times(0)).markCreated(any());
		verify(handler, times(1)).markModified(sample);
	}

	@Test // DATAMONGO-2261
	void hasExplicitOrder() {

		assertThat(callback).isInstanceOf(Ordered.class);
		assertThat(callback.getOrder()).isEqualTo(100);
	}

	@Test // DATAMONGO-2261
	void propagatesChangedInstanceToEvent() {

		ImmutableSample sample = new ImmutableSample();

		ImmutableSample newSample = new ImmutableSample();
		IsNewAwareAuditingHandler handler = mock(IsNewAwareAuditingHandler.class);
		doReturn(newSample).when(handler).markAudited(eq(sample));

		AuditingEntityCallback listener = new AuditingEntityCallback(() -> handler);
		Object result = listener.onBeforeConvert(sample, "foo");

		assertThat(result).isSameAs(newSample);
	}

	@Test // GH-4732
	void shouldApplyAuditingToUnwrappedImmutableObject() {

		WithUnwrapped sample = new WithUnwrapped();
		sample.auditingData = new MyAuditingData(null, null);

		IsNewAwareAuditingHandler handler = new IsNewAwareAuditingHandler(PersistentEntities.of(mappingContext));

		AuditingEntityCallback listener = new AuditingEntityCallback(() -> handler);
		WithUnwrapped result = (WithUnwrapped) listener.onBeforeConvert(sample, "foo");

		assertThat(result.auditingData.created).isNotNull();
		assertThat(result.auditingData.modified).isNotNull();
	}

	static class Sample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}

	static class WithUnwrapped {

		@Id String id;

		@Unwrapped(onEmpty = Unwrapped.OnEmpty.USE_NULL) MyAuditingData auditingData;

	}

	record MyAuditingData(@CreatedDate Date created, @LastModifiedDate Date modified) {

	}

	private static final class ImmutableSample {

		@Id private final String id;
		@CreatedDate private final Date created;
		@LastModifiedDate private final Date modified;

		public ImmutableSample() {
			this(null, null, null);
		}

		public ImmutableSample(String id, Date created, Date modified) {
			this.id = id;
			this.created = created;
			this.modified = modified;
		}

		public String getId() {
			return this.id;
		}

		public Date getCreated() {
			return this.created;
		}

		public Date getModified() {
			return this.modified;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ImmutableSample that = (ImmutableSample) o;
			return Objects.equals(id, that.id) && Objects.equals(created, that.created)
					&& Objects.equals(modified, that.modified);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, created, modified);
		}

		public String toString() {
			return "AuditingEntityCallbackUnitTests.ImmutableSample(id=" + this.getId() + ", created=" + this.getCreated()
					+ ", modified=" + this.getModified() + ")";
		}

		public ImmutableSample withId(String id) {
			return this.id == id ? this : new ImmutableSample(id, this.created, this.modified);
		}

		public ImmutableSample withCreated(Date created) {
			return this.created == created ? this : new ImmutableSample(this.id, created, this.modified);
		}

		public ImmutableSample withModified(Date modified) {
			return this.modified == modified ? this : new ImmutableSample(this.id, this.created, modified);
		}
	}
}
