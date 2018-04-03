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
package org.springframework.data.mongodb;

import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.ResourceHolderSupport;

import com.mongodb.client.ClientSession;

/**
 * MongoDB specific {@link ResourceHolderSupport resource holder}, wrapping a {@link ClientSession}.
 * {@link MongoTransactionManager} binds instances of this class to the thread.
 * <p />
 * <strong>Note:</strong> Intended for internal usage only.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @see MongoTransactionManager
 * @see org.springframework.data.mongodb.core.MongoTemplate
 */
class MongoResourceHolder extends ResourceHolderSupport {

	private @Nullable ClientSession session;
	private MongoDbFactory dbFactory;

	/**
	 * Create a new {@link MongoResourceHolder} for a given {@link ClientSession session}.
	 *
	 * @param session the associated {@link ClientSession}. Can be {@literal null}.
	 * @param dbFactory the associated {@link MongoDbFactory}. must not be {@literal null}.
	 */
	MongoResourceHolder(@Nullable ClientSession session, MongoDbFactory dbFactory) {

		this.session = session;
		this.dbFactory = dbFactory;
	}

	/**
	 * @return the associated {@link ClientSession}. Can be {@literal null}.
	 */
	@Nullable
	ClientSession getSession() {
		return session;
	}

	/**
	 * @return the associated {@link MongoDbFactory}.
	 */
	public MongoDbFactory getDbFactory() {
		return dbFactory;
	}

	/**
	 * Set the {@link ClientSession} to guard.
	 *
	 * @param session can be {@literal null}.
	 */
	public void setSession(@Nullable ClientSession session) {
		this.session = session;
	}

	/**
	 * Only set the timeout if it does not match the {@link TransactionDefinition#TIMEOUT_DEFAULT default timeout}.
	 *
	 * @param seconds
	 */
	void setTimeoutIfNotDefaulted(int seconds) {

		if (seconds != TransactionDefinition.TIMEOUT_DEFAULT) {
			setTimeoutInSeconds(seconds);
		}
	}

	/**
	 * @return {@literal true} if session is not {@literal null}.
	 */
	boolean hasSession() {
		return session != null;
	}

	/**
	 * @return {@literal true} if the session is active and has not been closed.
	 */
	boolean hasActiveSession() {

		if (!hasSession()) {
			return false;
		}

		return hasServerSession() && !getSession().getServerSession().isClosed();
	}

	/**
	 * @return {@literal true} if the {@link ClientSession} has a {@link com.mongodb.session.ServerSession} associated
	 *         that is accessible via {@link ClientSession#getServerSession()}.
	 */
	boolean hasServerSession() {

		try {
			return getSession().getServerSession() != null;
		} catch (IllegalStateException serverSessionClosed) {
			// ignore
		}

		return false;
	}

}
