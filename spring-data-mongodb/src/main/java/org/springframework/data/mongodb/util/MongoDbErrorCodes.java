/*
 * Copyright 2015-2021 the original author or authors.
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

import java.util.HashMap;

import org.springframework.lang.Nullable;

import com.mongodb.MongoException;

/**
 * {@link MongoDbErrorCodes} holds MongoDB specific error codes outlined in {@literal mongo/base/error_codes.err}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public final class MongoDbErrorCodes {

	static HashMap<Integer, String> dataAccessResourceFailureCodes;
	static HashMap<Integer, String> dataIntegrityViolationCodes;
	static HashMap<Integer, String> duplicateKeyCodes;
	static HashMap<Integer, String> invalidDataAccessApiUsageExeption;
	static HashMap<Integer, String> permissionDeniedCodes;
	static HashMap<Integer, String> clientSessionCodes;

	static HashMap<Integer, String> errorCodes;

	static {

		dataAccessResourceFailureCodes = new HashMap<>(12, 1f);
		dataAccessResourceFailureCodes.put(6, "HostUnreachable");
		dataAccessResourceFailureCodes.put(7, "HostNotFound");
		dataAccessResourceFailureCodes.put(89, "NetworkTimeout");
		dataAccessResourceFailureCodes.put(91, "ShutdownInProgress");
		dataAccessResourceFailureCodes.put(12000, "SlaveDelayDifferential");
		dataAccessResourceFailureCodes.put(10084, "CannotFindMapFile64Bit");
		dataAccessResourceFailureCodes.put(10085, "CannotFindMapFile");
		dataAccessResourceFailureCodes.put(10357, "ShutdownInProgress");
		dataAccessResourceFailureCodes.put(10359, "Header==0");
		dataAccessResourceFailureCodes.put(13440, "BadOffsetInFile");
		dataAccessResourceFailureCodes.put(13441, "BadOffsetInFile");
		dataAccessResourceFailureCodes.put(13640, "DataFileHeaderCorrupt");

		dataIntegrityViolationCodes = new HashMap<>(6, 1f);
		dataIntegrityViolationCodes.put(67, "CannotCreateIndex");
		dataIntegrityViolationCodes.put(68, "IndexAlreadyExists");
		dataIntegrityViolationCodes.put(85, "IndexOptionsConflict");
		dataIntegrityViolationCodes.put(86, "IndexKeySpecsConflict");
		dataIntegrityViolationCodes.put(112, "WriteConflict");
		dataIntegrityViolationCodes.put(117, "ConflictingOperationInProgress");

		duplicateKeyCodes = new HashMap<>(4, 1f);
		duplicateKeyCodes.put(3, "OBSOLETE_DuplicateKey");
		duplicateKeyCodes.put(84, "DuplicateKeyValue");
		duplicateKeyCodes.put(11000, "DuplicateKey");
		duplicateKeyCodes.put(11001, "DuplicateKey");

		invalidDataAccessApiUsageExeption = new HashMap<>(31, 1f);
		invalidDataAccessApiUsageExeption.put(5, "GraphContainsCycle");
		invalidDataAccessApiUsageExeption.put(9, "FailedToParse");
		invalidDataAccessApiUsageExeption.put(14, "TypeMismatch");
		invalidDataAccessApiUsageExeption.put(15, "Overflow");
		invalidDataAccessApiUsageExeption.put(16, "InvalidLength");
		invalidDataAccessApiUsageExeption.put(20, "IllegalOperation");
		invalidDataAccessApiUsageExeption.put(21, "EmptyArrayOperation");
		invalidDataAccessApiUsageExeption.put(22, "InvalidBSON");
		invalidDataAccessApiUsageExeption.put(23, "AlreadyInitialized");
		invalidDataAccessApiUsageExeption.put(29, "NonExistentPath");
		invalidDataAccessApiUsageExeption.put(30, "InvalidPath");
		invalidDataAccessApiUsageExeption.put(40, "ConflictingUpdateOperators");
		invalidDataAccessApiUsageExeption.put(45, "UserDataInconsistent");
		invalidDataAccessApiUsageExeption.put(52, "DollarPrefixedFieldName");
		invalidDataAccessApiUsageExeption.put(53, "InvalidIdField");
		invalidDataAccessApiUsageExeption.put(54, "NotSingleValueField");
		invalidDataAccessApiUsageExeption.put(55, "InvalidDBRef");
		invalidDataAccessApiUsageExeption.put(56, "EmptyFieldName");
		invalidDataAccessApiUsageExeption.put(57, "DottedFieldName");
		invalidDataAccessApiUsageExeption.put(59, "CommandNotFound");
		invalidDataAccessApiUsageExeption.put(60, "DatabaseNotFound");
		invalidDataAccessApiUsageExeption.put(61, "ShardKeyNotFound");
		invalidDataAccessApiUsageExeption.put(62, "OplogOperationUnsupported");
		invalidDataAccessApiUsageExeption.put(66, "ImmutableField");
		invalidDataAccessApiUsageExeption.put(72, "InvalidOptions");
		invalidDataAccessApiUsageExeption.put(115, "CommandNotSupported");
		invalidDataAccessApiUsageExeption.put(116, "DocTooLargeForCapped");
		invalidDataAccessApiUsageExeption.put(10003, "CannotGrowDocumentInCappedNamespace");
		invalidDataAccessApiUsageExeption.put(130, "SymbolNotFound");
		invalidDataAccessApiUsageExeption.put(17280, "KeyTooLong");
		invalidDataAccessApiUsageExeption.put(13334, "ShardKeyTooBig");

		permissionDeniedCodes = new HashMap<>(8, 1f);
		permissionDeniedCodes.put(11, "UserNotFound");
		permissionDeniedCodes.put(18, "AuthenticationFailed");
		permissionDeniedCodes.put(31, "RoleNotFound");
		permissionDeniedCodes.put(32, "RolesNotRelated");
		permissionDeniedCodes.put(33, "PrivilegeNotFound");
		permissionDeniedCodes.put(15847, "CannotAuthenticate");
		permissionDeniedCodes.put(16704, "CannotAuthenticateToAdminDB");
		permissionDeniedCodes.put(16705, "CannotAuthenticateToAdminDB");

		clientSessionCodes = new HashMap<>(4, 1f);
		clientSessionCodes.put(206, "NoSuchSession");
		clientSessionCodes.put(213, "DuplicateSession");
		clientSessionCodes.put(217, "IncompleteTransactionHistory");
		clientSessionCodes.put(225, "TransactionTooOld");
		clientSessionCodes.put(228, "SessionTransferIncomplete");
		clientSessionCodes.put(244, "TransactionAborted");
		clientSessionCodes.put(251, "NoSuchTransaction");
		clientSessionCodes.put(256, "TransactionCommitted");
		clientSessionCodes.put(257, "TransactionToLarge");
		clientSessionCodes.put(261, "TooManyLogicalSessions");
		clientSessionCodes.put(263, "OperationNotSupportedInTransaction");
		clientSessionCodes.put(264, "TooManyLogicalSessions");
		clientSessionCodes.put(267, "PreparedTransactionInProgress");
		clientSessionCodes.put(290, "TransactionExceededLifetimeLimitSeconds");

		errorCodes = new HashMap<>();
		errorCodes.putAll(dataAccessResourceFailureCodes);
		errorCodes.putAll(dataIntegrityViolationCodes);
		errorCodes.putAll(duplicateKeyCodes);
		errorCodes.putAll(invalidDataAccessApiUsageExeption);
		errorCodes.putAll(permissionDeniedCodes);
		errorCodes.putAll(clientSessionCodes);
	}

	public static boolean isDataIntegrityViolationCode(@Nullable Integer errorCode) {
		return errorCode == null ? false : dataIntegrityViolationCodes.containsKey(errorCode);
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isDataIntegrityViolationError(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isDataIntegrityViolationCode(((MongoException) exception).getCode());
		}
		return false;
	}

	public static boolean isDataAccessResourceFailureCode(@Nullable Integer errorCode) {
		return errorCode == null ? false : dataAccessResourceFailureCodes.containsKey(errorCode);
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isDataAccessResourceError(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isDataAccessResourceFailureCode(((MongoException) exception).getCode());
		}
		return false;
	}

	public static boolean isDuplicateKeyCode(@Nullable Integer errorCode) {
		return errorCode == null ? false : duplicateKeyCodes.containsKey(errorCode);
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isDuplicateKeyError(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isDuplicateKeyCode(((MongoException) exception).getCode());
		}
		return false;
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isDataDuplicateKeyError(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isDuplicateKeyCode(((MongoException) exception).getCode());
		}
		return false;
	}

	public static boolean isPermissionDeniedCode(@Nullable Integer errorCode) {
		return errorCode == null ? false : permissionDeniedCodes.containsKey(errorCode);
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isPermissionDeniedError(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isPermissionDeniedCode(((MongoException) exception).getCode());
		}
		return false;
	}

	public static boolean isInvalidDataAccessApiUsageCode(@Nullable Integer errorCode) {
		return errorCode == null ? false : invalidDataAccessApiUsageExeption.containsKey(errorCode);
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isInvalidDataAccessApiUsageError(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isInvalidDataAccessApiUsageCode(((MongoException) exception).getCode());
		}
		return false;
	}

	public static String getErrorDescription(@Nullable Integer errorCode) {
		return errorCode == null ? null : errorCodes.get(errorCode);
	}

	/**
	 * Check if the given error code matches a know session related error.
	 *
	 * @param errorCode the error code to check.
	 * @return {@literal true} if error matches.
	 * @since 2.1
	 */
	public static boolean isClientSessionFailureCode(@Nullable Integer errorCode) {
		return errorCode == null ? false : clientSessionCodes.containsKey(errorCode);
	}

	/**
	 * @param exception can be {@literal null}.
	 * @return
	 * @since 3.3
	 */
	public static boolean isClientSessionFailure(@Nullable Exception exception) {

		if(exception instanceof MongoException) {
			return isClientSessionFailureCode(((MongoException) exception).getCode());
		}
		return false;
	}
}
