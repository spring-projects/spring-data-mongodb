/**
 * Options for
 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.replaceOne/">replaceOne</a>.
 * <br />
 * Defaults to
 * <dl>
 * <dt>upsert</dt>
 * <dd>false</dd>
 * </dl>
 *
 * @author Jakub Zurawa
 */
package org.springframework.data.mongodb.core;

public class ReplaceOptions {
	private boolean upsert;

	private static final ReplaceOptions NONE = new ReplaceOptions() {

		private static final String ERROR_MSG = "ReplaceOptions.none() cannot be changed; Please use ReplaceOptions.options() instead";

		@Override
		public ReplaceOptions upsert() {
			throw new UnsupportedOperationException(ERROR_MSG);
		}
	};

	/**
	 * Static factory method to create a {@link ReplaceOptions} instance.
	 * <dl>
	 * <dt>upsert</dt>
	 * <dd>false</dd>
	 * </dl>
	 *
	 * @return new instance of {@link ReplaceOptions}.
	 */
	public static ReplaceOptions options() {
		return new ReplaceOptions();
	}

	/**
	 * Static factory method returning an unmodifiable {@link ReplaceOptions} instance.
	 *
	 * @return unmodifiable {@link ReplaceOptions} instance.
	 * @since 2.2
	 */
	public static ReplaceOptions none() {
		return NONE;
	}

	/**
	 * Static factory method to create a {@link ReplaceOptions} instance with
	 * <dl>
	 * <dt>upsert</dt>
	 * <dd>false</dd>
	 * </dl>
	 *
	 * @return new instance of {@link ReplaceOptions}.
	 */
	public static ReplaceOptions empty() {
		return new ReplaceOptions();
	}

	/**
	 * Insert a new document if not exists.
	 *
	 * @return this.
	 */
	public ReplaceOptions upsert() {

		this.upsert = true;
		return this;
	}

	/**
	 * Get the bit indicating if to create a new document if not exists.
	 *
	 * @return {@literal true} if set.
	 */
	public boolean isUpsert() {
		return upsert;
	}

}
