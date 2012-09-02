package org.tarantool.core.exception;

/**
 * <p>
 * CommunicationException class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class CommunicationException extends RuntimeException {

	/**
	 * <p>
	 * Constructor for CommunicationException.
	 * </p>
	 * 
	 * @param message
	 *            a {@link java.lang.String} object.
	 * @param cause
	 *            a {@link java.lang.Throwable} object.
	 */
	public CommunicationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * <p>
	 * Constructor for CommunicationException.
	 * </p>
	 * 
	 * @param message
	 *            a {@link java.lang.String} object.
	 */
	public CommunicationException(String message) {
		super(message);
	}

	private static final long serialVersionUID = 1L;

}
