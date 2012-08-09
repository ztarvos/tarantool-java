package org.tarantool.core.exception;

public class CommunicationException extends RuntimeException {

	public CommunicationException(String message, Throwable cause) {
		super(message, cause);
	}

	public CommunicationException(String message) {
		super(message);
	}

	private static final long serialVersionUID = 1L;

}
