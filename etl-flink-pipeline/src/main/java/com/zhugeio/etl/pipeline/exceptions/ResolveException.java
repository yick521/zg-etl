package com.zhugeio.etl.pipeline.exceptions;

import java.io.Serializable;

public class ResolveException extends RuntimeException implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6600289192395352928L;

	public ResolveException() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ResolveException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public ResolveException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public ResolveException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public ResolveException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	
}
