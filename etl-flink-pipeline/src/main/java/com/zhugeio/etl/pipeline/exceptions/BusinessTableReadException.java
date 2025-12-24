package com.zhugeio.etl.pipeline.exceptions;

import java.io.Serializable;

/**
 * 读取业务表数据异常
 */
public class BusinessTableReadException extends RuntimeException implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -6600289192395352928L;

	public BusinessTableReadException() {
		super();
		// TODO Auto-generated constructor stub
	}

	public BusinessTableReadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public BusinessTableReadException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public BusinessTableReadException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public BusinessTableReadException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	
}
