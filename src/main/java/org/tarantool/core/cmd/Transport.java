package org.tarantool.core.cmd;

import java.io.Closeable;

/**
 * <p>Transport interface.</p>
 *
 * @author dgreen
 * @version $Id: $
 */
public interface Transport extends Closeable {

	/**
	 * <p>execute.</p>
	 *
	 * @param request a {@link org.tarantool.core.cmd.Request} object.
	 * @return a {@link org.tarantool.core.cmd.Response} object.
	 */
	Response execute(Request request);

}
