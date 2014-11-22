package migratable.protocols;

import java.io.Serializable;

/**
 * Created by Derek on 9/6/2014.
 *
 * The structure of the response to a ChildManager to ProcessManager request.
 *
 */
public class ChildToProcessResponse implements Serializable {
    private boolean success;

    public ChildToProcessResponse(boolean success) {
	this.success = success;
    }

    public boolean getSuccess() {
	return success;
    }
}
