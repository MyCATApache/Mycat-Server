package io.mycat.backend.jdbc.mongodb;

import java.sql.DriverPropertyInfo;
import java.util.ArrayList;


public class DriverPropertyInfoHelper{
	
	public static final String AUTO_CONNECT_RETRY = "autoConnectRetry";

	public static final String CONNECTIONS_PER_HOST = "connecionsPerHost";

	public static final String CONNECT_TIMEOUT = "connectTimeout";

	public static final String CURSOR_FINALIZER_ENABLED = "cursorFinalizerEnabled";

	public static final String MAX_AUTO_CONNECT_RETRY_TIME = "maxAutoConnectRetryTime";

	public static final String READ_PREFERENCE = "readPreference";

	public static final String SOCKET_TIMEOUT = "socketTimeout";

	public DriverPropertyInfo[] getPropertyInfo()
	{
		ArrayList<DriverPropertyInfo> propInfos = new ArrayList<DriverPropertyInfo>();

		addPropInfo(
			propInfos,
			AUTO_CONNECT_RETRY,
			"false",
			"If true, the driver will keep trying to connect to the same server in case that the socket "
				+ "cannot be established. There is maximum amount of time to keep retrying, which is 15s by "
				+ "default.", null);

		addPropInfo(propInfos, CONNECTIONS_PER_HOST, "10", "The maximum number of connections allowed per "
			+ "host for this Mongo instance. Those connections will be kept in a pool when idle.", null);

		addPropInfo(propInfos, CONNECT_TIMEOUT, "10000", "The connection timeout in milliseconds. ", null);

		addPropInfo(propInfos, CURSOR_FINALIZER_ENABLED, "true", "Sets whether there is a a finalize "
			+ "method created that cleans up instances of DBCursor that the client does not close.",
			null);

		addPropInfo(propInfos, MAX_AUTO_CONNECT_RETRY_TIME, "0",
			"The maximum amount of time in MS to spend retrying to open connection to the same server."
				+ "Default is 0, which means to use the default 15s if autoConnectRetry is on.", null);

		addPropInfo(propInfos, READ_PREFERENCE, "primary",
			"represents preferred replica set members to which a query or command can be sent", new String[] {
					"primary", "primary preferred", "secondary", "secondary preferred", "nearest" });

		addPropInfo(propInfos, SOCKET_TIMEOUT, "0", "The socket timeout in milliseconds It is used for "
			+ "I/O socket read and write operations "
			+ "Socket.setSoTimeout(int) Default is 0 and means no timeout.", null);

		return propInfos.toArray(new DriverPropertyInfo[propInfos.size()]);
	}

	private void addPropInfo(final ArrayList<DriverPropertyInfo> propInfos, final String propName,
		final String defaultVal, final String description, final String[] choices)
	{
		DriverPropertyInfo newProp = new DriverPropertyInfo(propName, defaultVal);
		newProp.description = description;
		if (choices != null)
		{
			newProp.choices = choices;
		}
		propInfos.add(newProp);
	}
}