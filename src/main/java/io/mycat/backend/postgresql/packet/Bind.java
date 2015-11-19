package io.mycat.backend.postgresql.packet;



//		Bind (F)
//		Byte1('B')
//		标识该信息是一个绑定命令。
//		
//		Int32
//		以字节记的消息内容的长度，包括长度本身。
//		
//		String
//		目标入口的名字（空字串则选取未命名的入口）。
//		
//		String
//		源准备好语句的名字（空字串则选取未命名的准备好语句）。
//		
//		Int16
//		
//			后面跟着的参数格式代码的数目（在下面的 C 中说明）。 这个数值可以是零，表示没有参数，或者是参数都使用缺省格式（文本）； 或者是一，这种情况下声明的格式代码应用于所有参数；或者它可以等于实际数目的参数。
//		
//		Int16[C]
//		参数格式代码。目前每个都必须是零（文本）或者一（二进制）。
//		
//		Int16
//		后面跟着的参数值的数目（可能为零）。这些必须和查询需要的参数个数匹配。
//		
//		然后，每个参数都会出现下面的字段对：
//		
//		Int32
//		参数值的长度，以字节记（这个长度并不包含长度本身）。可以为零。 一个特殊的情况是，-1 表示一个 NULL 参数值。在 NULL 的情况下， 后面不会跟着数值字节。
//		
//		Byten
//		参数值，格式是关联的格式代码标明的。n 是上面的长度。
//		
//		在最后一个参数之后，出现下面的字段：
//		
//		Int16
//		
//			后面跟着的结果字段格式代码数目（下面的 R 描述）。 这个数目可以是零表示没有结果字段，或者结果字段都使用缺省格式（文本）； 或者是一，这种情况下声明格式代码应用于所有结果字段（如果有的话）；或者它可以等于查询的结果字段的实际数目。
//		
//		Int16[R]
//		结果字段格式代码。目前每个必须是零（文本）或者一（二进制）。


public class Bind extends PostgreSQLPacket {

	public static class DataParameter {
		/**
		 * 字段值的长度，以字节记（这个长度不包括它自己）。 可以为零。一个特殊的情况是，-1 表示一个 NULL 的字段值。 在 NULL
		 * 的情况下就没有跟着数据字段。
		 */
		private int length;
		private byte[] data;

		private boolean isNull;

		/**
		 * @return the length
		 */
		public int getLength() {
			return length;
		}

		/**
		 * @param length the length to set
		 */
		public void setLength(int length) {
			this.length = length;
		}

		/**
		 * @return the data
		 */
		public byte[] getData() {
			return data;
		}

		/**
		 * @param data the data to set
		 */
		public void setData(byte[] data) {
			this.data = data;
		}

		/**
		 * @return the isNull
		 */
		public boolean isNull() {
			return isNull;
		}

		/**
		 * @param isNull the isNull to set
		 */
		public void setNull(boolean isNull) {
			this.isNull = isNull;
		}

	}

	private char marker;
	private int length;
	private String name;
	private String sql;
	private short parameterProtocolNumber;
	private DataProtocol[] parameterProtocol;
	private short parameterNumber;
	private DataParameter[] parameter;	
	private short resultNumber;	
	private DataProtocol[] resultProtocol;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the sql
	 */
	public String getSql() {
		return sql;
	}

	/**
	 * @return the parameterProtocolNumber
	 */
	public short getParameterProtocolNumber() {
		return parameterProtocolNumber;
	}

	/**
	 * @return the parameterProtocol
	 */
	public DataProtocol[] getParameterProtocol() {
		return parameterProtocol;
	}

	/**
	 * @return the parameterNumber
	 */
	public short getParameterNumber() {
		return parameterNumber;
	}

	/**
	 * @return the parameter
	 */
	public DataParameter[] getParameter() {
		return parameter;
	}

	/**
	 * @return the resultNumber
	 */
	public short getResultNumber() {
		return resultNumber;
	}

	/**
	 * @return the resultProtocol
	 */
	public DataProtocol[] getResultProtocol() {
		return resultProtocol;
	}

}
