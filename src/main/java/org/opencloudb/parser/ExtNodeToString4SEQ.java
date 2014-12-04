package org.opencloudb.parser;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.sequence.handler.IncrSequenceMySQLHandler;
import org.opencloudb.sequence.handler.IncrSequencePropHandler;
import org.opencloudb.sequence.handler.SequenceHandler;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.NextSequenceNode;
import com.foundationdb.sql.unparser.NodeToString;

/**
 * extends NodeToString support 自定义序列号
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @create Mar 3, 2014 11:03:25 AM
 * @version 1.0
 * @logs <table cellPadding="1" cellSpacing="1" width="300">
 *       <thead style="font-weight:bold;background-color:#2FABE9">
 *       <tr>
 *       <td>Date</td>
 *       <td>Author</td>
 *       <td>Version</td>
 *       <td>Comments</td>
 *       </tr>
 *       </thead> <tbody style="background-color:#b5cfd2">
 *       <tr>
 *       <td>Mar 3, 2014</td>
 *       <td><a href="http://www.micmiu.com">Michael</a></td>
 *       <td>1.0</td>
 *       <td>Create</td>
 *       </tr>
 *       </tbody>
 *       </table>
 */
public class ExtNodeToString4SEQ extends NodeToString {
	private final SequenceHandler sequenceHandler;

	public ExtNodeToString4SEQ(int seqHandlerType) {
		super();
		switch(seqHandlerType)
		{
		case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
			sequenceHandler = IncrSequenceMySQLHandler.getInstance();
			break;
		case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
			sequenceHandler = IncrSequencePropHandler.getInstance();
			break;
			default:
				throw new java.lang.IllegalArgumentException("Invalid sequnce handler type "+seqHandlerType);
		}
	}

	protected String nextSequenceNode(NextSequenceNode node)
			throws StandardException {
		// michael@micmiu 全局ID处理
		String tableName = node.getSequenceName().getTableName();
		if (null != tableName
				&& tableName.toUpperCase().startsWith("MYCATSEQ_")) {
			String prefixName = tableName.split("_", 2)[1];
			return sequenceHandler.nextId(prefixName.toUpperCase()) + "";
		}
		return "NEXT VALUE FOR " + toString(node.getSequenceName());
	}

}
