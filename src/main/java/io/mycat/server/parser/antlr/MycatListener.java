// Generated from C:\Users\Coollf\Mycat.g4 by ANTLR 4.1
package io.mycat.server.parser.antlr;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link MycatParser}.
 */
public interface MycatListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link MycatParser#help_sql}.
	 * @param ctx the parse tree
	 */
	void enterHelp_sql(@NotNull MycatParser.Help_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#help_sql}.
	 * @param ctx the parse tree
	 */
	void exitHelp_sql(@NotNull MycatParser.Help_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#rollback_sql}.
	 * @param ctx the parse tree
	 */
	void enterRollback_sql(@NotNull MycatParser.Rollback_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#rollback_sql}.
	 * @param ctx the parse tree
	 */
	void exitRollback_sql(@NotNull MycatParser.Rollback_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#start_sql}.
	 * @param ctx the parse tree
	 */
	void enterStart_sql(@NotNull MycatParser.Start_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#start_sql}.
	 * @param ctx the parse tree
	 */
	void exitStart_sql(@NotNull MycatParser.Start_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#show_sql}.
	 * @param ctx the parse tree
	 */
	void enterShow_sql(@NotNull MycatParser.Show_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#show_sql}.
	 * @param ctx the parse tree
	 */
	void exitShow_sql(@NotNull MycatParser.Show_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#reload_msql}.
	 * @param ctx the parse tree
	 */
	void enterReload_msql(@NotNull MycatParser.Reload_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#reload_msql}.
	 * @param ctx the parse tree
	 */
	void exitReload_msql(@NotNull MycatParser.Reload_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#show_msql}.
	 * @param ctx the parse tree
	 */
	void enterShow_msql(@NotNull MycatParser.Show_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#show_msql}.
	 * @param ctx the parse tree
	 */
	void exitShow_msql(@NotNull MycatParser.Show_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#switch_msql}.
	 * @param ctx the parse tree
	 */
	void enterSwitch_msql(@NotNull MycatParser.Switch_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#switch_msql}.
	 * @param ctx the parse tree
	 */
	void exitSwitch_msql(@NotNull MycatParser.Switch_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#stop_msql}.
	 * @param ctx the parse tree
	 */
	void enterStop_msql(@NotNull MycatParser.Stop_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#stop_msql}.
	 * @param ctx the parse tree
	 */
	void exitStop_msql(@NotNull MycatParser.Stop_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#commit_sql}.
	 * @param ctx the parse tree
	 */
	void enterCommit_sql(@NotNull MycatParser.Commit_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#commit_sql}.
	 * @param ctx the parse tree
	 */
	void exitCommit_sql(@NotNull MycatParser.Commit_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#ddl_sql}.
	 * @param ctx the parse tree
	 */
	void enterDdl_sql(@NotNull MycatParser.Ddl_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#ddl_sql}.
	 * @param ctx the parse tree
	 */
	void exitDdl_sql(@NotNull MycatParser.Ddl_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#mycat}.
	 * @param ctx the parse tree
	 */
	void enterMycat(@NotNull MycatParser.MycatContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#mycat}.
	 * @param ctx the parse tree
	 */
	void exitMycat(@NotNull MycatParser.MycatContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#explain_sql}.
	 * @param ctx the parse tree
	 */
	void enterExplain_sql(@NotNull MycatParser.Explain_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#explain_sql}.
	 * @param ctx the parse tree
	 */
	void exitExplain_sql(@NotNull MycatParser.Explain_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#replace_sql}.
	 * @param ctx the parse tree
	 */
	void enterReplace_sql(@NotNull MycatParser.Replace_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#replace_sql}.
	 * @param ctx the parse tree
	 */
	void exitReplace_sql(@NotNull MycatParser.Replace_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#kill_msql}.
	 * @param ctx the parse tree
	 */
	void enterKill_msql(@NotNull MycatParser.Kill_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#kill_msql}.
	 * @param ctx the parse tree
	 */
	void exitKill_msql(@NotNull MycatParser.Kill_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#describe_sql}.
	 * @param ctx the parse tree
	 */
	void enterDescribe_sql(@NotNull MycatParser.Describe_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#describe_sql}.
	 * @param ctx the parse tree
	 */
	void exitDescribe_sql(@NotNull MycatParser.Describe_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#begin_sql}.
	 * @param ctx the parse tree
	 */
	void enterBegin_sql(@NotNull MycatParser.Begin_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#begin_sql}.
	 * @param ctx the parse tree
	 */
	void exitBegin_sql(@NotNull MycatParser.Begin_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#update_sql}.
	 * @param ctx the parse tree
	 */
	void enterUpdate_sql(@NotNull MycatParser.Update_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#update_sql}.
	 * @param ctx the parse tree
	 */
	void exitUpdate_sql(@NotNull MycatParser.Update_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#insert_sql}.
	 * @param ctx the parse tree
	 */
	void enterInsert_sql(@NotNull MycatParser.Insert_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#insert_sql}.
	 * @param ctx the parse tree
	 */
	void exitInsert_sql(@NotNull MycatParser.Insert_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#rollback_msql}.
	 * @param ctx the parse tree
	 */
	void enterRollback_msql(@NotNull MycatParser.Rollback_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#rollback_msql}.
	 * @param ctx the parse tree
	 */
	void exitRollback_msql(@NotNull MycatParser.Rollback_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#clear_msql}.
	 * @param ctx the parse tree
	 */
	void enterClear_msql(@NotNull MycatParser.Clear_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#clear_msql}.
	 * @param ctx the parse tree
	 */
	void exitClear_msql(@NotNull MycatParser.Clear_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#delete_sql}.
	 * @param ctx the parse tree
	 */
	void enterDelete_sql(@NotNull MycatParser.Delete_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#delete_sql}.
	 * @param ctx the parse tree
	 */
	void exitDelete_sql(@NotNull MycatParser.Delete_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#savepoint_sql}.
	 * @param ctx the parse tree
	 */
	void enterSavepoint_sql(@NotNull MycatParser.Savepoint_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#savepoint_sql}.
	 * @param ctx the parse tree
	 */
	void exitSavepoint_sql(@NotNull MycatParser.Savepoint_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#use_sql}.
	 * @param ctx the parse tree
	 */
	void enterUse_sql(@NotNull MycatParser.Use_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#use_sql}.
	 * @param ctx the parse tree
	 */
	void exitUse_sql(@NotNull MycatParser.Use_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#load_data_infile_sql}.
	 * @param ctx the parse tree
	 */
	void enterLoad_data_infile_sql(@NotNull MycatParser.Load_data_infile_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#load_data_infile_sql}.
	 * @param ctx the parse tree
	 */
	void exitLoad_data_infile_sql(@NotNull MycatParser.Load_data_infile_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#online_msql}.
	 * @param ctx the parse tree
	 */
	void enterOnline_msql(@NotNull MycatParser.Online_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#online_msql}.
	 * @param ctx the parse tree
	 */
	void exitOnline_msql(@NotNull MycatParser.Online_msqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#sql}.
	 * @param ctx the parse tree
	 */
	void enterSql(@NotNull MycatParser.SqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#sql}.
	 * @param ctx the parse tree
	 */
	void exitSql(@NotNull MycatParser.SqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#msql}.
	 * @param ctx the parse tree
	 */
	void enterMsql(@NotNull MycatParser.MsqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#msql}.
	 * @param ctx the parse tree
	 */
	void exitMsql(@NotNull MycatParser.MsqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#mysql_comment_sql}.
	 * @param ctx the parse tree
	 */
	void enterMysql_comment_sql(@NotNull MycatParser.Mysql_comment_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#mysql_comment_sql}.
	 * @param ctx the parse tree
	 */
	void exitMysql_comment_sql(@NotNull MycatParser.Mysql_comment_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#call_sql}.
	 * @param ctx the parse tree
	 */
	void enterCall_sql(@NotNull MycatParser.Call_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#call_sql}.
	 * @param ctx the parse tree
	 */
	void exitCall_sql(@NotNull MycatParser.Call_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#select_sql}.
	 * @param ctx the parse tree
	 */
	void enterSelect_sql(@NotNull MycatParser.Select_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#select_sql}.
	 * @param ctx the parse tree
	 */
	void exitSelect_sql(@NotNull MycatParser.Select_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#kill_query_sql}.
	 * @param ctx the parse tree
	 */
	void enterKill_query_sql(@NotNull MycatParser.Kill_query_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#kill_query_sql}.
	 * @param ctx the parse tree
	 */
	void exitKill_query_sql(@NotNull MycatParser.Kill_query_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#kill_sql}.
	 * @param ctx the parse tree
	 */
	void enterKill_sql(@NotNull MycatParser.Kill_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#kill_sql}.
	 * @param ctx the parse tree
	 */
	void exitKill_sql(@NotNull MycatParser.Kill_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#set_sql}.
	 * @param ctx the parse tree
	 */
	void enterSet_sql(@NotNull MycatParser.Set_sqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#set_sql}.
	 * @param ctx the parse tree
	 */
	void exitSet_sql(@NotNull MycatParser.Set_sqlContext ctx);

	/**
	 * Enter a parse tree produced by {@link MycatParser#offline_msql}.
	 * @param ctx the parse tree
	 */
	void enterOffline_msql(@NotNull MycatParser.Offline_msqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link MycatParser#offline_msql}.
	 * @param ctx the parse tree
	 */
	void exitOffline_msql(@NotNull MycatParser.Offline_msqlContext ctx);
}