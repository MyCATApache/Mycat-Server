// Generated from C:\Users\Coollf\Mycat.g4 by ANTLR 4.1
package io.mycat.server.parser.antlr;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link MycatParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface MycatVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link MycatParser#help_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHelp_sql(@NotNull MycatParser.Help_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#rollback_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRollback_sql(@NotNull MycatParser.Rollback_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#start_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStart_sql(@NotNull MycatParser.Start_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#show_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShow_sql(@NotNull MycatParser.Show_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#reload_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReload_msql(@NotNull MycatParser.Reload_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#show_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShow_msql(@NotNull MycatParser.Show_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#switch_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSwitch_msql(@NotNull MycatParser.Switch_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#stop_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStop_msql(@NotNull MycatParser.Stop_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#commit_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommit_sql(@NotNull MycatParser.Commit_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#ddl_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDdl_sql(@NotNull MycatParser.Ddl_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#mycat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMycat(@NotNull MycatParser.MycatContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#explain_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExplain_sql(@NotNull MycatParser.Explain_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#replace_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReplace_sql(@NotNull MycatParser.Replace_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#kill_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKill_msql(@NotNull MycatParser.Kill_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#describe_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribe_sql(@NotNull MycatParser.Describe_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#begin_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBegin_sql(@NotNull MycatParser.Begin_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#update_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdate_sql(@NotNull MycatParser.Update_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#insert_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsert_sql(@NotNull MycatParser.Insert_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#rollback_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRollback_msql(@NotNull MycatParser.Rollback_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#clear_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitClear_msql(@NotNull MycatParser.Clear_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#delete_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDelete_sql(@NotNull MycatParser.Delete_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#savepoint_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSavepoint_sql(@NotNull MycatParser.Savepoint_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#use_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUse_sql(@NotNull MycatParser.Use_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#load_data_infile_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoad_data_infile_sql(@NotNull MycatParser.Load_data_infile_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#online_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOnline_msql(@NotNull MycatParser.Online_msqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql(@NotNull MycatParser.SqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMsql(@NotNull MycatParser.MsqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#mysql_comment_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMysql_comment_sql(@NotNull MycatParser.Mysql_comment_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#call_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCall_sql(@NotNull MycatParser.Call_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#select_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_sql(@NotNull MycatParser.Select_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#kill_query_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKill_query_sql(@NotNull MycatParser.Kill_query_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#kill_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKill_sql(@NotNull MycatParser.Kill_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#set_sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSet_sql(@NotNull MycatParser.Set_sqlContext ctx);

	/**
	 * Visit a parse tree produced by {@link MycatParser#offline_msql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOffline_msql(@NotNull MycatParser.Offline_msqlContext ctx);
}