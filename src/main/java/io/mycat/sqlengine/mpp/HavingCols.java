package io.mycat.sqlengine.mpp;

import java.io.Serializable;

/**
 * Created by v1.lion on 2015/6/10.
 */
public class HavingCols implements Serializable {
	String left;
	String right;
	String operator;
	public ColMeta colMeta;

	public HavingCols(String left, String right, String operator) {
		this.left = left;
		this.right = right;
		this.operator = operator;
	}

	public String getLeft() {
		return left;
	}

	public void setLeft(String left) {
		this.left = left;
	}

	public String getRight() {
		return right;
	}

	public void setRight(String right) {
		this.right = right;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public ColMeta getColMeta() {
		return colMeta;
	}

	public void setColMeta(ColMeta colMeta) {
		this.colMeta = colMeta;
	}
}
