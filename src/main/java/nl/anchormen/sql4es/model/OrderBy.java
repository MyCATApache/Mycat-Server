package nl.anchormen.sql4es.model;

import org.elasticsearch.search.sort.SortOrder;

public class OrderBy {

	private String field;
	private int index;
	private SortOrder order;
	
	public OrderBy(String field, SortOrder order, int index){
		this.field = field;
		this.order = order;
		this.index = index;
	}

	public String getField() {
		return field;
	}

	public SortOrder getOrder() {
		return order;
	}
	
	public int getIndex(){
		return index;
	}
	
	public int func(){
		if(order == SortOrder.ASC) return 1;
		else return -1;
	}
	
}
