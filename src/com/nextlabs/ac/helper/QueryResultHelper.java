package com.nextlabs.ac.helper;

import java.util.ArrayList;

public class QueryResultHelper {
	String query;
	ArrayList<String> tableFieldList;
	public QueryResultHelper()
	{
		tableFieldList=new ArrayList<String>();
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public ArrayList<String> getTableFieldList() {
		return tableFieldList;
	}
	public void setTableFieldList(ArrayList<String> tableFieldList) {
		this.tableFieldList = tableFieldList;
	}

}
