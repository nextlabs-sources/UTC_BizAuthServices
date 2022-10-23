package com.nextlabs.ac.helper;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueryBuilder {
	
	private static final Log LOG = LogFactory.getLog(QueryBuilder.class);
	
	String preparedQuery;
	ArrayList<String> queryParameters;

	public QueryBuilder() {
		preparedQuery = "";
		queryParameters = new ArrayList<String>();
	}

	public String getPreparedQuery() {
		if (preparedQuery.contains("?")) {
			ArrayList<String> finalQueryParameters = new ArrayList<String>();
			StringBuffer finalQuery = new StringBuffer();
			String[] splitQuery = preparedQuery.split("\\?");

			for (int i = 0; i < queryParameters.size(); i++) {
				finalQuery.append(splitQuery[i]);
				String str = queryParameters.get(i);
				if (str.indexOf("','") > 0 || (str.startsWith("'")
						&& str.endsWith("'"))) {
					finalQuery.append(str);
				} else {
					finalQuery.append("?");
					finalQueryParameters.add(str);
				}

				if (i == (queryParameters.size() - 1)) {
					finalQuery.append(splitQuery[queryParameters.size()]);
				}
			}

			if (finalQueryParameters.size() == 0) {
				finalQueryParameters.add("a");
				finalQuery.append(" AND 'a' = ?");
			}

			preparedQuery = finalQuery.toString();
			queryParameters = finalQueryParameters;

		}
		
		LOG.debug("Final Prepared Query :: "+preparedQuery);
		return preparedQuery;
	}

	public void setPreparedQuery(String preparedQuery) {
		this.preparedQuery = preparedQuery;
	}

	public ArrayList<String> getQueryParameters() {
		return queryParameters;
	}

	public void setQueryParameters(ArrayList<String> queryParameters) {
		this.queryParameters = queryParameters;
	}
	
	public static void main(String[] args){
		QueryBuilder qb = new QueryBuilder();
		qb.setPreparedQuery("au.status = ? AND au.lic in (?) AND au.item in (?) AND 1=1");
		ArrayList queryParameters = new ArrayList();
		queryParameters.add("1234");
		queryParameters.add("'abc','pqr'");
		queryParameters.add("'99'");
		qb.setQueryParameters(queryParameters);
		System.out.println(qb.getPreparedQuery());
		System.out.println(qb.getQueryParameters());
	}
}
