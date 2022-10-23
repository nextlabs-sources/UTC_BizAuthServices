package com.nextlabs.ac.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nextlabs.ac.AuthorityService;

public class IOHelper extends Thread {
	private static final Log LOG = LogFactory.getLog(IOHelper.class);

	public String getEnv_id() {
		return env_id;
	}

	public void setEnv_id(String env_id) {
		this.env_id = env_id;
	}

	public ArrayList<String> getLicenses() {
		return licenses;
	}

	public void setLicenses(ArrayList<String> licenses) {
		this.licenses = licenses;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isAuthorityNeeded() {
		return isAuthorityNeeded;
	}

	public void setAuthorityNeeded(boolean isAuthorityNeeded) {
		this.isAuthorityNeeded = isAuthorityNeeded;
	}

	public HSQLHelper getHelper() {
		return helper;
	}

	public void setHelper(HSQLHelper helper) {
		this.helper = helper;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	String env_id;
	ArrayList<String> licenses;
	String path;
	boolean isAuthorityNeeded = false;
	HSQLHelper helper;
	String query;

	public void run() {
		File file;
	
			file = new File(path  + env_id+".log");
			if(!file.exists())
				try {
					file.createNewFile();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		StringBuilder licenseString = new StringBuilder();
		int count = 0;
		for (String license : licenses) {
			if (count == 3)
				break;
			if (isAuthorityNeeded)
				licenseString.append(helper.getAuthId(prepareQuery(license)));
			else
				licenseString.append(license);
			licenseString.append(",");
			count++;
		}
		String tolog=licenseString.toString().substring(0,licenseString.length()-1);
		try {
			PrintStream ps = new PrintStream(file);
						ps.println(tolog);
			ps.close();
		} catch (FileNotFoundException e) {
			LOG.warn("File Not found Exception", e);
		}
		
		LOG.info("Licenses are written to file for License Log Obligation");

	}

	private String prepareQuery(String license) {
		return MessageFormat.format(query, license);
	}

}
