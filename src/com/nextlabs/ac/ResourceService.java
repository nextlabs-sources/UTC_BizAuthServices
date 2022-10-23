package com.nextlabs.ac;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.IEvalValue;
import com.bluejungle.framework.expressions.IMultivalue;
import com.bluejungle.framework.expressions.Multivalue;
import com.bluejungle.framework.expressions.ValueType;
import com.bluejungle.pf.domain.destiny.serviceprovider.IFunctionServiceProvider;
import com.bluejungle.pf.domain.destiny.serviceprovider.ServiceProviderException;
import com.nextlabs.ac.helper.ACConstants;
import com.nextlabs.ac.helper.HSQLHelper;
import com.nextlabs.ac.helper.PropertyLoader;
import com.nextlabs.ac.helper.QueryBuilder;

public class ResourceService implements IFunctionServiceProvider {

	private static final Log LOG = LogFactory.getLog(ResourceService.class);
	private static Properties prop = PropertyLoader
			.loadProperties(ACConstants.COMMON_AC_PROPFILE);
	private static HSQLHelper hsqlHelper;

	// UTC test Case 5: Bug fix start
	private static IEvalValue emptySet = EvalValue.build(Multivalue.EMPTY);
	// UTC test Case 5: Bug fix end
	private final static String INTERNALLICENSEARGUMENT = MessageFormat.format(
			"DISTINCT({0})", prop.getProperty("au_col_InternalLicenseNo"));
	private final static String LICENSEARGUMENT = MessageFormat.format(
			"DISTINCT({0})", prop.getProperty("au_col_AuthorityId"));

	@Override
	public void init() throws Exception {
		LOG.info("ResourceService init() started.");

		if (null != prop) {
			hsqlHelper = new HSQLHelper(prop.getProperty("hsql_server_url"),
					prop.getProperty("hsql_user_name"),
					prop.getProperty("hsql_password"), Integer.parseInt(prop
							.getProperty("hsql_connectionpool_size")));

		}
		LOG.info("ResourceService init() completed.");

	}

	@Override
	public IEvalValue callFunction(String functionName, IEvalValue[] args)
			throws ServiceProviderException {
		IEvalValue result = emptySet;
		try {
			LOG.info("ResourceService callfunction() started, with function: "
					+ functionName);
			long lCurrentTime = System.nanoTime();
			if ("getLicenseNos".equalsIgnoreCase(functionName)) {
				result = getLicenseNos(args, false, LICENSEARGUMENT);
			} else if ("getValidLicenseNos".equalsIgnoreCase(functionName)) {
				result = getLicenseNos(args, true, LICENSEARGUMENT);
			}
			if ("getInternalLicenseNos".equalsIgnoreCase(functionName)) {
				result = getLicenseNos(args, false, INTERNALLICENSEARGUMENT);
			} else if ("getValidInternalLicenseNos"
					.equalsIgnoreCase(functionName)) {
				result = getLicenseNos(args, true, INTERNALLICENSEARGUMENT);
			}
			LOG.debug("ResourceService callfunction() completed. Result: "
					+ result.toString() + " Time spent: "
					+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
		} catch (Exception e) {
			LOG.error("ResourceService callfunction() error: ", e);
		}

		return result;
	}

	/*
	 * Get the LicenseNos(authorityIds) based on the predicates passed by the
	 * policy. if valid flag is true returns only active licenses else returns
	 * both active and inactive licenses. This single method returns the list of
	 * internal licensenos and authority ids based on the coloumnargument.
	 */
	private IEvalValue getLicenseNos(IEvalValue[] args, boolean validFlag,
			String colArgument) {

		LOG.info("ResourceService getLicenseNos() called.");
		LOG.debug("ResourceService getLicenseNos() sColumn.length : "
				+ args.length);
		IEvalValue evalue = emptySet;
		if (args.length < 1) {
			LOG.warn("Wrong number of parameters.");
			return evalue;
		}
		ArrayList<String> sArrDataInput = processValues(args);

		LOG.debug("ResourceService sArrDataInput : " + sArrDataInput);
		LOG.debug("ResourceService sArrDataInput.size() : "
				+ sArrDataInput.size());
		ArrayList<String> licenses = hsqlHelper
				.retrieveLicenses(prepareQueryforLicenses(sArrDataInput,
						colArgument, validFlag));
		LOG.debug(" ResourceService Licenses " + licenses);
		if (licenses != null) {
			evalue = new AuthorityService().prepareLicenses(licenses);
		}
		return evalue;
	}

	/*
	 * This method prepares a parameterized query string and list of parameters
	 * for the query into a single QueryBuilder object
	 */
	private QueryBuilder prepareQueryforLicenses(
			ArrayList<String> sArrDataInput, String projColumn,
			boolean validFlag) {
		LOG.info("Resource Service prepareQueryforLicenses() called.");
		QueryBuilder qb = new QueryBuilder();
		StringBuilder query = new StringBuilder(
				"SELECT {0} FROM {1} {2}  LEFT OUTER JOIN {3} {4} ON {2}.{5}={4}.{6}  WHERE LCASE(TRIM({4}.{7}))=LCASE(TRIM(?))");

		if (validFlag)
			query.append(" AND  {8}=''{11}'' AND   {9} < current_date AND  {10} > current_date   ");

		Object[] tcArgs = { projColumn, prop.getProperty("table_au"),
				prop.getProperty("alias_au"), prop.getProperty("table_arm"),
				prop.getProperty("alias_arm"),
				prop.getProperty("au_col_AuthorityId"),
				prop.getProperty("arm_col_AuthorityId"),
				prop.getProperty("arm_col_Resource"),
				prop.getProperty("au_col_LicenseStatus"),
				prop.getProperty("au_col_StartDate"),
				prop.getProperty("au_col_EndDate"),
				prop.getProperty("license_status_active") };
		Iterator<String> iterator = sArrDataInput.iterator();
		while (iterator.hasNext()) {
			qb.getQueryParameters().add(iterator.next());
		}
		qb.setPreparedQuery(MessageFormat.format(query.toString(), tcArgs));
		LOG.info(qb.getPreparedQuery() + " " + qb.getQueryParameters());
		return qb;

	}

	protected ArrayList<String> processValues(IEvalValue[] args) {
		int i = 0;
		ArrayList<String> sOutData = new ArrayList<String>();
		for (IEvalValue ieValue : args) {
			String sData = "";
			if (null != ieValue) {
				LOG.info("ieValue.getType()" + ieValue.getType());
				if (ieValue.getType() == ValueType.STRING) {
					sData = ieValue.getValue().toString();
					sOutData.add(sData);
				}
				if (ieValue.getType() == ValueType.MULTIVAL) {
					IMultivalue mv = (IMultivalue) ieValue.getValue();
					if (mv.iterator().hasNext()) {
						IEvalValue ev = mv.iterator().next();
						if (ev.getType() == ValueType.STRING) {
							if (!ev.getValue().toString().isEmpty()) {
								sData = ev.getValue().toString();
								sOutData.add(sData);
							}
						}
					}
				}
				LOG.info("----" + i + "." + sData + "-----");

			}
			i++;
		}
		return sOutData;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ResourceService plugin = new ResourceService();
		plugin.init();
		IEvalValue[] sDataArr = new IEvalValue[1];
		sDataArr[0] = EvalValue.build("10001");

		System.out.println(plugin.callFunction("getValidLicenseNos", sDataArr));

	}

}
