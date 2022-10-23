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
import com.nextlabs.ac.helper.PropertyLoader;
import com.nextlabs.ac.helper.QueryBuilder;

public class BizAuthService implements IFunctionServiceProvider {
	private static final Log LOG = LogFactory.getLog(BizAuthService.class);
	private static IEvalValue nullResult = EvalValue.build("NULL");
	private static Properties prop = PropertyLoader
			.loadProperties(ACConstants.COMMON_AC_PROPFILE);
	private static UserService us = new UserService();
	private static AuthorityService as = new AuthorityService();
	private static ResourceService rs = new ResourceService();
	private static UtilService uts = new UtilService();

	/*
	 * global default values and other functionalities which has to be processed
	 * during the initiation of the service
	 */
	@Override
	public void init() throws Exception {
		LOG.info("BizAuthService init() started");
		us.init();
		as.init();
		rs.init();
		uts.init();
		LOG.info("BizAuthService Registered");

	}

	/*
	 * This is a main call function called by the policy controller through
	 * advanced condition.It checks for the valid methods and pass the control
	 * flow to the correct methods.
	 */
	@Override
	public IEvalValue callFunction(String functionName, IEvalValue[] args)
			throws ServiceProviderException {
		IEvalValue result = nullResult;
		try {
			LOG.info("BizAuthService callfunction() started, with function: "
					+ functionName);
			long lCurrentTime = System.nanoTime();
			ArrayList<String> inputList = us.processValues(args);
			if ("directMatchCount".equalsIgnoreCase(functionName)) {
				result = directMatchCount(inputList, args);
			} else if ("checkCCLCountries".equalsIgnoreCase(functionName)) {
				result = checkCCLCountries(inputList, args);
			}
			if ("licenseMatchCount".equalsIgnoreCase(functionName)) {
				result = licenseMatchCount(inputList, args);
			}
			LOG.debug("BizAuthService callfunction() completed. Result: "
					+ result.toString() + " Time spent: "
					+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
		} catch (Exception e) {
			LOG.error("BizAuthService callfunction() error: ", e);
		}

		return result;
	}

	private IEvalValue licenseMatchCount(ArrayList<String> inputList,
			IEvalValue[] args) {
		LOG.info("BizAuthService licenseMatchCount() started, with function: ");
		IEvalValue evalue = nullResult;
		if (inputList.size() < 8) {
			LOG.warn("Warning: Incorrected no of arguments passed and returning default value");
			return evalue;
		}
		boolean validityFlag = Boolean.parseBoolean(inputList.get(7));
		LOG.info(validityFlag);
		String physicalLocation = inputList.get(6);
		QueryBuilder qb = new QueryBuilder();
		String userid = inputList.get(5);
		ArrayList<String> allAuthids = new ArrayList<String>();
		us.identifyUserIdType(userid);
		String countryQuery;

		// Getting authorityid for physical location.
		String plQuery = "SELECT DISTINCT {0} FROM {1} WHERE {2}={3}";
		Object[] plarguments = { prop.getProperty("actm_col_AuthorityId"),
				prop.getProperty("table_actm"),
				prop.getProperty("actm_col_CountryCode"),
				"'" + physicalLocation + "'" };
		qb.setPreparedQuery(MessageFormat.format(plQuery, plarguments));
		ArrayList<String> plauthids = us.getGenAttributeList(qb);

		// Getting authority ids for the licensecountry and user country all
		// match
		countryQuery = "SELECT {2} FROM {3} {4} LEFT OUTER JOIN {5} {6} ON {6}.{7} = {4}.{8} WHERE LCASE(TRIM({0})) = LCASE(TRIM(''{1}'')) GROUP BY {2} HAVING COUNT(DISTINCT {6}.{7}) = (SELECT COUNT(DISTINCT {8}) FROM {3} WHERE LCASE(TRIM({0})) = LCASE(TRIM(''{1}'')))";

		if (us.userIDtype.equals(prop.getProperty("user_col_WindowsSID"))) {
			Object[] arguments = { us.userIDtype,  userid ,
					prop.getProperty("actm_col_AuthorityId"),
					prop.getProperty("table_ucm"),
					prop.getProperty("alias_ucm"),
					prop.getProperty("table_actm"),
					prop.getProperty("alias_actm"),
					prop.getProperty("actm_col_CountryCode"),
					prop.getProperty("ucm_col_countrycode") };
			qb.setPreparedQuery(MessageFormat.format(countryQuery, arguments));
			System.out.println(qb.getPreparedQuery());
			allAuthids = us.getGenAttributeList(qb);

		} else {
			Object[] arguments = { us.userIDtype, userid,
					prop.getProperty("actm_col_AuthorityId"),
					prop.getProperty("table_ucm"),
					prop.getProperty("alias_ucm"),
					prop.getProperty("table_actm"),
					prop.getProperty("alias_actm"),
					prop.getProperty("actm_col_CountryCode"),
					prop.getProperty("ucm_col_countrycode") };
			qb.setPreparedQuery(MessageFormat.format(countryQuery, arguments));
			System.out.println(qb.getPreparedQuery());
			allAuthids = us.getGenAttributeList(qb);
		}

		StringBuilder wcb = new StringBuilder();
		IEvalValue[] argumentsforASGetCount;
		int argumentcount = 0;
		int loopbreakersize = 5;
		/*
		 * where clause predicate construction for
		 * foreignconsigeee=usercompanyinc and user all countries in license
		 * countries
		 */
		if (allAuthids.size() > 0) {
			LOG.info("Allauthids:"+allAuthids);
			String inAuthid = getInAuthid(allAuthids);
			IEvalValue[] evs = new IEvalValue[2];
			String fc = "";
			evs[0] = EvalValue.build(userid);
			evs[1] = EvalValue.build(prop.getProperty("user_col_CompanyId"));
			try {
				IEvalValue foreignconsignee = us.callFunction("getAttribute",
						evs);
				fc = foreignconsignee.getValue().toString();
			} catch (ServiceProviderException e1) {
				LOG.error(
						"BizAuthService callfunction() to User Service error: ",
						e1);
			}
			System.out.println("ForeignConsignee" + fc);
			if (null!=plauthids && plauthids.size() > 0)
				wcb.append("(");
			wcb.append(" ( {0}.{1} IN ");
			wcb.append(inAuthid);
			wcb.append(" AND ");
			wcb.append("LCASE(TRIM({11}.{12}))=LCASE(TRIM(''''");
			wcb.append(fc);
			wcb.append("'''' )))");
			if (null!=plauthids && plauthids.size() > 0)
				wcb.append(" OR ");
			else
				wcb.append(" AND ");
		} /*
		 * where clause predicate construction for userid in authorized user and
		 * user all countries in license countries
		 */
		if (null!=plauthids && plauthids.size() > 0) {
			LOG.info("Allauthids:"+plauthids);
			String otherIdtype=us.getOtherIDType();
			IEvalValue[] evs = new IEvalValue[2];
			String otheridvalue = "";
			evs[0] = EvalValue.build(userid);
			evs[1] = EvalValue.build(otherIdtype);
			IEvalValue otheridIeval;
			try {
				otheridIeval = us.callFunction("getAttribute",
						evs);
				otheridvalue = otheridIeval.getValue().toString();
			} catch (ServiceProviderException e) {
				LOG.error(
						"BizAuthService callfunction() to User Service error: ",
						e);
			}
		
			
			String plAuthid = getInAuthid(plauthids);
			wcb.append(" ( {0}.{1} IN ");
			wcb.append(plAuthid);
			wcb.append(" AND ");
			wcb.append("(LCASE(TRIM({9}.{10}))=? OR  LCASE(TRIM({9}.");
			wcb.append(otherIdtype);
			wcb.append("))=LCASE(TRIM(''''");
			wcb.append(otheridvalue);
			wcb.append("''''))) )");
			if (allAuthids.size() > 0)
				wcb.append(")");
			wcb.append(" AND ");
			argumentsforASGetCount = new IEvalValue[7];
			argumentsforASGetCount[1] = EvalValue.build(userid);
			argumentcount = 1;
			loopbreakersize = 6;
		} else {
			argumentsforASGetCount = new IEvalValue[6];
		}
		/*
		 * where predicate for the resource level predicates like
		 * jurisdiction,classification,scope,sme and productno
		 */
		wcb.append("((LCASE(TRIM({2}.{3})) = ?  AND LCASE(TRIM({2}.{4})) = ? AND LCASE(TRIM({0}.{5}))= ? AND LCASE(TRIM({0}.{6}))= ?) OR  ( LCASE(TRIM({7}.{8}))= ? ) )");

		Object[] wcbargs = { prop.getProperty("alias_au"),
				prop.getProperty("au_col_AuthorityId"),
				prop.getProperty("alias_jcm"),
				prop.getProperty("jcm_col_Jurisdiction"),
				prop.getProperty("jcm_col_Classification"),
				prop.getProperty("au_col_SME"),
				prop.getProperty("au_col_Scope"),
				prop.getProperty("alias_arm"),
				prop.getProperty("arm_col_Resource"),
				prop.getProperty("alias_aaum"), us.userIDtype,
				prop.getProperty("alias_afsm"),
				prop.getProperty("afsm_col_ForeignConsignee") };
		// building arguments for call funtion getCount in AuthorityService

		argumentsforASGetCount[0] = EvalValue.build(MessageFormat.format(
				wcb.toString(), wcbargs));
		Iterator<String> iter = inputList.iterator();
		for (; argumentcount < loopbreakersize; argumentcount++)
			argumentsforASGetCount[argumentcount + 1] = EvalValue.build(iter
					.next().toLowerCase());

		// CAlling Authorityservice get count
		try {
			if (validityFlag)
				evalue = as.callFunction("getValidCount",
						argumentsforASGetCount);
			else
				evalue = as.callFunction("getCount", argumentsforASGetCount);
		} catch (ServiceProviderException e) {
			LOG.error(
					"BizAuthService callfunction() to Authority Service error: ",
					e);
		}
		return evalue;
	}

	private String getInAuthid(ArrayList<String> authids) {

		LOG.info("BizAuthService getInCountry() started, with function: ");
		StringBuilder result = new StringBuilder("(");
		Iterator<String> iter = authids.iterator();
		while (iter.hasNext()) {
			result.append("'");
			result.append(iter.next());
			result.append("'");
			if (iter.hasNext())
				result.append(",");

		}
		result.append(")");
		return result.toString();
	}

	/*
	 * This method takes up jurisdiction,classification,userid and physical
	 * location and checks whether the user countries are in the CCL countries
	 * list. if there is anymatch between the two lists return true else false
	 */
	private IEvalValue checkCCLCountries(ArrayList<String> inputList,
			IEvalValue[] args) {
		LOG.info("BizAuthService checkCCLCountries() started, with function: ");
		IEvalValue evalue = nullResult;
		if (inputList.size() < 4) {
			LOG.warn("Warning: Incorrected no of arguments passed and returning default value");
			return evalue;
		}
		IEvalValue[] parameter = new IEvalValue[1];

		IEvalValue userCountries, cclCountries;
		try {
			parameter[0] = args[2];// UserID
			userCountries = us.callFunction("getAllCountries", parameter);

			parameter = new IEvalValue[2];
			parameter[0] = args[0];// Jurisdiction
			parameter[1] = args[1];// Classification
			cclCountries = as.callFunction("getCCLCountries", parameter);
			userCountries = addPhysicalLocation(userCountries, inputList.get(3));
			parameter[0] = userCountries;
			parameter[1] = cclCountries;

			evalue = uts.callFunction("isAnyMatch", parameter);

		} catch (ServiceProviderException e) {
			LOG.error("Error: in BizAuthService directMatchCount ", e);
		} catch (NullPointerException e) {
			LOG.error("Error: in BizAuthService directMatchCount ", e);
		}
		return evalue;
	}

	/*
	 * This method helps to add the physical location with the list of available
	 * user countries
	 */
	private IEvalValue addPhysicalLocation(IEvalValue userCountries,
			String phyLocation) {
		LOG.info("BizAuthService addPhysicalLocation() started, with function: ");
		IEvalValue evalue = userCountries;
		IMultivalue mv = (IMultivalue) evalue.getValue();
		Iterator<IEvalValue> itr = mv.iterator();
		ArrayList<IEvalValue> evs = new ArrayList<IEvalValue>();
		while (itr.hasNext()) {
			evs.add(itr.next());
		}
		if (phyLocation != null && !phyLocation.trim().isEmpty()) {
			evs.add(EvalValue.build(phyLocation));
		}

		IMultivalue result = Multivalue.create(evs, ValueType.STRING);
		evalue = EvalValue.build(result);

		return evalue;
	}

	/*
	 * This method is helps to find the direct match count of user licenses and
	 * resource licenses. This method simply reduces the work of policy builder
	 * by calling a single function instead of multiple call function at
	 * advanced condition.
	 */
	private IEvalValue directMatchCount(ArrayList<String> inputList,
			IEvalValue[] args) {
		LOG.info("BizAuthService directMatchCount() started, with function: ");
		IEvalValue evalue = nullResult;
		if (inputList.size() < 4) {
			LOG.warn("Warning: Incorrected no of arguments passed and returning default value");
			return evalue;
		}
		IEvalValue[] parameter = new IEvalValue[1];

		IEvalValue userLicenses, resourceLicenses;
		boolean validityFlag = Boolean.parseBoolean(inputList.get(3));
		try {
			parameter[0] = args[0];
			userLicenses = us.callFunction("getLicenses", parameter);
			if (validityFlag) {

				parameter[0] = args[1];
				resourceLicenses = rs.callFunction("getLicenseNos", parameter);
			} else {

				parameter[0] = args[1];
				resourceLicenses = rs.callFunction("getValidLicenseNos",
						parameter);

			}
			parameter = new IEvalValue[2];
			parameter[0] = userLicenses;
			parameter[1] = resourceLicenses;
			return uts.callFunction("getMatchCount", parameter);
		} catch (ServiceProviderException e) {
			LOG.error("Error: in BizAuthService directMatchCount ", e);
		} catch (NullPointerException e) {
			LOG.error("Error: in BizAuthService directMatchCount ", e);
		}
		return evalue;
	}

	/*
	 * This method is for just unit test the code
	 */
	public static void main(String[] args) throws Exception {
		BizAuthService bas = new BizAuthService();
		bas.init();
		IEvalValue[] sDataArr = new IEvalValue[8];

		/*
		 * ArrayList<IEvalValue> evs = new ArrayList<IEvalValue>();
		 * 
		 * evs.add(EvalValue.build("1")); evs.add(EvalValue.build("2"));
		 * evs.add(EvalValue.build("3")); evs.add(EvalValue.build("4"));
		 * evs.add(EvalValue.build("5"));
		 * 
		 * IMultivalue imv = Multivalue.create(evs, ValueType.STRING);
		 * 
		 * evs = new ArrayList<IEvalValue>();
		 * 
		 * evs.add(EvalValue.build("1")); evs.add(EvalValue.build("2"));
		 * evs.add(EvalValue.build("3")); evs.add(EvalValue.build("5"));
		 * evs.add(EvalValue.build("4"));
		 */
		// IMultivalue imv1IMultivalue;

		sDataArr[0] = EvalValue.build("EAR");
		sDataArr[1] = EvalValue.build("9E003.a.4");
		sDataArr[2] = EvalValue.build("Y");
		sDataArr[3] = EvalValue.build("");
		sDataArr[4] = EvalValue.build("4567-09");
		sDataArr[5] = EvalValue.build("30979");
		sDataArr[6] = EvalValue.build("FR");
		sDataArr[7] = EvalValue.build("true");
		System.out.println(bas.callFunction("licenseMatchCount", sDataArr));
	}

}
