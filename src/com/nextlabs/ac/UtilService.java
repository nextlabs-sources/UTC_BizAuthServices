package com.nextlabs.ac;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;

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
import com.nextlabs.ac.helper.IOHelper;
import com.nextlabs.ac.helper.PropertyLoader;

public class UtilService implements IFunctionServiceProvider {

	private static IEvalValue nullResult = EvalValue.NULL;
	private static IEvalValue emptyString = EvalValue.build("");

	private static final Log LOG = LogFactory.getLog(UtilService.class);
	private static Properties prop = PropertyLoader
			.loadProperties(ACConstants.COMMON_AC_PROPFILE);
	private String custom_logging_path;
	private String env_reqid = null;
	private static HSQLHelper hsqlHelper;

	/*
	 * 
	 * global default values which has to be assigned during the initiation of
	 * 
	 * the service
	 */

	@Override
	public void init() throws Exception {

		LOG.info("UtilService init() started.");
		if (prop != null) {
			hsqlHelper = new HSQLHelper(prop.getProperty("hsql_server_url"),
					prop.getProperty("hsql_user_name"),
					prop.getProperty("hsql_password"), Integer.parseInt(prop
							.getProperty("hsql_connectionpool_size")));
			custom_logging_path = prop.getProperty("custom_logging_path");
			if (!custom_logging_path.endsWith("\\")) {
				custom_logging_path += "\\";
			}
		}
		LOG.info("UtilService init() completed.");

	}

	/*
	 * 
	 * This is a main call function called by the policy controller through
	 * 
	 * advanced condition.It checks for the valid methods and pass the control
	 * 
	 * flow to the correct methods.
	 */

	@Override
	public IEvalValue callFunction(String functionName, IEvalValue[] args)

	throws ServiceProviderException {

		LOG.info("UtilService callfunction() started, with function: "

		+ functionName);

		IEvalValue result = nullResult;

		ArrayList<ArrayList<String>> inputList;

		try {

			long lCurrentTime = System.nanoTime();
			// UTC 5:BUG fix start
			if (args.length < 1) {
				if (functionName.equalsIgnoreCase("isStale")) {
					String isStale = "true";
					isStale = hsqlHelper.getIsStale();
					if (isStale != null)
						result = EvalValue.build(isStale);

					LOG.info("UtilService callfunction() completed. Result: isStale is "
							+ result.toString()
							+ " Time spent: "
							+ ((System.nanoTime() - lCurrentTime) / 1000000.00)
							+ "ms");
					return result;

				}
				LOG.warn("Error:Incorrect no of arguments");
				result = EvalValue.build("true");
			}
			if ("isEmptySet".equalsIgnoreCase(functionName)) {
				result = isEmptySet(args);

			} else if ("isNUll".equalsIgnoreCase(functionName)) {
				result = isNUll(args);

			} else if ("isAfterCurrentTimeStamp".equalsIgnoreCase(functionName)
					&& args.length == 1) {
				result = isAfterCurrentTimeStamp(args);
			} else if ("parseMaterial".equalsIgnoreCase(functionName)) {
				result = parseMaterial(args);
			} else if ("toLower".equalsIgnoreCase(functionName)) {
				result = toLowerCase(args);
			}
			// UTC 5:BUG fix end
			else {
				inputList = processValues(args);

				if (inputList == null) {

					LOG.warn("Error UnexectedArguments:Expected arguments is a list of multivalues");

					return nullResult;

				}

				if (inputList.size() < 2) {

					LOG.warn("Error:Incorrect no of arguments");

					return nullResult;

				}

				if ("getMatchCount".equalsIgnoreCase(functionName)) {

					result = matchCount(inputList);

				} else if ("isAnyMatch".equalsIgnoreCase(functionName)) {

					result = isAnyMatch(inputList);

				}

				else if ("displayContent".equalsIgnoreCase(functionName)) {

					LOG.info(inputList.get(0) + " for Util Display:"
							+ inputList.get(1));
					return nullResult;

				}
				// PWC Addendum 1.2 changes for CC7.0 upgrade changes start
				else if ("isLicensesMatch".equalsIgnoreCase(functionName)) {
					result = isLicensesMatch(inputList);
				}
				// // PWC Addendum 1.2 changes for CC7.0 upgrade changes end
				/*
				 * 
				 * else if ("isAllMatch".equalsIgnoreCase(functionName)) {
				 * result =
				 * 
				 * isAllMatch(inputList); }
				 */

				else if ("toList".equalsIgnoreCase(functionName)) {

					result = toList(inputList);

				} else if ("isAfterCurrentTimeStamp"
						.equalsIgnoreCase(functionName)) {
					result = isAfterCurrentTimeStamp(inputList);
				}
			}
			LOG.info("UtilService callfunction() completed. Result: "

			+ result.toString() + " Time spent: "

			+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");

		} catch (Exception e) {

			LOG.error("UtilService callfunction() error: ", e);

		}

		return result;

	}

	// PWC Addendum 1.2 changes for CC7.0 upgrade changes start
	private IEvalValue isLicensesMatch(ArrayList<ArrayList<String>> inputList) {
		IEvalValue falseVal = EvalValue.build("false");
		if (inputList.size() < 4) {
			LOG.warn("Error:Incorrect no of parameters ");
			return falseVal;
		}
		ArrayList<String> userLicenses = new ArrayList<String>(), resLicenses = new ArrayList<String>(), licenses;
		String isAuthorityneeded = "false";
		if (inputList.get(0) != null) {
			userLicenses = inputList.get(0);
		}
		if (inputList.get(1) != null) {
			resLicenses = inputList.get(1);
		}
		if (inputList.get(2) != null && inputList.get(2).get(0) != null) {
			env_reqid = inputList.get(2).get(0);
		}
		if (inputList.get(3) != null && inputList.get(3).get(0) != null) {
			isAuthorityneeded = inputList.get(3).get(0);
		}
		licenses = getIntersectedList(userLicenses, resLicenses);
		if (env_reqid != null && hsqlHelper != null
				&& custom_logging_path != null && licenses != null
				&& !licenses.isEmpty()) {
			IOHelper ioh = new IOHelper();
			String query = "SELECT {0} FROM {1} WHERE {2}='{0}'";
			Object[] args = { prop.getProperty("au_col_InternalLicenseNo"),
					prop.getProperty("table_au"),
					prop.getProperty("au_col_AuthorityId") };
			ioh.setQuery(MessageFormat.format(query, args));
			ioh.setHelper(hsqlHelper);
			ioh.setPath(custom_logging_path);
			ioh.setEnv_id(env_reqid);
			ioh.setLicenses(licenses);
			if (isAuthorityneeded.equals("true"))
				ioh.setAuthorityNeeded(true);
			ioh.start();
			LOG.info("Thread called to log the license in a file");
		} else {
			return EvalValue.build("false");
		}
		return EvalValue.build("true");
	}

	// PWC Addendum 1.2 changes for CC7.0 upgrade changes end
	/**
	 * This method is to parse the SAP material and return the material name
	 * alone.
	 * 
	 * @param args
	 *            full path of the SAP material name at args[0]
	 * @return Material name or an empty string
	 * 
	 */
	private IEvalValue parseMaterial(IEvalValue[] args) {
		String result = "";
		ArrayList<String> resourceNames = new ArrayList<String>();
		try {
			if (args.length != 1) {
				return emptyString;
			}
			LOG.info("UtilService - parseMaterial args ::" + args[0]);
			if (args[0] != null) {
				if (args[0].getType() == ValueType.STRING) {
					resourceNames.add(args[0].getValue().toString());
				}
				if (args[0].getType() == ValueType.MULTIVAL) {
					IMultivalue mv = (IMultivalue) args[0].getValue();
					for (Iterator<IEvalValue> it = mv.iterator(); it.hasNext();) {
						IEvalValue ev = it.next();
						if (ev.getType() == ValueType.STRING) {
							if (!ev.getValue().toString().isEmpty()) {
								resourceNames.add(ev.getValue().toString());
							}
						}
					}

				}
			}

			LOG.info("UtilService - parseMaterial resourceNames List ::"
					+ resourceNames);
			for (String resource : resourceNames) {
				String[] tokens = resource.split("/");
				if (tokens.length > 7) {
					result = tokens[7];
					LOG.info("UtilService - parseMaterial full resource :"
							+ resource);
					LOG.info("UtilService - parseMaterial parsed resource :"
							+ result);
					return EvalValue.build(result);
				}

			}

		} catch (Exception e) {
			LOG.warn(
					"UtilService: Exception in parseMaterial: "
							+ e.getMessage(), e);
		}
		return emptyString;
	}

	/**
	 * This method is to check the input time is after the current time
	 * 
	 * @param args
	 *            input time will be there in args[0]
	 * @return true or false
	 */

	private IEvalValue isAfterCurrentTimeStamp(IEvalValue[] args) {
		LOG.info("UTILSERVICE: Using the timestamp format from Common_AC.properties");
		String inputTime = null;
		String timeFormat = prop
				.getProperty("sharepoint_user_location_timestamp");
		if (args[0] != null && args[0].getType() == ValueType.DATE) {
			LOG.info("UTILSERVICE:Type Date : User Physical Location Expiry Time"
					+ args[0].getValue());
			DateFormat df = new SimpleDateFormat(timeFormat);
			inputTime = df.format(args[0].getValue());
			return evalIsAfterCurrentTimeStamp(timeFormat, inputTime);
		} else if (args[0] != null && args[0].getType() == ValueType.STRING) {
			LOG.info("UTILSERVICE:Type String:User Physical Location Expiry Time"
					+ args[0].getValue());
			inputTime = args[0].getValue().toString();
			return evalIsAfterCurrentTimeStamp(timeFormat, inputTime);
		}

		return EvalValue.build(prop
				.getProperty("physical_location_expiry_missing_return_value"));

	}

	/**
	 * This method is to check the input time is after the current time
	 * 
	 * @param inputList
	 *            inputList.get(0).get(0) has time format
	 *            inputList.get(1).get(0) has inputtime
	 * @return true or false
	 */

	private IEvalValue isAfterCurrentTimeStamp(
			ArrayList<ArrayList<String>> inputList) {
		LOG.info("UTILSERVICE: Using the timestamp format from sharepoint policy controller");
		String timeFormat = null, inputTime = null;
		if (inputList.size() == 2) {
			if (null != inputList.get(0) && null != inputList.get(0).get(0))
				inputTime = inputList.get(0).get(0);
			if (null != inputList.get(1) && null != inputList.get(1).get(0))
				timeFormat = inputList.get(1).get(0);

			return evalIsAfterCurrentTimeStamp(timeFormat, inputTime);
		}
		return EvalValue.build(prop
				.getProperty("physical_location_expiry_missing_return_value"));
	}

	/**
	 * Main evaluation of input time after current time stamp takes place here
	 * 
	 * @param timeFormat
	 *            Time format of inputtime string
	 * @param inputTime
	 *            inputtime in string
	 * @return true or false
	 */

	private IEvalValue evalIsAfterCurrentTimeStamp(String timeFormat,
			String inputTime) {

		if (null != timeFormat && null != inputTime) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);

				// Converting local system time to utc time
				Date currentDate = new Date();
				String format = "yyyy/MM/dd HH:mm:ss";
				SimpleDateFormat df = new SimpleDateFormat(format);
				df.setTimeZone(TimeZone.getTimeZone("UTC"));
				Date currentDateinUTC = new Date(df.format(currentDate));

				Date evalDate = sdf.parse(inputTime);

				LOG.info("UTILSERVICE: Physical location expiry time from Sharepoint in UTC : "
						+ evalDate.toString().substring(0, 20));
				LOG.info("UTILSERVICE: Current System time in "
						+ TimeZone.getDefault().getDisplayName(false,
								TimeZone.SHORT, Locale.US) + " : "
						+ currentDate.toString().substring(0, 20));
				LOG.info("UTILSERVICE: Current System time in UTC : "
						+ currentDateinUTC.toString().substring(0, 20));

				if (evalDate.after(currentDateinUTC)) {
					return EvalValue.build("true");
				}
				LOG.info("UTILSERVICE:User Physical Location Expiry Time has expired");
				return EvalValue.build("false");

			} catch (NullPointerException e) {
				LOG.warn("UTILSERVICE:NullPointerException:The time format is not present in the COMMON_AC properties");
			} catch (IllegalArgumentException e) {
				LOG.warn("UTILSERVICE:IllegalArgumentException:The time format  in the COMMON_AC properties is invalid");
			} catch (ParseException e) {
				LOG.warn("UTILSERVICE:ParseException:The time format  in the COMMON_AC properties is not valid or input date is not in valid format");
			} catch (Exception e) {
				LOG.warn("UTILSERVICE:" + e.getMessage());
			}
		}

		return EvalValue.build(prop
				.getProperty("physical_location_expiry_missing_return_value"));
	}

	/*
	 * This method check whethere the Evalvalue is null or not
	 */

	private IEvalValue isNUll(IEvalValue[] args) {
		IEvalValue nullVariable = args[0];
		if (nullVariable == IEvalValue.NULL)
			return EvalValue.build("true");
		else
			return EvalValue.build("false");
	}

	/*
	 * This method check whethere the multivalue is empty or not
	 */
	private IEvalValue isEmptySet(IEvalValue[] args) {
		IEvalValue empty = args[0];

		if (empty.getType().equals(ValueType.MULTIVAL)
				&& ((IMultivalue) (empty.getValue())).isEmpty())
			return EvalValue.build("true");
		else
			return EvalValue.build("false");
	}

	/*
	 * This method check converts the input string to lowercase
	 */
	private IEvalValue toLowerCase(IEvalValue[] args) {
		IEvalValue toLower = args[0];
		IEvalValue result = IEvalValue.NULL;
		if (!toLower.equals(ValueType.NULL)) {
			if (toLower.getType().equals(ValueType.MULTIVAL)) {

				ArrayList<String> list = new ArrayList<String>();

				IMultivalue value = (IMultivalue) toLower.getValue();

				Iterator<IEvalValue> ievIter = value.iterator();

				while (ievIter.hasNext()) {

					IEvalValue iev = ievIter.next();

					if (iev != null) {

						if (!iev.getValue().toString().isEmpty()) {

							list.add(iev.getValue().toString().toLowerCase());

						}

					}
				}

				result = EvalValue.build(Multivalue.create(list));
			} else if (toLower.getType().equals(ValueType.STRING)) {
				result = EvalValue.build(toLower.getValue().toString()
						.toLowerCase());
			} else {
				result = toLower;
			}
		}
		return result;
	}

	/*
	 * 
	 * This method takes a string with separator and returns a list of strings
	 * 
	 * separtted by the separator
	 */

	private IEvalValue toList(ArrayList<ArrayList<String>> inputList) {

		String toList = null, token = null;

		LOG.info("UtilService toList entered ");

		IEvalValue evalValue = IEvalValue.EMPTY;

		if (inputList.get(0) != null && inputList.get(0).get(0) != null) {

			toList = inputList.get(0).get(0);

		}

		if (inputList.get(1) != null && inputList.get(1).get(0) != null) {

			token = inputList.get(1).get(0);

		}

		LOG.debug("String:" + toList + "  separtor:" + token);

		if (toList != null && token != null) {

			ArrayList<String> evList = new ArrayList<String>();

			StringTokenizer tokens = new StringTokenizer(toList, token);

			while (tokens.hasMoreTokens()) {

				evList.add(tokens.nextToken());

			}

			IMultivalue imv = Multivalue.create(evList, ValueType.STRING);

			evalValue = EvalValue.build(imv);

		}

		return evalValue;

	}

	/*
	 * 
	 * 
	 * 
	 * This method checks whether all the set are equal
	 * 
	 * 
	 * 
	 * private IEvalValue isAllMatch(ArrayList<ArrayList<String>> inputList)
	 * 
	 * throws Exception { LOG.info("UtilService isAllMatch entered "); int mc =
	 * 
	 * getMatchCount(inputList); boolean flag = true; for (ArrayList<String>
	 * 
	 * list : inputList) { if (mc != list.size()) { flag = false; break; } }
	 * 
	 * return EvalValue.build(Boolean.toString(flag)); }
	 */

	/*
	 * 
	 * This method checks whether there is any one element common in the sets
	 */

	private IEvalValue isAnyMatch(ArrayList<ArrayList<String>> inputList)

	throws Exception {

		LOG.info("UtilService isAnyMatch entered ");

		int mc = getMatchCount(inputList);

		boolean flag = false;

		if (mc > 0)

			flag = true;

		return EvalValue.build(Boolean.toString(flag));

	}

	/*
	 * 
	 * This method gives the no elements present in all the sets and returns the
	 * 
	 * value in IEvalValue.
	 */

	private IEvalValue matchCount(ArrayList<ArrayList<String>> inputList)

	throws Exception {

		LOG.info("UtilService matchCount entered ");

		int mc = getMatchCount(inputList);

		return EvalValue.build(mc);

	}

	/*
	 * 
	 * This method gives the no elements present in all the sets. This method is
	 * 
	 * the base method and is invoked by other major funtions for processing.
	 */

	private int getMatchCount(ArrayList<ArrayList<String>> inputList)

	throws Exception {

		LOG.info("UtilService getMatchCount entered ");

		ArrayList<String> matchList = getIntersectedList(inputList.get(0),

		inputList.get(1));

		for (int i = 2; i < inputList.size(); i++) {

			matchList = getIntersectedList(matchList, inputList.get(i));

		}

		return matchList.size();

	}

	/*
	 * 
	 * This method gets two list and returns a list which has common elements in
	 * 
	 * both the list
	 */

	private ArrayList<String> getIntersectedList(ArrayList<String> list1,

	ArrayList<String> list2) {

		LOG.info("UtilService getIntersectedList entered ");

		ArrayList<String> resultList = new ArrayList<String>();

		for (String item : list1) {

			if (list2.contains(item))

				resultList.add(item);

		}

		return resultList;

	}

	/*
	 * 
	 * Process the input data and put in arraylist od string
	 */

	private ArrayList<ArrayList<String>> processValues(IEvalValue[] args)

	throws Exception {

		LOG.info("UtilService processValues entered ");

		ArrayList<ArrayList<String>> sOutData = new ArrayList<ArrayList<String>>();

		for (IEvalValue ieValue : args) {
			LOG.info("ieValue " + ieValue.toString());
			if (null != ieValue) {

				if (ieValue.getType() == ValueType.MULTIVAL) {

					ArrayList<String> list = new ArrayList<String>();

					IMultivalue value = (IMultivalue) ieValue.getValue();

					Iterator<IEvalValue> ievIter = value.iterator();

					while (ievIter.hasNext()) {

						IEvalValue iev = ievIter.next();

						if (iev != null) {

							if (!iev.getValue().toString().isEmpty()) {

								list.add(iev.getValue().toString());

							}

						}

					}

					sOutData.add(list);

				} else if (ieValue.getType() == ValueType.STRING

				&& !ieValue.getValue().toString().isEmpty()) {

					ArrayList<String> list = new ArrayList<String>();

					list.add(ieValue.getValue().toString());

					sOutData.add(list);

				} else if (ieValue.getType() == ValueType.DATE) {

					ArrayList<String> list = new ArrayList<String>();
					String timeFormat = prop
							.getProperty("sharepoint_user_location_timestamp");
					DateFormat df = new SimpleDateFormat(timeFormat);

					list.add(df.format(ieValue.getValue()));

					sOutData.add(list);
				}

			}

		}
		LOG.info("Input Data: " + sOutData);
		return sOutData;

	}

	// For testing purpose

	public static void main(String args[]) throws Exception {

		UtilService plugin = new UtilService();

		plugin.init();

		IEvalValue[] sDataArr = new IEvalValue[1];
		ArrayList<IEvalValue> evs = new ArrayList<IEvalValue>();
		evs.add(EvalValue.build("sap://d10-ci/abc"));
		evs.add(EvalValue
				.build("sap://d10-ci/d10/900/ECC/MM03/ITAR_DIR_1/abc/pqr"));
		evs.add(EvalValue.build("sap://d10-ci/d10/900/ECC/"));
		evs.add(EvalValue.build("sap://d10-ci/d10/900/ECC/MM03/ITAR_DIR_1"));
		evs.add(EvalValue.build("sap://d10-ci/"));
		evs.add(EvalValue.build("sap://d10-ci/d10/900/ECC/MM03/ITAR_DIR_1/"));
		sDataArr[0] = EvalValue.build(Multivalue.create(evs, ValueType.STRING));

		System.out.println(plugin.callFunction("toLower", sDataArr));
		/* ArrayList<IEvalValue> evs = new ArrayList<IEvalValue>(); */

		/*
		 * 
		 * evs.add(EvalValue.build("1")); evs.add(EvalValue.build("2"));
		 * 
		 * evs.add(EvalValue.build("3")); evs.add(EvalValue.build("4"));
		 * 
		 * evs.add(EvalValue.build("5"));
		 * 
		 * 
		 * 
		 * IMultivalue imv = Multivalue.create(evs, ValueType.STRING);
		 * 
		 * sDataArr[0] = EvalValue.build(imv); evs = new
		 * 
		 * ArrayList<IEvalValue>();
		 * 
		 * 
		 * 
		 * evs.add(EvalValue.build("1")); evs.add(EvalValue.build("2"));
		 * 
		 * evs.add(EvalValue.build("3")); evs.add(EvalValue.build("5"));
		 * 
		 * evs.add(EvalValue.build("4"));
		 */

		// IMultivalue imv1 = Multivalue.create(evs, ValueType.STRING);
		/*
		 * sDataArr[0] = EvalValue.build("123;345;678;34;567;879");
		 * 
		 * sDataArr[1] = EvalValue.build(";");
		 * 
		 * sDataArr[0] = EvalValue.build("S-1-1-11-3436"); UserService us = new
		 * UserService(); us.init(); sDataArr[0] =
		 * us.callFunction("getLicenses", sDataArr);
		 * System.out.println(plugin.callFunction("isEmptySet", sDataArr));
		 */
		/*
		 * ArrayList list = new ArrayList(); list.add("1"); list.add("5");
		 * list.add("7"); System.out.println(plugin.join(list, "','"));
		 */

	}

	public static String join(ArrayList<String> list, String separator) {
		return join(list, separator, null, null);
	}

	public static String join(ArrayList<String> list, String separator,
			String prefix, String suffix) {
		StringBuilder sb = new StringBuilder();
		if (prefix != null && !prefix.isEmpty())
			sb.append(prefix);

		if (list.size() <= 0) {
			sb.append("");
		} else {
			int sz = list.size();
			for (int i = 0; i < sz; i++) {
				String str = list.get(i);
				if (i == (sz - 1)) {
					if (str != null && !str.isEmpty())
						sb.append(str);

				} else {
					if (str != null && !str.isEmpty())
						sb.append(str).append(separator);
				}
			}
		}

		if (suffix != null && !suffix.isEmpty())
			sb.append(suffix);

		return sb.toString();
	}

}
