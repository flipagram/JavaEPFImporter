/**
 * 
 */
package com.spacehopperstudios.epf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * @author billy1380
 * 
 */
public class Program {

	private static final String USAGE_FORMAT = "usage = \"usage: %S [-fxrak] [-d db_host] [-u db_user] [-p db_password] [-n db_name]" + "\r\n"
			+ "[-s record_separator] [-t field_separator] [-w regex [-w regex2 [...]]]" + "\r\n"
			+ "[-b regex [-b regex2 [...]]] source_directory [source_directory2 ...]\\";

	private static final String OPTION_SHORT_FLAT = "f";
	private static final String OPTION_FULL_FLAT = "flat";

	private static final String OPTION_SHORT_RESUME = "r";
	private static final String OPTION_FULL_RESUME = "resume";

	private static final String OPTION_SHORT_DBHOST = "d";
	private static final String OPTION_FULL_DBHOST = "dbhost";

	private static final String OPTION_SHORT_DBUSER = "u";
	private static final String OPTION_FULL_DBUSER = "dbuser";

	private static final String OPTION_SHORT_DBPASSWORD = "p";
	private static final String OPTION_FULL_DBPASSWORD = "dbpassword";

	private static final String OPTION_SHORT_DBNAME = "n";
	private static final String OPTION_FULL_DBNAME = "dbname";

	private static final String OPTION_SHORT_RECORDSEPARATOR = "s";
	private static final String OPTION_FULL_RECORDSEPARATOR = "recordseparator";

	private static final String OPTION_SHORT_FIELDSEPARATOR = "t";
	private static final String OPTION_FULL_FIELDSEPARATOR = "fieldseparator";

	private static final String OPTION_SHORT_ALLOWEXTENSIONS = "a";
	private static final String OPTION_FULL_ALLOWEXTENSIONS = "allowextensions";

	private static final String OPTION_SHORT_TABLEPREFIX = "x";
	private static final String OPTION_FULL_TABLEPREFIX = "tableprefix";

	private static final String OPTION_SHORT_WHITELIST = "w";
	private static final String OPTION_FULL_WHITELIST = "whitelist";

	private static final String OPTION_SHORT_BLACKLIST = "b";
	private static final String OPTION_FULL_BLACKLIST = "blacklist";

	private static final String OPTION_SHORT_SKIPKEYVIOLATORS = "k";
	private static final String OPTION_FULL_SKIPKEYVIOLATORS = "skipkeyviolators";

	private static final String VERSION = "1.2.1";
	private static final String DESCRIPTION = "EPFImporter is a tool for importing EPF files into a database.";

	private static final String CONFIG_PATH = "./EPFConfig.json";
	private static final String FLAT_CONFIG_PATH = "./EPFFlatConfig.json";

	public static final String SNAPSHOT_PATH = "./EPFSnapshot.json";
	public static JsonObject SNAPSHOT_DICT = null;

	public static final String SNAPSHOT_DIRSLEFT = "dirsLeft";
	public static final String SNAPSHOT_DIRSTOIMPORT = "dirsToImport";
	public static final String SNAPSHOT_WLIST = "wList";
	public static final String SNAPSHOT_BLIST = "bList";
	public static final String SNAPSHOT_CURRENTDICT = "currentDict";
	public static final String SNAPSHOT_DIRPATH = "dirPath";
	public static final String SNAPSHOT_FILESLEFT = "filesLeft";
	public static final String SNAPSHOT_FILESIMPORTED = "filesImported";

	private static final String LOGS_FOLDER = "EPFLogs";

	private static Logger LOGGER = Logger.getLogger(Program.class.getName());

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) {

		createLogFolder();

		createDefaultConfigFile();
		createDefaultFlatConfigFile();

		Map<String, Object> optionsMap = new HashMap<String, Object>();

		CommandLine parsed = parseArgs(args);

		overwriteDefaults(optionsMap, parsed);

		List<String> dirsToImport = parsed.getArgList();
		if ((dirsToImport == null || dirsToImport.size() == 0)
				&& (optionsMap.get(OPTION_FULL_RESUME) != null && !((Boolean) optionsMap.get(OPTION_FULL_RESUME)).booleanValue())) {
			System.out.println(String.format(USAGE_FORMAT, "epfimporter"));
			return;
		}

		// TODO: rollover the log file

		JsonObject config = getConfig(optionsMap.get(OPTION_FULL_FLAT) == null ? false : ((Boolean) optionsMap.get(OPTION_FULL_FLAT)).booleanValue());

		for (Entry<String, JsonElement> entry : config.entrySet()) {
			if (optionsMap.get(entry.getKey()) == null) {
				String value = entry.getValue().getAsString();
				if ("true".equals(value) || "false".equals(value)) {
					optionsMap.put(entry.getKey(), Boolean.valueOf(value));
				} else {
					optionsMap.put(entry.getKey(), value);
				}
			}
		}

		Map<String, List<String>> failedFilesMap = new HashMap<String, List<String>>();

		String tablePrefix = (String) optionsMap.get(OPTION_FULL_TABLEPREFIX);
		List<String> wList = (List<String>) optionsMap.get(OPTION_FULL_WHITELIST);
		List<String> bList = (List<String>) optionsMap.get(OPTION_FULL_BLACKLIST);
		String recordSep = (String) optionsMap.get(OPTION_FULL_RECORDSEPARATOR);
		String fieldSep = (String) optionsMap.get(OPTION_FULL_FIELDSEPARATOR);
		boolean allowExtensions = ((Boolean) optionsMap.get(OPTION_FULL_ALLOWEXTENSIONS)).booleanValue();

		JsonArray stringArray;

		SNAPSHOT_DICT.add(SNAPSHOT_DIRSTOIMPORT, stringArray = new JsonArray());
		for (String dirPath : dirsToImport) {
			stringArray.add(new JsonPrimitive(dirPath));
		}

		SNAPSHOT_DICT.add(SNAPSHOT_DIRSLEFT, stringArray = new JsonArray());
		for (String dirPath : dirsToImport) {
			stringArray.add(new JsonPrimitive(dirPath));
		}

		Date startTime = new Date();

		if (((Boolean) optionsMap.get(OPTION_FULL_RESUME)).booleanValue()) {
			SNAPSHOT_DICT = getSnapShot();
			tablePrefix = SNAPSHOT_DICT.get(OPTION_FULL_TABLEPREFIX).getAsString();
			JsonObject currentDict = SNAPSHOT_DICT.get(SNAPSHOT_CURRENTDICT).getAsJsonObject();

			if (LOGGER.isInfoEnabled()) {
				LOGGER.info(String.format("Resuming import for %s", currentDict.get(SNAPSHOT_DIRPATH).getAsString()));
			}

			List<String> failedFiles = resumeImport(currentDict, tablePrefix, (String) optionsMap.get(OPTION_FULL_DBHOST),
					(String) optionsMap.get(OPTION_FULL_DBUSER), (String) optionsMap.get(OPTION_FULL_DBPASSWORD), (String) optionsMap.get(OPTION_FULL_DBNAME),
					((Boolean) optionsMap.get(OPTION_FULL_SKIPKEYVIOLATORS)).booleanValue(), recordSep, fieldSep);

			if (failedFiles != null && failedFiles.size() > 0) {
				failedFilesMap.put(currentDict.get(SNAPSHOT_DIRPATH).getAsString(), failedFiles);
			}

			stringArray = SNAPSHOT_DICT.get(SNAPSHOT_DIRSLEFT).getAsJsonArray();

			if (dirsToImport != null) {
				dirsToImport.clear();
			}

			for (JsonElement dirPath : stringArray) {
				if (dirsToImport == null) {
					dirsToImport = new ArrayList<String>();
				}

				dirsToImport.add(dirPath.getAsString());
			}

			stringArray = SNAPSHOT_DICT.get(SNAPSHOT_WLIST).getAsJsonArray();

			if (wList != null) {
				wList.clear();
			}

			for (JsonElement mask : stringArray) {
				if (wList == null) {
					wList = new ArrayList<String>();
				}

				wList.add(mask.getAsString());
			}

			stringArray = SNAPSHOT_DICT.get(SNAPSHOT_BLIST).getAsJsonArray();

			if (bList != null) {
				bList.clear();
			}

			for (JsonElement mask : stringArray) {
				if (bList == null) {
					bList = new ArrayList<String>();
				}

				bList.add(mask.getAsString());
			}
		}

		if (dirsToImport != null && dirsToImport.size() > 0) {
			for (String dirPath : dirsToImport) {
				List<String> failedFiles = doImport(dirPath, (String) optionsMap.get(OPTION_FULL_DBHOST), (String) optionsMap.get(OPTION_FULL_DBUSER),
						(String) optionsMap.get(OPTION_FULL_DBPASSWORD), (String) optionsMap.get(OPTION_FULL_DBNAME), wList, bList, tablePrefix,
						allowExtensions, ((Boolean) optionsMap.get(OPTION_FULL_SKIPKEYVIOLATORS)).booleanValue(), recordSep, fieldSep);

				if (failedFiles != null && failedFiles.size() > 0) {
					failedFilesMap.put(dirPath, failedFiles);
				}
			}
		}

		Date endTime = new Date();

		long ts = endTime.getTime() - startTime.getTime();

		if (LOGGER.isEnabledFor(Priority.WARN)) {

			StringBuffer buffer = new StringBuffer();

			for (String key : failedFilesMap.keySet()) {

				buffer.append(String.format("    %s/%s", key, failedFilesMap.get(key)));
				buffer.append("\n");

			}

			if (buffer.length() > 0) {
				LOGGER.warn(String.format("The following files encountered errors and were not imported:\n    %s", buffer.toString()));
			}
		}

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Total import time for all directories: %d", ts));
		}

	}

	private static JsonObject getSnapShot() {

		return null;
	}

	private static void createDefaultFlatConfigFile() {
		if (!(new File(FLAT_CONFIG_PATH)).exists()) {
			JsonObject flatOptions = new JsonObject();

			flatOptions.add(OPTION_FULL_DBHOST, new JsonPrimitive("localhost"));
			flatOptions.add(OPTION_FULL_DBUSER, new JsonPrimitive("epfimporter"));
			flatOptions.add(OPTION_FULL_DBPASSWORD, new JsonPrimitive("epf123"));
			flatOptions.add(OPTION_FULL_DBNAME, new JsonPrimitive("epf"));
			flatOptions.add(OPTION_FULL_ALLOWEXTENSIONS, new JsonPrimitive(true));
			flatOptions.add(OPTION_FULL_TABLEPREFIX, new JsonPrimitive("epfflat"));
			JsonArray stringArray = null;
			flatOptions.add(OPTION_FULL_WHITELIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive(".*?"));
			flatOptions.add(OPTION_FULL_BLACKLIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive("^\\."));
			flatOptions.add(OPTION_FULL_RECORDSEPARATOR, new JsonPrimitive("\n"));
			flatOptions.add(OPTION_FULL_FIELDSEPARATOR, new JsonPrimitive("\t"));

			OutputStream configStream = null;
			OutputStreamWriter configWriter = null;

			try {
				configStream = new FileOutputStream(FLAT_CONFIG_PATH);
				configWriter = new OutputStreamWriter(configStream);
				configWriter.write(flatOptions.toString());
			} catch (FileNotFoundException e) {
				if (LOGGER.isEnabledFor(Priority.ERROR)) {
					LOGGER.error("", e);
				}
			} catch (IOException e) {
				if (LOGGER.isEnabledFor(Priority.ERROR)) {
					LOGGER.error("", e);
				}
			} finally {

				if (configWriter != null) {
					try {
						configWriter.close();
					} catch (IOException e) {
						if (LOGGER.isEnabledFor(Priority.ERROR)) {
							LOGGER.error("", e);
						}
					}
				}

				if (configStream != null) {
					try {
						configStream.close();
					} catch (IOException e) {
						if (LOGGER.isEnabledFor(Priority.ERROR)) {
							LOGGER.error("", e);
						}
					}
				}
			}
		}

	}

	private static void createDefaultConfigFile() {
		if (!(new File(CONFIG_PATH)).exists()) {

			JsonObject defaultOptions = new JsonObject();

			defaultOptions.add(OPTION_FULL_DBHOST, new JsonPrimitive("localhost"));
			defaultOptions.add(OPTION_FULL_DBUSER, new JsonPrimitive("epfimporter"));
			defaultOptions.add(OPTION_FULL_DBPASSWORD, new JsonPrimitive("epf123"));
			defaultOptions.add(OPTION_FULL_DBNAME, new JsonPrimitive("epf"));
			defaultOptions.add(OPTION_FULL_ALLOWEXTENSIONS, new JsonPrimitive(false));
			defaultOptions.add(OPTION_FULL_TABLEPREFIX, new JsonPrimitive("epf"));
			JsonArray stringArray = null;
			defaultOptions.add(OPTION_FULL_WHITELIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive(".*?"));
			defaultOptions.add(OPTION_FULL_BLACKLIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive("^\\."));
			defaultOptions.add(OPTION_FULL_RECORDSEPARATOR, new JsonPrimitive("\u0002\n"));
			defaultOptions.add(OPTION_FULL_FIELDSEPARATOR, new JsonPrimitive("\u0001"));

			OutputStream configStream = null;
			OutputStreamWriter configWriter = null;
			try {
				configStream = new FileOutputStream(CONFIG_PATH);
				configWriter = new OutputStreamWriter(configStream);
				configWriter.write(defaultOptions.toString());

			} catch (FileNotFoundException e) {
				if (LOGGER.isEnabledFor(Priority.ERROR)) {
					LOGGER.error("", e);
				}
			} catch (IOException e) {
				if (LOGGER.isEnabledFor(Priority.ERROR)) {
					LOGGER.error("", e);
				}
			} finally {
				if (configWriter != null) {
					try {
						configWriter.close();
					} catch (IOException e) {
						if (LOGGER.isEnabledFor(Priority.ERROR)) {
							LOGGER.error("", e);
						}
					}
				}

				if (configStream != null) {
					try {
						configStream.close();
					} catch (IOException e) {
						if (LOGGER.isEnabledFor(Priority.ERROR)) {
							LOGGER.error("", e);
						}
					}
				}

			}
		}
	}

	private static void overwriteDefaults(Map<String, Object> defaults, CommandLine commandLine) {
		if (commandLine.hasOption(OPTION_SHORT_FLAT)) {
			defaults.put(OPTION_FULL_FLAT, Boolean.parseBoolean(commandLine.getOptionValue(OPTION_SHORT_FLAT)));
		}

		if (commandLine.hasOption(OPTION_SHORT_RESUME)) {
			defaults.put(OPTION_FULL_RESUME, Boolean.parseBoolean(commandLine.getOptionValue(OPTION_SHORT_RESUME)));
		}

		if (commandLine.hasOption(OPTION_SHORT_DBHOST)) {
			defaults.put(OPTION_FULL_DBHOST, commandLine.getOptionValue(OPTION_SHORT_DBHOST));
		}

		if (commandLine.hasOption(OPTION_SHORT_DBUSER)) {
			defaults.put(OPTION_FULL_DBUSER, commandLine.getOptionValue(OPTION_SHORT_DBUSER));
		}

		if (commandLine.hasOption(OPTION_SHORT_DBPASSWORD)) {
			defaults.put(OPTION_FULL_DBPASSWORD, commandLine.getOptionValue(OPTION_SHORT_DBPASSWORD));
		}

		if (commandLine.hasOption(OPTION_SHORT_DBNAME)) {
			defaults.put(OPTION_FULL_DBNAME, commandLine.getOptionValue(OPTION_SHORT_DBNAME));
		}

		if (commandLine.hasOption(OPTION_SHORT_RECORDSEPARATOR)) {
			defaults.put(OPTION_FULL_RECORDSEPARATOR, commandLine.getOptionValue(OPTION_SHORT_RECORDSEPARATOR));
		}

		if (commandLine.hasOption(OPTION_SHORT_FIELDSEPARATOR)) {
			defaults.put(OPTION_FULL_FIELDSEPARATOR, commandLine.getOptionValue(OPTION_SHORT_FIELDSEPARATOR));
		}

		if (commandLine.hasOption(OPTION_SHORT_ALLOWEXTENSIONS)) {
			defaults.put(OPTION_FULL_ALLOWEXTENSIONS, Boolean.parseBoolean(commandLine.getOptionValue(OPTION_SHORT_ALLOWEXTENSIONS)));
		}

		if (commandLine.hasOption(OPTION_SHORT_TABLEPREFIX)) {
			defaults.put(OPTION_FULL_TABLEPREFIX, commandLine.getOptionValue(OPTION_SHORT_TABLEPREFIX));
		}

		if (commandLine.hasOption(OPTION_SHORT_WHITELIST)) {
			defaults.put(OPTION_FULL_WHITELIST, commandLine.getOptionValue(OPTION_SHORT_WHITELIST));
		}

		if (commandLine.hasOption(OPTION_SHORT_BLACKLIST)) {
			defaults.put(OPTION_FULL_BLACKLIST, commandLine.getOptionValue(OPTION_SHORT_BLACKLIST));
		}

		if (commandLine.hasOption(OPTION_SHORT_SKIPKEYVIOLATORS)) {
			defaults.put(OPTION_FULL_SKIPKEYVIOLATORS, Boolean.parseBoolean(commandLine.getOptionValue(OPTION_SHORT_SKIPKEYVIOLATORS)));
		}

	}

	private static void createLogFolder() {
		File logFolder = new File(LOGS_FOLDER);

		if (!logFolder.exists()) {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Not found log folder @ %s", LOGS_FOLDER));
			}

			if (logFolder.mkdirs()) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info(String.format("Created log folder @ %s", LOGS_FOLDER));
				}
			}
		} else {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(String.format("Fount log folder @ %s", LOGS_FOLDER));
			}
		}
	}

	private static JsonObject getFileConfig(String filePath) {
		JsonParser jsonParser = new JsonParser();
		InputStream configStream = null;
		JsonElement config = null;

		try {
			configStream = new FileInputStream(filePath);
			InputStreamReader configReader = new InputStreamReader(configStream);
			config = jsonParser.parse(configReader);
		} catch (FileNotFoundException e) {
			if (LOGGER.isEnabledFor(Priority.ERROR)) {
				LOGGER.error("", e);
			}
		} finally {
			if (configStream != null) {
				try {
					configStream.close();
				} catch (IOException e) {
					if (LOGGER.isEnabledFor(Priority.ERROR)) {
						LOGGER.error("", e);
					}
				}
			}
		}

		return config == null ? null : config.getAsJsonObject();
	}

	private static JsonObject getConfig(boolean isFlat) {
		return getFileConfig(isFlat ? FLAT_CONFIG_PATH : CONFIG_PATH);
	}

	private static CommandLine parseArgs(String[] args) {
		Options options = new Options();
		try {
			CommandLineParser cliParser = new GnuParser();

			options.addOption(OPTION_SHORT_FLAT, OPTION_FULL_FLAT, false, "Import EPF Flat files, using values from EPFFlat.config if not overridden");
			options.addOption(OPTION_SHORT_RESUME, OPTION_FULL_RESUME, false,
					"Resume the most recent import according to the relevant .json status file (EPFStatusIncremental.json if -i, otherwise EPFStatusFull.json)");
			options.addOption(OPTION_SHORT_DBHOST, OPTION_FULL_DBHOST, true, "The hostname of the database (default is localhost)");
			options.addOption(OPTION_SHORT_DBUSER, OPTION_FULL_DBUSER, true,
					"The user which will execute the database commands; must have table create/drop priveleges");
			options.addOption(OPTION_SHORT_DBPASSWORD, OPTION_FULL_DBPASSWORD, true, "The user's password for the database");
			options.addOption(OPTION_SHORT_DBNAME, OPTION_FULL_DBNAME, true, "The name of the database to connect to");
			options.addOption(OPTION_SHORT_RECORDSEPARATOR, OPTION_FULL_RECORDSEPARATOR, true, "The string separating records in the file");
			options.addOption(OPTION_SHORT_FIELDSEPARATOR, OPTION_FULL_FIELDSEPARATOR, true, "The string separating fields in the file");
			options.addOption(OPTION_SHORT_ALLOWEXTENSIONS, OPTION_FULL_ALLOWEXTENSIONS, false, "Include files with dots in their names in the import");
			options.addOption(OPTION_SHORT_TABLEPREFIX, OPTION_FULL_TABLEPREFIX, true,
					"Optional prefix which will be added to all table names, e.g. \"MyPrefix_video_translation\"");
			options.addOption(OPTION_SHORT_WHITELIST, OPTION_FULL_WHITELIST, true,
					"A regular expression to add to the whiteList; repeated -w arguments will append");
			options.addOption(OPTION_SHORT_BLACKLIST, OPTION_FULL_BLACKLIST, true,
					"A regular expression to add to the whiteList; repeated -b arguments will append");
			options.addOption(OPTION_SHORT_SKIPKEYVIOLATORS, OPTION_FULL_SKIPKEYVIOLATORS, false,
					"Ignore inserts which would violate a primary key constraint; only applies to full imports");

			return cliParser.parse(options, args);

		} catch (ParseException e) {
			if (LOGGER.isEnabledFor(Priority.ERROR)) {
				LOGGER.error("", e);
			}

			System.out.println(e.toString());
			System.out.println(String.format(USAGE_FORMAT, "epfimporter"));

		}

		return null;
	}

	public static List<String> doImport(String directoryPath, String dbHost, String dbUser, String dbPassword, String dbName, List<String> whiteList,
			List<String> blackList, String tablePrefix, boolean allowExtensions, boolean skipKeyViolators, String recordSep, String fieldSep) {

		// if (! allowExtensions) {
		// blackList.add(".*\\..*?");
		// }
		//
		// // wListRe = (r"|".join(whiteList) if whiteList else r"$a^") #The latter can never match anything
		// // bListRe = (r"|".join(blackList) if blackList else r"$a^") #The latter can never match anything
		// // wMatcher = re.compile(wListRe)
		// // bMatcher = re.compile(bListRe)
		//
		// File dirPath = new File(directoryPath);
		// String [] fileList = dirPath.list(); // list paths
		// //filter the list down to the entries matching our whitelist/blacklist
		// // List<String> fileList = null; // [f for f in fileList if (wMatcher.search(f) and not bMatcher.search(f))]
		// // fileList.sort();
		// List<String> filesLeft = fileList.copy();
		// List <String>filesImported = new ArrayList<String>();
		// List <String >failedFiles = new ArrayList<String>();
		//
		// SNAPSHOT_DICT.add(OPTION_FULL_TABLEPREFIX, new JsonPrimitive(tablePrefix);
		//
		// JsonArray stringArray;
		// SNAPSHOT_DICT.add(SNAPSHOT_WLIST, stringArray = new JsonArray());
		// for (String mask : whiteList) {
		// stringArray.add(new JsonPrimitive(mask));
		// }
		//
		// SNAPSHOT_DICT.add(SNAPSHOT_BLIST, stringArray = new JsonArray());
		// for (String mask : blackList) {
		// stringArray.add(new JsonPrimitive(mask));
		// }
		//
		//
		// stringArray = SNAPSHOT_DICT.get(SNAPSHOT_DIRSLEFT).getAsJsonArray();
		//
		// List<String> dirPathList = new ArrayList<String>();
		// for (JsonElement element : stringArray) {
		// dirPathList.add(element.getAsString());
		// }
		//
		// dirPathList.remove(dirPath)
		//
		// currentDict = SNAPSHOT_DICT['currentDict']
		// currentDict['recordSep'] = recordDelim
		// currentDict['fieldSep'] = fieldDelim
		// currentDict['dirPath'] = dirPath
		// currentDict['filesToImport'] = fileList
		// currentDict['filesLeft'] = filesLeft
		// currentDict['filesImported'] = filesImported
		// currentDict['failedFiles'] = failedFiles
		//
		// _dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH)
		//
		// pathList = [os.path.join(dirPath, fileName) for fileName in fileList]
		//
		// Date startTime = new Date();
		//
		// LOGGER.info(String.format("Starting import of %s...", dirPath));
		// for (String aPath : pathList) {
		// fName = os.path.basename(aPath);
		// //In order to keep supposedly "matching" warnings from being suppressed during future
		// //ingests, we need to clear the module's warning registry before each ingest
		// try {
		// EPFIngester.__warningregistry__.clear()
		// catch (AttributeException e) {
		// throw e;
		// }
		//
		// try {
		// ing = EPFIngester.Ingester(aPath,
		// tablePrefix=tablePrefix,
		// dbHost=dbHost,
		// dbUser=dbUser,
		// dbPassword=dbPassword,
		// dbName=dbName,
		// recordDelim=recordDelim,
		// fieldDelim=fieldDelim);
		// } catch( Exception e) {
		// if (LOGGER.isLoggable(Level.SEVERE)) {
		// LOGGER.log(Level.SEVERE, String.format("Unable to create EPFIngester for %s", fName), e);
		// }
		//
		// failedFiles.add(fName);
		// _dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);
		//
		// continue;
		// }
		//
		// try {
		// ing.ingest(skipKeyViolators);
		// filesLeft.remove(fName);
		// filesImported.add(fName);
		//
		// _dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);
		// }catch (SQLException e) {
		// failedFiles.append(fName)
		// _dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH)
		// continue;
		// }
		//
		// Date endTime = new Date();
		// long ts = endTime.getTime() - startTime.getTime();
		// dirName = os.path.basename(dirPath);
		// LOGGER.info(String.format("Import of %s completed at: %s", dirName, endTime.strftime(EPFIngester.DATETIME_FORMAT)));
		// LOGGER.info(String.format("Total import time for %s: %d" , dirName, ts));
		// if (failedFiles != null && failedFiles.size() > 0) {
		// LOGGER.warning("The following files encountered errors and were not imported:\n %s",
		// ", ".join(failedFiles));
		// }
		// return failedFiles;

		return null;
	}

	public static List<String> resumeImport(JsonObject currentDict, String tablePrefix, String dbHost, String dbUser, String dbPassword, String dbName,
			boolean skipKeyViolators, String recordSep, String fieldSep) {

		String dirPath = currentDict.get(SNAPSHOT_DIRPATH).getAsString();
		JsonArray stringArray = currentDict.get(SNAPSHOT_FILESLEFT).getAsJsonArray();

		List<String> filesLeft = new ArrayList<String>();
		for (JsonElement element : stringArray) {
			filesLeft.add(element.getAsString());
		}

		recordSep = currentDict.get(OPTION_FULL_RECORDSEPARATOR).getAsString();
		fieldSep = currentDict.get(OPTION_FULL_FIELDSEPARATOR).getAsString();

		List<String> wList = new ArrayList<String>();// = ["^%s$" % aFile for aFile in filesLeft] //anchor the regexes for exact matches

		stringArray = currentDict.get(SNAPSHOT_FILESIMPORTED).getAsJsonArray();
		List<String> filesImported = new ArrayList<String>();
		for (JsonElement element : stringArray) {
			filesImported.add(element.getAsString());
		}

		List<String> bList = new ArrayList<String>();// = ["^%s$" % aFile for aFile in filesImported] //anchor the regexes for exact matches

		return doImport(dirPath, dbHost, dbUser, dbPassword, dbName, wList, bList, tablePrefix, false, skipKeyViolators, recordSep, fieldSep);

	}
}
