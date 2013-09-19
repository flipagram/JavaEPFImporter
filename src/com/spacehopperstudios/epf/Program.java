/**
 * 
 */
package com.spacehopperstudios.epf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.google.common.base.Joiner;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.spacehopperstudios.epf.ingest.Ingester;
import com.spacehopperstudios.epf.ingest.IngesterProvider;
import com.spacehopperstudios.epf.parse.V3Parser;

public class Program {

	private static final String USAGE_FORMAT = "usage: %s [-fxrak] [-d db_host] [-u db_user] [-p db_password] [-n db_name]" + "\r\n"
			+ "[-s record_separator] [-t field_separator] [-w regex [-w regex2 [...]]]" + "\r\n"
			+ "[-b regex [-b regex2 [...]]] source_directory [source_directory2 ...]";

	private static final String OPTION_SHORT_FLAT = "f";
	private static final String OPTION_FULL_FLAT = "flat";

	private static final String OPTION_SHORT_RESUME = "r";
	private static final String OPTION_FULL_RESUME = "resume";

	private static final String OPTION_SHORT_INGESTERTYPE = "z";
	private static final String OPTION_FULL_INGESTERTYPE = "ingestertype";

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

	public static final String SNAPSHOT_DIRSLEFT = "dirsLeft";
	public static final String SNAPSHOT_DIRSTOIMPORT = "dirsToImport";
	public static final String SNAPSHOT_WLIST = "wList";
	public static final String SNAPSHOT_BLIST = "bList";
	public static final String SNAPSHOT_CURRENTDICT = "currentDict";
	public static final String SNAPSHOT_DIRPATH = "dirPath";
	public static final String SNAPSHOT_FILESLEFT = "filesLeft";
	public static final String SNAPSHOT_FILESIMPORTED = "filesImported";
	public static final String SNAPSHOT_FILESTOIMPORT = "filesToImport";
	public static final String SNAPSHOT_FAILEDFILES = "failedFiles";

	private static final String LOGS_FOLDER = "EPFLogs";
	private static final String LOGGER_CONFIG_PATH = "./EPFLogger.xml";

	private static final Logger LOGGER;

	/**
	 * Snapshot is updated throughout the import; it is used for resuming interrupted imports
	 */
	public static JsonObject SNAPSHOT_DICT;

	// FULL_STATUS_PATH = "./EPFStatusFull.json"
	// INCREMENTAL_STATUS_PATH = "./EPFStatusIncremental.json"
	// FULL_STATUS_DICT = {"tablePrefix":None, "dirsToImport":[], "dirsLeft":[], "currentDict":{}}
	// INCREMENTAL_STATUS_DICT = {"tablePrefix":None, "dirsToImport":[], "dirsLeft":[], "currentDict":{}}
	//
	// STATUS_MAP = {"full":(FULL_STATUS_DICT, FULL_STATUS_PATH),
	// "incremental":(INCREMENTAL_STATUS_DICT, INCREMENTAL_STATUS_PATH)}

	static {

		SNAPSHOT_DICT = new JsonObject();

		SNAPSHOT_DICT.add(OPTION_FULL_TABLEPREFIX, new JsonNull());
		SNAPSHOT_DICT.add(SNAPSHOT_DIRSTOIMPORT, new JsonArray());
		SNAPSHOT_DICT.add(SNAPSHOT_DIRSLEFT, new JsonArray());
		SNAPSHOT_DICT.add(SNAPSHOT_CURRENTDICT, new JsonObject());

		configureLogger();

		LOGGER = Logger.getLogger(Program.class.getName());

		// Create a directory for rotating logs
		createLogFolder();
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
			flatOptions.add(OPTION_FULL_INGESTERTYPE, new JsonPrimitive("MySQL"));

			JsonArray stringArray = null;
			flatOptions.add(OPTION_FULL_WHITELIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive(".*?"));

			flatOptions.add(OPTION_FULL_BLACKLIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive("^\\."));

			flatOptions.add(OPTION_FULL_RECORDSEPARATOR, new JsonPrimitive("\n"));
			flatOptions.add(OPTION_FULL_FIELDSEPARATOR, new JsonPrimitive("\t"));

			dumpDict(flatOptions, FLAT_CONFIG_PATH);
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
			defaultOptions.add(OPTION_FULL_INGESTERTYPE, new JsonPrimitive("MySQL"));

			JsonArray stringArray = null;
			defaultOptions.add(OPTION_FULL_WHITELIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive(".*?"));

			defaultOptions.add(OPTION_FULL_BLACKLIST, stringArray = new JsonArray());
			stringArray.add(new JsonPrimitive("^\\."));

			defaultOptions.add(OPTION_FULL_RECORDSEPARATOR, new JsonPrimitive("\u0002\n"));
			defaultOptions.add(OPTION_FULL_FIELDSEPARATOR, new JsonPrimitive("\u0001"));

			dumpDict(defaultOptions, CONFIG_PATH);
		}
	}

	private static void overwriteDefaults(Map<String, Object> defaults, CommandLine commandLine) {
		List<String> stringList = null;

		if (commandLine.hasOption(OPTION_SHORT_FLAT)) {
			defaults.put(OPTION_FULL_FLAT, Boolean.TRUE);
		}

		if (commandLine.hasOption(OPTION_SHORT_RESUME)) {
			defaults.put(OPTION_FULL_RESUME, Boolean.TRUE);
		}

		if (commandLine.hasOption(OPTION_FULL_INGESTERTYPE)) {
			defaults.put(OPTION_FULL_INGESTERTYPE, commandLine.getOptionValue(OPTION_FULL_INGESTERTYPE));
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
			defaults.put(OPTION_FULL_ALLOWEXTENSIONS, Boolean.TRUE);
		}

		if (commandLine.hasOption(OPTION_SHORT_TABLEPREFIX)) {
			defaults.put(OPTION_FULL_TABLEPREFIX, commandLine.getOptionValue(OPTION_SHORT_TABLEPREFIX));
		}

		if (commandLine.hasOption(OPTION_SHORT_INGESTERTYPE)) {
			defaults.put(OPTION_FULL_INGESTERTYPE, commandLine.getOptionValue(OPTION_SHORT_INGESTERTYPE));
		}

		if (commandLine.hasOption(OPTION_SHORT_WHITELIST)) {
			defaults.put(OPTION_FULL_WHITELIST, stringList = new ArrayList<String>());
			Collections.addAll(stringList, commandLine.getOptionValues(OPTION_SHORT_WHITELIST));
		}

		if (commandLine.hasOption(OPTION_SHORT_BLACKLIST)) {
			defaults.put(OPTION_FULL_BLACKLIST, stringList = new ArrayList<String>());
			Collections.addAll(stringList, commandLine.getOptionValues(OPTION_SHORT_BLACKLIST));
		}

		if (commandLine.hasOption(OPTION_SHORT_SKIPKEYVIOLATORS)) {
			defaults.put(OPTION_FULL_SKIPKEYVIOLATORS, Boolean.TRUE);
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

	private static void configureLogger() {
		DOMConfigurator.configure(LOGGER_CONFIG_PATH);
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
			LOGGER.error(String.format("Error; config file %s not found", filePath), e);
		} finally {
			if (configStream != null) {
				try {
					configStream.close();
				} catch (IOException e) {
					LOGGER.error("Error; config stream could not be closed", e);
				}
			}
		}

		return config == null ? null : config.getAsJsonObject();
	}

	private static JsonObject getConfig(boolean isFlat) {
		return getFileConfig(isFlat ? FLAT_CONFIG_PATH : CONFIG_PATH);
	}

	private static CommandLine parseArgs(String[] args, Map<String, Object> optionsMap) {
		Options options = new Options();
		try {
			CommandLineParser cliParser = new GnuParser();

			options.addOption(OPTION_SHORT_FLAT, OPTION_FULL_FLAT, false, "Import EPF Flat files, using values from EPFFlat.config if not overridden");
			optionsMap.put(OPTION_FULL_FLAT, Boolean.FALSE);

			options.addOption(OPTION_SHORT_RESUME, OPTION_FULL_RESUME, false,
					"Resume the most recent import according to the relevant .json status file (EPFStatusIncremental.json if -i, otherwise EPFStatusFull.json)");
			optionsMap.put(OPTION_FULL_RESUME, Boolean.FALSE);

			options.addOption(OPTION_SHORT_DBHOST, OPTION_FULL_DBHOST, true, "The hostname of the database (default is localhost)");

			options.addOption(OPTION_SHORT_DBUSER, OPTION_FULL_DBUSER, true,
					"The user which will execute the database commands; must have table create/drop priveleges");

			options.addOption(OPTION_SHORT_DBPASSWORD, OPTION_FULL_DBPASSWORD, true, "The user's password for the database");

			options.addOption(OPTION_SHORT_DBNAME, OPTION_FULL_DBNAME, true, "The name of the database to connect to");

			options.addOption(OPTION_SHORT_RECORDSEPARATOR, OPTION_FULL_RECORDSEPARATOR, true, "The string separating records in the file");

			options.addOption(OPTION_SHORT_FIELDSEPARATOR, OPTION_FULL_FIELDSEPARATOR, true, "The string separating fields in the file");

			options.addOption(OPTION_SHORT_ALLOWEXTENSIONS, OPTION_FULL_ALLOWEXTENSIONS, false, "Include files with dots in their names in the import");
			optionsMap.put(OPTION_FULL_ALLOWEXTENSIONS, Boolean.FALSE);

			options.addOption(OPTION_SHORT_TABLEPREFIX, OPTION_FULL_TABLEPREFIX, true,
					"Optional prefix which will be added to all table names, e.g. \"MyPrefix_video_translation\"");

			options.addOption(OPTION_SHORT_INGESTERTYPE, OPTION_FULL_INGESTERTYPE, true,
					"Ingester type name, currently the only ones supported are \"MySQL\" and \"DataStore\"");

			options.addOption(OPTION_SHORT_WHITELIST, OPTION_FULL_WHITELIST, true,
					"A regular expression to add to the whiteList; repeated -w arguments will append");

			options.addOption(OPTION_SHORT_BLACKLIST, OPTION_FULL_BLACKLIST, true,
					"A regular expression to add to the whiteList; repeated -b arguments will append");

			options.addOption(OPTION_SHORT_SKIPKEYVIOLATORS, OPTION_FULL_SKIPKEYVIOLATORS, false,
					"Ignore inserts which would violate a primary key constraint; only applies to full imports");
			optionsMap.put(OPTION_FULL_SKIPKEYVIOLATORS, Boolean.FALSE);

			return cliParser.parse(options, args);

		} catch (ParseException e) {
			LOGGER.error("Error parsing command line", e);

			if (LOGGER.isInfoEnabled()) {
				LOGGER.info(DESCRIPTION);
				LOGGER.info(VERSION);
				LOGGER.info(String.format(USAGE_FORMAT, "java -jar epfimporter.jar"));
			}

		}

		return null;
	}

	/**
	 * Perform a full import of the EPF files in the directory specified by directoryPath.
	 * 
	 * importMode can be 'full' or 'incremental'
	 * 
	 * whiteList is a sequence of regular expressions. Only files whose basenames (i.e., the last element in the path) match one or more of the regexes in
	 * whiteList will be imported. For example, whiteList=[".*song.*", ".*video.*"] would result in all files containing "song" or "video" anywhere in the
	 * filename being imported, and the rest being ignored. To import only exact matches, precede the name with a caret (^) and follow it with a dollar sign
	 * ($), e.g. "^video$".
	 * 
	 * The default is for all files to be whitelisted.
	 * 
	 * blackList works similarly; any filenames matching any of the items in blackList will be excluded from the import, even if they are matched in whiteList.
	 * By default, any filename with a dot (".") in it will be excluded. Since EPF filenames never include a dot, this permits placing any file with an
	 * extension (e.g., .txt) in the directory without disrupting the import.
	 * 
	 * Returns a list of any files for which the import failed (empty if all succeeded)
	 */
	public static List<String> doImport(String ingesterType, String directoryPath, String dbHost, String dbUser, String dbPassword, String dbName,
			List<String> whiteList, List<String> blackList, String tablePrefix, boolean allowExtensions, boolean skipKeyViolators, String recordDelim,
			String fieldDelim) {

		if (!allowExtensions) {
			blackList.add(".*\\..*?");
		}

		String wListRe;
		if (whiteList != null) {
			wListRe = Joiner.on("|").join(whiteList);
		} else {
			wListRe = "$a^"; // The latter can never match anything
		}

		String bListRe;
		if (blackList != null) {
			bListRe = Joiner.on("|").join(blackList);
		} else {
			bListRe = "$a^"; // The latter can never match anything
		}

		Pattern wMatcher = Pattern.compile(wListRe);
		Pattern bMatcher = Pattern.compile(bListRe);

		File dirPath = new File(directoryPath);
		List<String> fileList = new ArrayList<String>();
		Collections.addAll(fileList, dirPath.list()); // list paths
		// filter the list down to the entries matching our whitelist/blacklist

		String f;
		for (int i = fileList.size() - 1; i >= 0; i--) {
			f = fileList.get(i);
			if (!wMatcher.matcher(f).matches() || bMatcher.matcher(f).matches()) {
				fileList.remove(f);
			}
		}

		// commented out because - not sure what the relevance is
		// fileList.sort();

		List<String> filesLeft = new ArrayList<String>(fileList);
		List<String> filesImported = new ArrayList<String>();
		List<String> failedFiles = new ArrayList<String>();

		SNAPSHOT_DICT.add(OPTION_FULL_INGESTERTYPE, new JsonPrimitive(ingesterType));

		SNAPSHOT_DICT.add(OPTION_FULL_TABLEPREFIX, new JsonPrimitive(tablePrefix));

		JsonArray stringArray;
		SNAPSHOT_DICT.add(SNAPSHOT_WLIST, stringArray = new JsonArray());
		for (String mask : whiteList) {
			stringArray.add(new JsonPrimitive(mask));
		}

		SNAPSHOT_DICT.add(SNAPSHOT_BLIST, stringArray = new JsonArray());
		for (String mask : blackList) {
			stringArray.add(new JsonPrimitive(mask));
		}

		// remove this directory from the "left to do" directories

		stringArray = SNAPSHOT_DICT.get(SNAPSHOT_DIRSLEFT).getAsJsonArray();

		JsonArray anotherArray = new JsonArray();

		for (JsonElement jsonElement : stringArray) {
			if (!dirPath.getAbsolutePath().equals(jsonElement.getAsString())) {
				anotherArray.add(jsonElement);
			}
		}

		SNAPSHOT_DICT.add(SNAPSHOT_DIRSTOIMPORT, anotherArray);

		JsonObject currentDict = SNAPSHOT_DICT.get(SNAPSHOT_CURRENTDICT).getAsJsonObject();

		currentDict.add(OPTION_FULL_RECORDSEPARATOR, new JsonPrimitive(recordDelim));
		currentDict.add(OPTION_FULL_FIELDSEPARATOR, new JsonPrimitive(fieldDelim));
		currentDict.add(SNAPSHOT_DIRPATH, new JsonPrimitive(dirPath.getAbsolutePath()));

		currentDict.add(SNAPSHOT_FILESTOIMPORT, stringArray = new JsonArray());
		for (String file : fileList) {
			stringArray.add(new JsonPrimitive(file));
		}

		currentDict.add(SNAPSHOT_FILESLEFT, stringArray = new JsonArray());
		for (String file : filesLeft) {
			stringArray.add(new JsonPrimitive(file));
		}

		currentDict.add(SNAPSHOT_FILESIMPORTED, stringArray = new JsonArray());
		for (String file : filesImported) {
			stringArray.add(new JsonPrimitive(file));
		}

		currentDict.add(SNAPSHOT_FAILEDFILES, stringArray = new JsonArray());
		for (String file : failedFiles) {
			stringArray.add(new JsonPrimitive(file));
		}

		dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);

		List<String> pathList = new ArrayList<String>();
		for (String fileName : fileList) {
			pathList.add(dirPath + File.separator + fileName);
		}

		Date startTime = new Date();
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Starting import of %s...", dirPath.getAbsolutePath()));
		}

		for (String aPath : pathList) {
			String fName = (new File(aPath)).getName();
			// // In order to keep supposedly "matching" warnings from being suppressed during future
			// // ingests, we need to clear the module's warning registry before each ingest
			// try:
			// EPFIngester.__warningregistry__.clear()
			// except AttributeError:
			// pass

			Ingester ing;
			try {
				ing = IngesterProvider.getNamed(ingesterType);
				V3Parser parser = new V3Parser();
				parser.init(aPath, V3Parser.DEFAULT_TYPE_MAP, recordDelim, fieldDelim);
				ing.init(aPath, parser, tablePrefix, dbHost, dbUser, dbPassword, dbName, recordDelim, fieldDelim);

			} catch (Exception e) {
				LOGGER.error(String.format("Unable to create EPFIngester for %s", fName), e);
				failedFiles.add(fName);
				dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);
				continue;
			}

			try {
				ing.ingest(skipKeyViolators);

				filesLeft.remove(fName);
				currentDict.add(SNAPSHOT_FILESLEFT, stringArray = new JsonArray());
				for (String file : filesLeft) {
					stringArray.add(new JsonPrimitive(file));
				}

				filesImported.add(fName);
				currentDict.add(SNAPSHOT_FILESIMPORTED, stringArray = new JsonArray());
				for (String file : filesImported) {
					stringArray.add(new JsonPrimitive(file));
				}

				dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);
			} catch (RuntimeException e) {
				if (e.getCause() instanceof SQLException) {
				} else {
					LOGGER.error("An error occured while ingesting data.", e);
				}

				failedFiles.add(fName);
				dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);
				continue;
			} catch (Exception e) {
				LOGGER.error("An error occured while ingesting data.", e);
				failedFiles.add(fName);
				dumpDict(SNAPSHOT_DICT, SNAPSHOT_PATH);
				continue;
			}
		}

		Date endTime = new Date();
		String dirName = dirPath.getName();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Import of %s completed at: %s", dirName, TimeHelper.timeText(endTime)
			/* Ingester.DATETIME_FORMAT endTime) */));
			LOGGER.info(String.format("Total import time for %s: %s", dirName, TimeHelper.durationText(startTime, endTime)));
		}

		if (failedFiles != null && failedFiles.size() > 0) {
			LOGGER.warn(String.format("The following files encountered errors and were not imported:\n %s", Joiner.on(", ").join(failedFiles)));
		}

		return failedFiles;
	}

	/**
	 * Resume an interrupted full import based on the values in currentDict, which will normally be the currentDict unarchived from the EPFSnapshot.json file.
	 */
	public static List<String> resumeImport(JsonObject currentDict, String ingesterType, String tablePrefix /* = null */, String dbHost /* = "localhost" */,
			String dbUser /*
						 * = "epfimporter"
						 */, String dbPassword /* = "epf123" */, String dbName /* = "epf" */, boolean skipKeyViolators /* = false */, String recordDelim /*
																																						 * =
																																						 * "\u0002\n"
																																						 */,
			String fieldDelim /* ="\u0001" */) {

		String dirPath = currentDict.get(SNAPSHOT_DIRPATH).getAsString();
		JsonArray stringArray = currentDict.get(SNAPSHOT_FILESLEFT).getAsJsonArray();

		List<String> filesLeft = new ArrayList<String>();
		for (JsonElement element : stringArray) {
			filesLeft.add(element.getAsString());
		}

		recordDelim = currentDict.get(OPTION_FULL_RECORDSEPARATOR).getAsString();
		fieldDelim = currentDict.get(OPTION_FULL_FIELDSEPARATOR).getAsString();

		List<String> wList = new ArrayList<String>();

		for (String aFile : filesLeft) {
			wList.add(String.format("^%s$", aFile)); // anchor the regexes for exact matches
		}

		stringArray = currentDict.get(SNAPSHOT_FILESIMPORTED).getAsJsonArray();
		List<String> filesImported = new ArrayList<String>();
		for (JsonElement element : stringArray) {
			filesImported.add(element.getAsString());
		}

		List<String> bList = new ArrayList<String>();
		for (String aFile : filesImported) {
			bList.add(String.format("^%s$", aFile)); // anchor the regexes for exact matches
		}

		return doImport(ingesterType, dirPath, dbHost, dbUser, dbPassword, dbName, wList, bList, tablePrefix, false, skipKeyViolators, recordDelim, fieldDelim);

	}

	/**
	 * Opens the file at filePath (creating it if it doesn't exist, overwriting if not), writes aDict to it in json format, then closes it
	 */
	private static void dumpDict(JsonObject aDict, String filePath) {
		LOGGER.debug(String.format("Dumping dictionary: %s", aDict.toString()));
		LOGGER.debug(String.format("json path: %s", filePath));

		OutputStream configStream = null;
		OutputStreamWriter configWriter = null;
		try {
			configStream = new FileOutputStream(filePath);
			configWriter = new OutputStreamWriter(configStream);
			configWriter.write(aDict.toString());

		} catch (FileNotFoundException e) {
			LOGGER.error(String.format("Error creating file %s", filePath), e);
		} catch (IOException e) {
			LOGGER.error(String.format("Error writing file %s", filePath), e);
		} finally {
			if (configWriter != null) {
				try {
					configWriter.close();
				} catch (IOException e) {
					LOGGER.error(String.format("Error closing file %s", filePath), e);
				}
			}

			if (configStream != null) {
				try {
					configStream.close();
				} catch (IOException e) {
					LOGGER.error(String.format("Error closing file %s", filePath), e);
				}
			}
		}
	}

	/**
	 * Entry point for command-line execution
	 */
	public static void main(String[] args) {

		// If the default config file doesn't exist, create it using these values.
		createDefaultConfigFile();

		// likewise for the EPF Flat config file
		createDefaultFlatConfigFile();

		Map<String, Object> optionsMap = new HashMap<String, Object>();

		CommandLine parsed = parseArgs(args, optionsMap);

		overwriteDefaults(optionsMap, parsed);

		@SuppressWarnings("unchecked")
		List<String> dirsToImport = parsed.getArgList();
		if ((dirsToImport == null || dirsToImport.size() == 0) && !((Boolean) optionsMap.get(OPTION_FULL_RESUME)).booleanValue()) {
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info(DESCRIPTION);
				LOGGER.info(VERSION);
				LOGGER.info(String.format(USAGE_FORMAT, "epfimporter"));
			}

			return;
		}

		JsonObject config = getConfig(optionsMap.get(OPTION_FULL_FLAT) == null ? false : ((Boolean) optionsMap.get(OPTION_FULL_FLAT)).booleanValue());

		for (Entry<String, JsonElement> entry : config.entrySet()) {
			if (optionsMap.get(entry.getKey()) == null) {
				String value = entry.getValue().getAsString();
				if ("true".equals(value) || "false".equals(value)) {
					optionsMap.put(entry.getKey(), Boolean.valueOf(value));
				} else {
					if (entry.getValue().isJsonArray()) {
						List<String> stringArray = new ArrayList<String>();

						for (JsonElement element : entry.getValue().getAsJsonArray()) {
							stringArray.add(element.getAsString());
						}

						optionsMap.put(entry.getKey(), stringArray);
					} else {
						optionsMap.put(entry.getKey(), value);
					}
				}
			}
		}

		Map<String, List<String>> failedFilesMap = new HashMap<String, List<String>>();

		String tablePrefix = (String) optionsMap.get(OPTION_FULL_TABLEPREFIX);

		String ingesterType = (String) optionsMap.get(OPTION_FULL_INGESTERTYPE);
		
		if (ingesterType == null) {
			ingesterType = "MySQL";
		}

		@SuppressWarnings("unchecked")
		List<String> wList = (List<String>) optionsMap.get(OPTION_FULL_WHITELIST);

		@SuppressWarnings("unchecked")
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
			SNAPSHOT_DICT = loadSnapShot();

			tablePrefix = SNAPSHOT_DICT.get(OPTION_FULL_TABLEPREFIX).getAsString();
			JsonObject currentDict = SNAPSHOT_DICT.get(SNAPSHOT_CURRENTDICT).getAsJsonObject();

			if (LOGGER.isInfoEnabled()) {
				LOGGER.info(String.format("Resuming import for %s", currentDict.get(SNAPSHOT_DIRPATH).getAsString()));
			}

			List<String> failedFiles = resumeImport(currentDict, ingesterType, tablePrefix, (String) optionsMap.get(OPTION_FULL_DBHOST),
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
				List<String> failedFiles = doImport(ingesterType, dirPath, (String) optionsMap.get(OPTION_FULL_DBHOST),
						(String) optionsMap.get(OPTION_FULL_DBUSER), (String) optionsMap.get(OPTION_FULL_DBPASSWORD),
						(String) optionsMap.get(OPTION_FULL_DBNAME), wList, bList, tablePrefix, allowExtensions,
						((Boolean) optionsMap.get(OPTION_FULL_SKIPKEYVIOLATORS)).booleanValue(), recordSep, fieldSep);

				if (failedFiles != null && failedFiles.size() > 0) {
					failedFilesMap.put(dirPath, failedFiles);
				}
			}
		}

		Date endTime = new Date();

		StringBuffer buffer = new StringBuffer();

		for (String key : failedFilesMap.keySet()) {
			buffer.append(String.format("    %s/%s", key, failedFilesMap.get(key)));
			buffer.append("\n");
		}

		if (buffer.length() > 0) {
			LOGGER.warn(String.format("The following files encountered errors and were not imported:\n    %s", buffer.toString()));
		}

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Total import time for all directories: %s", TimeHelper.durationText(startTime, endTime)));
		}

	}

	private static JsonObject loadSnapShot() {

		FileReader reader = null;

		try {
			reader = new FileReader(SNAPSHOT_PATH);
			return (new JsonParser()).parse(reader).getAsJsonObject();
		} catch (FileNotFoundException e) {
			LOGGER.error(String.format("Error locating file %s", SNAPSHOT_PATH), e);
		}

		return SNAPSHOT_DICT;
	}
}
