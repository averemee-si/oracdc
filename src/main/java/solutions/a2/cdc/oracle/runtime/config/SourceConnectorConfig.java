/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.cdc.oracle.runtime.config;

import static solutions.a2.cdc.oracle.OraCdcParameters.FIRST_CHANGE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_INT_ERROR;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_INT_RESTORE;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_INT_SKIP;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_ERROR;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_SKIP;
import static solutions.a2.cdc.oracle.OraCdcParameters.KEY_OVERRIDE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.LAST_SEQ_NOTIFIER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.LOB_TRANSFORM_CLASS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.NUMBER_MAP_PREFIX;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_FULL;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_FULL_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALF;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALFQUARTER;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALFQUARTER_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALF_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_QUARTER;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_QUARTER_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_TRANSACTION_IMPL_CHRONICLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_ANY_UNIQUE;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_INT_ANY_UNIQUE;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_INT_WELL_DEFINED;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_WELL_DEFINED;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_ASM;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_BFILE;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_SMB;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_SSH;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_TRANSFER;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_NAME_CONVERT_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_MAVERICK;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_MINA;
import static solutions.a2.cdc.oracle.OraCdcParameters.SUPPLEMENTAL_LOGGING_ALL;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_LIST_STYLE_STATIC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TEMP_DIR_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_INT_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_TABLE;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.LastProcessedSeqNotifier;
import solutions.a2.cdc.oracle.OraCdcKeyOverrideTypes;
import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class SourceConnectorConfig {

	public static final boolean LINUX;
	static {
		LINUX = Strings.CI.endsWith(System.getProperty("os.name"), "inux");
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SourceConnectorConfig.class);

	private final Map<String, Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> numberColumnsMap = new LinkedHashMap<>();
	private final int incompleteDataTolerance;
	private final int topicNameStyle;
	private final int pkType;
	private final Map<String, OraCdcKeyOverrideTypes> keyOverrideMap;
	private final Map<String, String> keyOverrideIndexMap;
	private final Path queuesRoot;
	private final long startScn;
	private final int transactionsThreshold;
	private LastProcessedSeqNotifier lpsn = null;
	private OraCdcLobTransformationsIntf transformLobsImpl = null;
	// Redo Miner only!
//	private boolean msWindows = false;
	private String fileSeparator = File.separator;
	private final String fileNameConvertParam;
	private boolean fileNameConversion = false;
	private Map<String, String> fileNameConversionMap;
	private boolean fileNameConversionInited = false;
	private static final int STATIC_OBJ_IDS       = 0x0001;
	private static final int OFF_HEAP_MEMORY      = 0x0002;
	private static final int LOG_MINER            = 0x0004;
	private static final int MS_WINDOWS           = 0x0008;
	private static final int MEDIUM_ASM           = 0x0010;
	private static final int MEDIUM_SSH           = 0x0020;
	private static final int MEDIUM_SMB           = 0x0040;
	private static final int MEDIUM_BFILE         = 0x0080;
	private static final int MEDIUM_TRANSFER      = 0x0100;
	private static final int SSH_MAVERICK         = 0x0200;
	private static final int SSH_MINA             = 0x0400;
	private static final int SUPPLEMENTAL_ALL     = 0x0800;
	private static final int OFF_HEAP_FULL        = 0x1000;
	private static final int OFF_HEAP_HALF        = 0x2000;
	private static final int OFF_HEAP_QUARTER     = 0x4000;
	private static final int OFF_HEAP_HALFQUARTER = 0x8000;
	private int flags = 0;

	SourceConnectorConfig(ParamsRecord paramsRecord) {
		//
		// numberMapParams
		//
		paramsRecord.numberMapParams().forEach((param, value) -> {
			var lastDotPos = StringUtils.lastIndexOf(param, '.');
			var column = StringUtils.substring(param, lastDotPos + 1);
			var fqn = StringUtils.substring(param, 0, lastDotPos);
			if (!numberColumnsMap.containsKey(fqn)) {
				numberColumnsMap.put(fqn, Triple.of(
						new ArrayList<Pair<String, OraColumn>>(),
						new HashMap<String, OraColumn>(),
						new ArrayList<Pair<String, OraColumn>>()));
			}
			var jdbcType = Types.NULL;
			var scale = 0;
			switch (StringUtils.upperCase(StringUtils.trim(StringUtils.substringBefore(value, '(')))) {
				case "BOOL", "BOOLEAN"    -> jdbcType = Types.BOOLEAN;
				case "BYTE", "TINYINT"    -> jdbcType = Types.TINYINT;
				case "SHORT", "SMALLINT"  -> jdbcType = Types.SMALLINT;
				case "INT", "INTEGER"     -> jdbcType = Types.INTEGER;
				case "LONG", "BIGINT"     -> jdbcType = Types.BIGINT;
				case "FLOAT"              -> jdbcType = Types.FLOAT;
				case "DOUBLE"             -> jdbcType = Types.DOUBLE;
				case "DECIMAL", "NUMERIC" -> {
					var precisionScale = StringUtils.trim(StringUtils.substringBetween(value, "(", ")"));
					if (StringUtils.countMatches(precisionScale, ',') == 1) {
						try {
							scale = Integer.parseInt(StringUtils.trim(StringUtils.split(precisionScale, ',')[1]));
						} catch (Exception e) {
							LOGGER.error(
									"""
									
									=====================
									Unable to parse decimal scale in '{}' for parameter '{}'!
									=====================
									
									""", value, NUMBER_MAP_PREFIX + param);
							scale = -1;
						}
						if (scale > -1)
							jdbcType = Types.DECIMAL;
					} else
						LOGGER.error(
								"""
								
								=====================
								Mapping '{}' for parameter '' will be ignored!
								=====================
								
								""", value, NUMBER_MAP_PREFIX + param);
				}
				default ->
					LOGGER.error(
							"""
							
							=====================
							Unable to recognize datatype '{}' for parameter '{}'!
							=====================
						
							""", value, NUMBER_MAP_PREFIX + param);
			}
			if (jdbcType != Types.NULL) {
				if (Strings.CS.endsWith(column, "%")) {
					numberColumnsMap.get(fqn).getLeft().add(Pair.of(
							StringUtils.substring(column, 0, column.length() - 1),
							new OraColumn(column, jdbcType, scale)));
				} else if (Strings.CS.startsWith(column, "%")) {
					numberColumnsMap.get(fqn).getRight().add(Pair.of(
							StringUtils.substring(column, 1),
							new OraColumn(column, jdbcType, scale)));
				} else {
					numberColumnsMap.get(fqn).getMiddle().put(column, new OraColumn(column, jdbcType, scale));
				}
			}
		});
		//
		// incompleteDataTolerance
		//
		switch (paramsRecord.incompleteDataTolerance()) {
			case INCOMPLETE_REDO_TOLERANCE_ERROR -> incompleteDataTolerance = INCOMPLETE_REDO_INT_ERROR;
			case INCOMPLETE_REDO_TOLERANCE_SKIP  -> incompleteDataTolerance = INCOMPLETE_REDO_INT_SKIP;
			default                              -> incompleteDataTolerance = INCOMPLETE_REDO_INT_RESTORE;
		}
		//
		// topicNameStyle
		//
		switch (paramsRecord.topicNameStyle()) {
			case TOPIC_NAME_STYLE_TABLE            -> topicNameStyle = TOPIC_NAME_STYLE_INT_TABLE;
			case TOPIC_NAME_STYLE_SCHEMA_TABLE     -> topicNameStyle = TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
			case TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE -> topicNameStyle = TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE;
			default                                -> topicNameStyle = -1;
		}
		//
		// pkType
		//
		switch (paramsRecord.pkType()) {
			case PK_TYPE_WELL_DEFINED -> pkType = PK_TYPE_INT_WELL_DEFINED;
			case PK_TYPE_ANY_UNIQUE   -> pkType = PK_TYPE_INT_ANY_UNIQUE;
			default                   -> pkType = -1;
		}
		//
		// lastProcessedSeqNotifier
		//
		if (StringUtils.isBlank(paramsRecord.lastProcessedSeqNotifier()))
			lpsn = null;
		else {
			try {
				var clazz = Class.forName(paramsRecord.lastProcessedSeqNotifier());
				var constructor = clazz.getConstructor();
				lpsn = (LastProcessedSeqNotifier) constructor.newInstance();
			} catch (ClassNotFoundException nfe) {
				LOGGER.error(
						"""
						
						=====================
						Class '{}' specified as the parameter '{}' value was not found.
						{}
						=====================
						
						""", paramsRecord.lastProcessedSeqNotifier(),
						LAST_SEQ_NOTIFIER_PARAM, ExceptionUtils.getExceptionStackTrace(nfe));
			} catch (NoSuchMethodException nsme) {
				LOGGER.error(
						"""
						
						=====================
						Unable to get default constructor for the class '{}'.
						{}
						
						=====================
						
						""", paramsRecord.lastProcessedSeqNotifier(),
						ExceptionUtils.getExceptionStackTrace(nsme));
			} catch (SecurityException | 
					InvocationTargetException | 
					IllegalAccessException | 
					InstantiationException e) {
				LOGGER.error(
						"""
						
						=====================
						'{}' while instantinating the class '{}'.
						{}
						
						=====================
						
						""", e.getMessage(),
						paramsRecord.lastProcessedSeqNotifier(),
						ExceptionUtils.getExceptionStackTrace(e));
			}
		}
		//
		// keyOverride
		//
		keyOverrideMap = new HashMap<>();
		keyOverrideIndexMap = new HashMap<>();
		paramsRecord.keyOverride().forEach(token -> {
			try {
				var pair = StringUtils.split(token, "=");
				var fullTableName = StringUtils.upperCase(pair[0]);
				var overrideValue = pair[1];
				if (Strings.CI.equals(overrideValue, "NOKEY")) {
					keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.NOKEY);
				} else if (Strings.CI.equals(overrideValue, "ROWID")) {
					keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.ROWID);
				} else if (Strings.CI.startsWith(overrideValue, "INDEX")) {
					keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.INDEX);
					keyOverrideIndexMap.put(fullTableName,
							StringUtils.substringBetween(overrideValue, "(", ")"));
				} else {
					LOGGER.error(
							"""
							
							=====================
							Incorrect value {} for parameter {}!
							=====================
							
							""", token, KEY_OVERRIDE_PARAM);
				}
			} catch (Exception e) {
				LOGGER.error(
						"""
						
						=====================
						Unable to parse '{}' for parameter {}!
						{}
						
						=====================
						
						""", token, KEY_OVERRIDE_PARAM,
						ExceptionUtils.getExceptionStackTrace(e));
			}
		});
		//
		// lobTransformClass
		//
		try {
			var clazz = Class.forName(paramsRecord.lobTransformClass());
			var constructor = clazz.getConstructor();
			transformLobsImpl = (OraCdcLobTransformationsIntf) constructor.newInstance();
		} catch (ClassNotFoundException nfe) {
			LOGGER.error(
					"""
					
					=====================
					Class '{}' specified as the parameter '{}' value was not found.
					{}
					=====================
					
					""", paramsRecord.lobTransformClass(),
					LOB_TRANSFORM_CLASS_PARAM, ExceptionUtils.getExceptionStackTrace(nfe));
		} catch (NoSuchMethodException nsme) {
			LOGGER.error(
					"""
					
					=====================
					Unable to get default constructor for the class '{}'.
					{}
					
					=====================
					
					""", paramsRecord.lobTransformClass(),
					ExceptionUtils.getExceptionStackTrace(nsme));
		} catch (SecurityException | 
				InvocationTargetException | 
				IllegalAccessException | 
				InstantiationException e) {
			LOGGER.error(
					"""
					
					=====================
					'{}' while instantinating the class '{}'.
					{}
					
					=====================
					
					""", e.getMessage(),
					paramsRecord.lobTransformClass(),
					ExceptionUtils.getExceptionStackTrace(e));
		}
		//
		// tempDir
		//
		if (Files.isDirectory(Paths.get(paramsRecord.tempDir()))) {
			if (!Files.isWritable(Paths.get(paramsRecord.tempDir()))) {
				var tempDir = System.getProperty("java.io.tmpdir");
				LOGGER.error(
						"""
						
						=====================
						Parameter '{}' points to non-writable directory '{}'.
						The value of '{}' will be set to '{}'.
						=====================
						
						""",
						TEMP_DIR_PARAM, paramsRecord.tempDir(), TEMP_DIR_PARAM, tempDir);
				queuesRoot = FileSystems.getDefault().getPath(tempDir);
			} else
				queuesRoot = FileSystems.getDefault().getPath(paramsRecord.tempDir());
		} else {
			var ok = false;
			var tempDir = System.getProperty("java.io.tmpdir");
			try {
				Files.createDirectories(Paths.get(paramsRecord.tempDir()));
				ok = true;
			} catch (IOException | UnsupportedOperationException | SecurityException  e) {
				LOGGER.error(
						"""
						
						=====================
						Unable to create directory! Parameter {} points to non-existent or invalid directory {}.
						The value of '{}' will be set to '{}'.
						=====================
						
						""",
						TEMP_DIR_PARAM, paramsRecord.tempDir(), TEMP_DIR_PARAM, tempDir);
			}
			queuesRoot = FileSystems.getDefault().getPath(ok ? paramsRecord.tempDir() : tempDir);
		}
		//
		// startScn
		//
		try {
			startScn = Long.parseUnsignedLong(paramsRecord.startScn());
		} catch (NumberFormatException nfe) {
			LOGGER.error(
					"""
					
					=====================
					Unable to parse value '{}' of parameter '{}' as unsigned long!
					=====================
					
					""", paramsRecord.startScn(), FIRST_CHANGE_PARAM);
			throw new IllegalArgumentException(nfe);
		}
		//
		// tableListStyle
		//
		if (Strings.CI.equals(TABLE_LIST_STYLE_STATIC, paramsRecord.tableListStyle()))
				flags |= STATIC_OBJ_IDS;
		//
		// transactionsThreshold
		//
		if (paramsRecord.transactionsThreshold() > 0)
			transactionsThreshold = paramsRecord.transactionsThreshold();
		else if (LINUX) {
			int maxMapCount = 0x10000;
			try (InputStream is = Files.newInputStream(Path.of("/proc/sys/vm/max_map_count"))) {
				final byte[] buffer = new byte[0x10];
				final int size = is.read(buffer, 0, buffer.length);
				maxMapCount = Integer.parseInt(StringUtils.trim(new String(buffer, 0, size)));
			} catch (IOException | NumberFormatException e) {
				LOGGER.error(
						"""
						
						=====================
						Unable to read and parse value of vm.max_map_count from  '/proc/sys/vm/max_map_count'!
						Exception: {}
						{}
						=====================
						
						""",
						e.getMessage(), ExceptionUtils.getExceptionStackTrace(e));
			}
			transactionsThreshold = ((int)(maxMapCount / 0x10)) * 0x7;
		} else
			transactionsThreshold = 0x7000;
		//
		// transactionImpl
		//
		if (Strings.CI.equals(paramsRecord.transactionImpl(), ORA_TRANSACTION_IMPL_CHRONICLE))
			flags |= OFF_HEAP_MEMORY;
		//
		// redoFileNameConversion
		//
		fileNameConvertParam = paramsRecord.redoFileNameConversion();
		//
		// medium
		//
		if (Strings.CI.equals(paramsRecord.medium(), REDO_FILE_MEDIUM_ASM))
			flags |= MEDIUM_ASM;
		else if (Strings.CI.equals(paramsRecord.medium(), REDO_FILE_MEDIUM_SSH))
			flags |= MEDIUM_SSH;
		else if (Strings.CI.equals(paramsRecord.medium(), REDO_FILE_MEDIUM_SMB))
			flags |= MEDIUM_SMB;
		else if (Strings.CI.equals(paramsRecord.medium(), REDO_FILE_MEDIUM_BFILE))
			flags |= MEDIUM_BFILE;
		else if (Strings.CI.equals(paramsRecord.medium(), REDO_FILE_MEDIUM_TRANSFER))
			flags |= MEDIUM_TRANSFER;
		//
		// sshProvider
		//
		if (Strings.CI.equals(paramsRecord.sshProvider(), SSH_PROVIDER_MAVERICK))
			flags |= SSH_MAVERICK;
		else if (Strings.CI.equals(paramsRecord.sshProvider(), SSH_PROVIDER_MINA))
			flags |= SSH_MINA;
		//
		// supplementalAll
		//
		if (Strings.CI.equals(paramsRecord.supplementalAll(), SUPPLEMENTAL_LOGGING_ALL)) {
			flags |= SUPPLEMENTAL_ALL;
		}
		//
		// offHeapSize
		//
		if (Strings.CI.equals(paramsRecord.offHeapSize(), OFFHEAP_SIZE_FULL))
			flags |= OFF_HEAP_FULL;
		else if (Strings.CI.equals(paramsRecord.offHeapSize(), OFFHEAP_SIZE_HALF))
			flags |= OFF_HEAP_HALF;
		else if (Strings.CI.equals(paramsRecord.offHeapSize(), OFFHEAP_SIZE_QUARTER))
			flags |= OFF_HEAP_QUARTER;
		else if (Strings.CI.equals(paramsRecord.offHeapSize(), OFFHEAP_SIZE_HALFQUARTER))
			flags |= OFF_HEAP_HALFQUARTER;
	}

	List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
		tableNumberMapping(final String pdbName, final String tableOwner, final String tableName) {
		var fqn =  tableOwner + "." + tableName;
		if (pdbName == null) {
			if (numberColumnsMap.containsKey(fqn)) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(1);
				result.add(numberColumnsMap.get(fqn));
				return result;
			} else {
				return null;
			}
		} else {
			var forAll = numberColumnsMap.get(fqn);
			var exact = numberColumnsMap.get(pdbName + "." + fqn);
			if (exact != null && forAll == null) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(1);
				result.add(exact);
				return result;
			} else if (exact != null && forAll != null) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(2);
				result.add(exact);
				result.add(forAll);
				return result;
			} else if (forAll != null) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(1);
				result.add(forAll);
				return result;
			} else {
				return null;
			}
		}
	}

	OraColumn columnNumberMapping(
			List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
				numberRemap, final String columnName) {
		if (numberRemap != null)
			for (int i = 0; i < numberRemap.size(); i++) {
				Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>> reDefs =
						numberRemap.get(i);
				OraColumn result = reDefs.getMiddle().get(columnName);
				if (result != null) {
					return result;
				} else if ((result = remapUsingPattern(reDefs.getLeft(), columnName, true)) != null) {
					return result;
				} else if ((result = remapUsingPattern(reDefs.getRight(), columnName, false)) != null) {
					return result;
				}
			}
		return null;
	}

	private OraColumn remapUsingPattern(final List<Pair<String, OraColumn>> patterns, final String columnName, final boolean startsWith) {
		for (final Pair<String, OraColumn> pattern : patterns)
			if (startsWith &&
					Strings.CS.startsWith(columnName, pattern.getKey()))
				return pattern.getValue();
			else if (!startsWith &&
					Strings.CS.endsWith(columnName, pattern.getKey()))
				return pattern.getValue();
		return null;
	}

	int incompleteDataTolerance() {
		return incompleteDataTolerance;
	}

	int topicNameStyle() {
		return topicNameStyle;
	}

	int pkType() {
		return pkType;
	}

	LastProcessedSeqNotifier lastProcessedSeqNotifier() {
		return lpsn;
	}

	Entry<OraCdcKeyOverrideTypes, String> getKeyOverrideType(final String fqtn) {
		return Map.entry(
				keyOverrideMap.getOrDefault(fqtn, OraCdcKeyOverrideTypes.NONE),
				keyOverrideIndexMap.getOrDefault(fqtn, ""));
	}

	OraCdcLobTransformationsIntf transformLobsImpl() {
		return transformLobsImpl;
	}

	Path queuesRoot() {
		return queuesRoot;
	}

	long startScn() {
		return startScn;
	}

	boolean staticObjIds() {
		return (flags & STATIC_OBJ_IDS) > 0;
	}

	int transactionsThreshold() {
		return transactionsThreshold;
	}

	boolean useOffHeapMemory() {
		return (flags & OFF_HEAP_MEMORY) > 0;
	}

	boolean logMiner() {
		return (flags & LOG_MINER) > 0;
	}

	void logMiner(final boolean logMiner) {
		if (logMiner)
			flags |= LOG_MINER;
		else
			flags &= (~LOG_MINER);
	}

	void msWindows(final boolean msWindows) {
		if (msWindows) {
			flags |= MS_WINDOWS;
			fileSeparator = "\\";
		} else
			flags &= (~MS_WINDOWS);
	}

	String convertRedoFileName(final String originalName, final boolean bfile) {
		if (bfile) {
			return StringUtils.substringAfterLast(originalName, fileSeparator);
		} else {
			if (!fileNameConversionInited) {
				if (StringUtils.isNotEmpty(fileNameConvertParam) &&
						StringUtils.contains(fileNameConvertParam, '=')) {
					var elements = StringUtils.split(fileNameConvertParam, ',');
					var newSize = 0;
					var processElement = new boolean[elements.length];
					for (var i = 0; i < elements.length; i++) {
						if (Strings.CS.contains(elements[i], "=")) {
							elements[i] = StringUtils.trim(elements[i]);
							processElement[i] = true;
							newSize += 1;
						} else {
							processElement[i] = false;
						}
					}
					if (newSize > 0) {
						fileNameConversionMap = new HashMap<>();
						for (int i = 0; i < elements.length; i++) {
							if (processElement[i]) {
								fileNameConversionMap.put(
										Strings.CS.appendIfMissing(
										StringUtils.trim(StringUtils.substringBefore(elements[i], "=")),
										fileSeparator),
										Strings.CS.appendIfMissing(
										StringUtils.trim(StringUtils.substringAfter(elements[i], "=")),
										fileSeparator));
							}
						}
						fileNameConversion = true;
					} else {
						fileNameConversion = false;
						fileNameConversionMap = null;
					}
					elements = null;
					processElement = null;
				}
			}
			if (fileNameConversion) {
				var maxPrefixSize = -1;
				String originalPrefix = null;
				for (var prefix : fileNameConversionMap.keySet()) {
					if (Strings.CS.startsWith(originalName, prefix)) {
						if (prefix.length() > maxPrefixSize) {
							maxPrefixSize = prefix.length();
							originalPrefix = prefix;
						}
					}
				}
				if (maxPrefixSize == -1) {
					LOGGER.error(
							"""
							
							=====================
							Unable to convert filename '{}' using parameter {}={} !
							Original filename will be returned!
							=====================
							
							""",
							originalName, REDO_FILE_NAME_CONVERT_PARAM, fileNameConvertParam);
					return originalName;
				} else {
					var replacementPrefix =  fileNameConversionMap.get(originalPrefix);
					if ((flags & MS_WINDOWS) > 0)
						return  Strings.CS.replace(
								Strings.CS.replace(originalName, originalPrefix, replacementPrefix),
								"\\",
								"/");
					else
						return Strings.CS.replace(originalName, originalPrefix, replacementPrefix);
				}
			} else {
				return originalName;
			}
		}
	}

	boolean useAsm() {
		return (flags & MEDIUM_ASM) > 0;
	}

	boolean useSsh() {
		return (flags & MEDIUM_SSH) > 0;
	}

	boolean useSmb() {
		return (flags & MEDIUM_SMB) > 0;
	}

	boolean useBfile() {
		return (flags & MEDIUM_BFILE) > 0;
	}

	boolean useFileTransfer() {
		return (flags & MEDIUM_TRANSFER) > 0;
	}

	boolean sshProviderMaverick() {
		return (flags & SSH_MAVERICK) > 0;
	}

	boolean sshProviderMina() {
		return (flags & SSH_MINA) > 0;
	}

	boolean supplementalLogAll() {
		return (flags & SUPPLEMENTAL_ALL) > 0;
	}

	int[] offHeapSize() {
		if ((flags & LOG_MINER) > 0)
			return OFFHEAP_SIZE_FULL_INT;
		else {
			if ((flags & OFF_HEAP_FULL) > 0)
				return OFFHEAP_SIZE_FULL_INT;
			else if ((flags & OFF_HEAP_HALF) > 0)
				return OFFHEAP_SIZE_HALF_INT;
			else if ((flags & OFF_HEAP_QUARTER) > 0)
				return OFFHEAP_SIZE_QUARTER_INT;
			else
				return OFFHEAP_SIZE_HALFQUARTER_INT;
		}
	}

}
