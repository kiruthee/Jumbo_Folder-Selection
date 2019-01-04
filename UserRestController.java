package com.hexaware.jumbo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.xml.sax.SAXException;

import com.hexaware.jumbo.dao.JdbcTemplateJobStatusDao;
import com.hexaware.jumbo.dao.JobStatus;
import com.hexaware.jumbo.pojo.BatchDetailsTO;
import com.hexaware.jumbo.pojo.ChildNodeTO;
import com.hexaware.jumbo.pojo.DBConfigTO;
import com.hexaware.jumbo.pojo.JoinCustomTO;
import com.hexaware.jumbo.pojo.ProjectTO;
import com.hexaware.jumbo.pojo.RerunResponse;
import com.hexaware.jumbo.pojo.RunDateTO;
import com.hexaware.jumbo.pojo.RunTimeTO;
import com.hexaware.jumbo.pojo.SchedulerTO;
import com.hexaware.jumbo.pojo.TestParamTO;
import com.hexaware.jumbo.security.User;
import com.hexaware.jumbo.utils.JumboUtils;
import com.hexaware.jumbo.utils.QTestClient;
import com.sybase.jdbc4.jdbc.SybDriver;

import static java.util.stream.Collectors.toList;

@CrossOrigin
@ComponentScan
@RestController
public class UserRestController {

	@Autowired
	JDBC_hive jdbcHive;

	@Autowired
	private UserRestManager manager;

	@Autowired
	private JdbcTemplateJobStatusDao jdbcTemplateJobStatusDao;

	@Autowired
	DataComparison csv2Object;

	@Autowired
	joinspark joinSpark;

	@Autowired
	clickToShowValues showValues;

	@Autowired
	excelWrite excelExport;

	@Autowired
	itextpdfpara pdfExport;
	
	@Autowired
	retrieveStats stats;

	static resource_path res_folder = new resource_path();
	static String res_path = res_folder.resource_folder();

	SparkContext sc = SparkInitiator.initializingSC();
	SQLContext spark = SparkInitiator.initializingSPARK(sc);
	encryptDecrypt eD = new encryptDecrypt();

	private static final Logger logger = LoggerFactory.getLogger(UserRestController.class);

	// Check the login credentials.
	@RequestMapping(value = "/checkLogin", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> checkLoginDetails(@RequestBody Map<String, String> requestBody)
			throws ClassNotFoundException, SQLException {

		logger.info("Entering into checkLogin");
		String userName = requestBody.get("uName");
		String userPass = requestBody.get("uPass");
		System.out.println("HAKUNAMATATA");
		System.out.println("userName  :   " + userName);
		System.out.println("userPass  :   " + userPass);

		String loginResult = jdbcHive.loginDetails(userName, userPass);
		System.out.println("Login Results  :   " + loginResult);
		return new ResponseEntity<String>("{\"message\":\"" + loginResult + "\"}", HttpStatus.OK);

	}

	// Create new project.
	@RequestMapping(value = "/createProject", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> createNewProject(@RequestBody Map<String, String> requestBody) {

		String prjName = requestBody.get("proName");
		String role = requestBody.get("role");
		String uName = requestBody.get("Uname");
		String projectResult = jdbcHive.createProject(prjName, role, uName);
		return new ResponseEntity<String>("{\"message\":\"" + projectResult + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getProjectList/{role}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getReleasesForProject(@PathVariable String role) throws SQLException {

		System.out.println("Fetching Project Names");
		List<String> dbName = jdbcHive.projectnames(role);
		return new ResponseEntity<List<String>>(dbName, HttpStatus.OK);

	}

	@RequestMapping(value = "/getFileList", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getFileList(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		System.out.println("Entered getFileList");
		String prjName = requestBody.get("projectName");
		List<String> fileName = jdbcHive.getFileList(prjName);
		return new ResponseEntity<List<String>>(fileName, HttpStatus.OK);

	}

	@RequestMapping(value = "/getColCountValue", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getColCountValue(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		System.out.println("Entered getColCountValue");
		String prjName = requestBody.get("proName");
		String testID = requestBody.get("testID");
		String colname = requestBody.get("colname");
		String fileType = requestBody.get("fileType");
		String isSrc = requestBody.get("isSrc");
		String page = requestBody.get("page").toString();
		Map<String, Object> colValue = showValues.getColValList(prjName, testID, spark, colname, fileType, isSrc, page);
		return new ResponseEntity<Map<String, Object>>(colValue, HttpStatus.OK);

	}

	@RequestMapping(value = "/getSrcTgtFile", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getSrcTgtFileList(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException, ClassNotFoundException {

		String prjName = requestBody.get("projectName");
		String fileName = requestBody.get("fileName");
		String keyName = requestBody.get("keyName");
		String testName = requestBody.get("testName");
		Map<String, Object> colList = null;
		try {
			colList = jdbcHive.getSrcTgtFileList(prjName, fileName, keyName, sc, spark, testName);
		} catch (SAXException sax) {
			Map<String, Object> errMsg = new HashMap<String, Object>();
			String aspx = sax.toString();
			aspx = aspx.replaceAll("\n", " ");
			errMsg.put("errormsg", aspx);
			logger.error("Error in getSrcTgtFile:" + sax.getMessage());
			return new ResponseEntity<Map<String, Object>>(errMsg, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			Map<String, Object> errMsg = new HashMap<String, Object>();
			String aspx = e.toString();
			aspx = aspx.replaceAll("\n", " ");
			errMsg.put("errormsg", aspx);
			logger.error("Error in getSrcTgtFile:" + e.getMessage());
			return new ResponseEntity<Map<String, Object>>(errMsg, HttpStatus.OK);

		}

		return new ResponseEntity<Map<String, Object>>(colList, HttpStatus.OK);

	}

	@RequestMapping(value = "/getAllFiles/{type}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getAllFiles(@PathVariable String type) throws IOException {

		List<String> fileList = jdbcHive.fileList(type);
		return new ResponseEntity<List<String>>(fileList, HttpStatus.OK);

	}

	@RequestMapping(value = "/addFile", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addFile(@RequestBody Map<String, String> requestBody) throws IOException {
		String type = "";
		String rowTag = "";
		String projName = requestBody.get("projName");
		String fileName = requestBody.get("fileName");
		String delimiter = requestBody.get("delimiter");
		if (delimiter.equalsIgnoreCase("Fixed Width")) {
			delimiter = "NA";
			type = "fixedwidth";
		} else if (fileName.contains(".xml") || fileName.contains(".XML")) {
			String[] splitTag = fileName.split(",");
			type = splitTag[0].substring(splitTag[0].lastIndexOf(".") + 1, splitTag[0].length());
			rowTag = splitTag[1];
			fileName = splitTag[0];
		} else if (delimiter.equalsIgnoreCase("Tag")) {
			type = "tag";
		} else
			type = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
		String isHeader = requestBody.get("isHeader");
		String headerFileName = requestBody.get("headerFileName");

		if (delimiter.equalsIgnoreCase("Tab"))
			delimiter = "	";

		String a = jdbcHive.addFile(projName, fileName, type, delimiter, isHeader, sc , spark, headerFileName, rowTag);

		return new ResponseEntity<String>("{\"message\":\"" + a + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/compareXML", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> compareXML(@RequestBody Map<String, String> requestBody) throws IOException {
		String sourceXML = requestBody.get("sourceXML");
		String targetXML = requestBody.get("targetXML");
		String PrjName = requestBody.get("PrjName");
		String tid = "";
		/*
		 * if(PrjName == null || PrjName =="") PrjName ="NEW_XML";
		 */
		/*
		 * if(resultPath.equalsIgnoreCase("NA")) resultPath = "E:\\pdfres"; File
		 * directory = new File(resultPath); if (! directory.exists()){
		 * directory.mkdir(); } PDFUtil pdfUtil = new PDFUtil();
		 * 
		 * int pgcnt = pdfUtil.getPageCount(path+sourcePDF);
		 * pdfUtil.setCompareMode(CompareMode.VISUAL_MODE);
		 * pdfUtil.highlightPdfDifference(true);
		 * pdfUtil.setImageDestinationPath(resultPath);
		 * System.out.println("resultPath >>"+resultPath);
		 * System.out.println("source >>"+path+sourcePDF);
		 * System.out.println("tgt  >>"+path+targetPDF); int ctr = 0; for(int
		 * i=1;i<=pgcnt;i++){ if(pdfUtil.compare(path+sourcePDF,
		 * path+targetPDF,i)) {} else ctr++; } String asd = ""; if(ctr == 0) asd
		 * = "true"; else asd = "false";
		 */
		String result = "";
		/*
		 * DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss"); Date
		 * date = new Date(); tid = PrjName+"_"+dateFormat.format(date);
		 * JobStatus js =
		 * JobStatus.create(tid,PrjName,tid,Calendar.getInstance().getTime().
		 * toString(),sourceXML,targetXML,userName,0,null,1,"New Test",null);
		 * jdbcTemplateJobStatusDao.save(js);
		 * jdbcTemplateJobStatusDao.updateProgress(tid, 5);
		 */
		result = manager.processXML(sourceXML, targetXML, PrjName, tid);
		/*
		 * jdbcTemplateJobStatusDao.updateProgress(tid, 100);
		 * jdbcTemplateJobStatusDao.updateTestStatus(tid, PrjName, "Executed");
		 */
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getTestNames", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getTestName(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("projectName");
		List<String> testNameList = jdbcHive.projectIDnames(prjName);
		return new ResponseEntity<List<String>>(testNameList, HttpStatus.OK);

	}

	@RequestMapping(value = "/getTestNamesWithDate", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getTestNamesWithDate(@RequestBody Map<String, String> requestBody)
			throws IOException {

		String prjName = requestBody.get("projectName");
		List<String> testNameList = jdbcHive.projectIDnamesWithDate(prjName);
		return new ResponseEntity<List<String>>(testNameList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getBatchNames/{projectName}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getBatchNames(@PathVariable String projectName) throws IOException {

		List<String> testNameList = jdbcHive.batchNames(projectName);
		return new ResponseEntity<List<String>>(testNameList, HttpStatus.OK);

	}

	@RequestMapping(value = "/createBatch", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> createBatch(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("projectName");
		String batname = requestBody.get("batname");
		String testnames = requestBody.get("testnames");
		String username = requestBody.get("userName");
		String result = "";
		int res = 0;
		try {
			String[] startTime = Calendar.getInstance().getTime().toString().split(" ");
			String doc = startTime[2] + "-" + startTime[1] + "-" + startTime[5];
			String toc = startTime[3].replace(':', '_');
			String[] abc = testnames.split(",");
			res = jdbcTemplateJobStatusDao.insertBatch(batname, abc.length, prjName, testnames.replaceAll(",", "##"),
					doc, toc, username);
			if (res > 0) {
				result = "Batch Created Successfully";
			} else
				result = "Failed to Create Batch";
		} catch (Exception ex) {
			logger.error("Exception in createBatch", ex.getMessage());
		}
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getBatchDetails/{projectName}/{batchName}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getBatchDetails(@PathVariable String projectName,
			@PathVariable String batchName) throws IOException {

		List<String> testNameList = jdbcHive.batchdetails(projectName, batchName);
		return new ResponseEntity<List<String>>(testNameList, HttpStatus.OK);

	}

	@RequestMapping(value = "/getTestDates", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getTestDate(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		List<String> testDateList = jdbcHive.fetch_dates(prjName, testName);
		return new ResponseEntity<List<String>>(testDateList, HttpStatus.OK);

	}

	@RequestMapping(value = "/getBatchDates", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getBatchDates(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("projectName");
		String batchName = requestBody.get("batchName");
		List<String> testDateList = jdbcHive.fetch_dates_batch(prjName, batchName);
		return new ResponseEntity<List<String>>(testDateList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getBatchTimes", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getBatchTimes(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("projectName");
		String batchName = requestBody.get("batchName");
		String batchDate = requestBody.get("batchDate");
		List<String> testDateList = jdbcHive.fetchtimestamp_batch(prjName, batchName, batchDate);
		return new ResponseEntity<List<String>>(testDateList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getTestTimes", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getTestTime(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		String testDate = requestBody.get("testDate");
		System.out.println(prjName);
		System.out.println(testName);
		System.out.println(testDate);
		List<String> testTimeList = jdbcHive.fetchtimestamp(prjName, testName, testDate);
		return new ResponseEntity<List<String>>(testTimeList, HttpStatus.OK);

	}

	@RequestMapping(value = "/deleteProject", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> deleteProjects(@RequestBody Map<String, String> requestBody) {

		String projectName = requestBody.get("projectName");
		System.out.println("project name  :   " + projectName);
		String prjPath = res_path + "jumbo_log/" + projectName;
		File path = new File(prjPath);
		boolean projectResult = jdbcHive.deleteDirectory(path, projectName);
		return new ResponseEntity<String>("{\"message\":\"" + projectResult + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/deleteFiles", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> deleteFiles(@RequestBody Map<String, String> requestBody) throws IOException {

		String projectName = requestBody.get("projectName");
		String fileName = requestBody.get("fileName");
		String projectResult = jdbcHive.removeLineFromFile(projectName, fileName);
		projectResult = projectResult.replaceAll("\n", " ");
		projectResult = projectResult.replaceAll("\"", "'");
		return new ResponseEntity<String>("{\"message\":\"" + projectResult + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/deleteTest", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> deleteTests(@RequestBody Map<String, String> requestBody) throws IOException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		String testCreationDateNTime = requestBody.get("testCreationDateNTime");
		String testExecutionDateNTime = requestBody.get("testExecutionDateNTime");
		String projectResult = jdbcHive.removeLineFromFile(projectName, testName, testCreationDateNTime,
				testExecutionDateNTime);
		return new ResponseEntity<String>("{\"message\":\"" + projectResult + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getDatabaseConnections", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getDatabaseConnections() throws SQLException, IOException {

		List<String> dbName = jdbcHive.getDatabaseConnections();
		if (dbName == null) {
			dbName = new ArrayList<String>();
			dbName.add("--NO DB CONNECTIONS AVAILABLE--");
		}
		return new ResponseEntity<List<String>>(dbName, HttpStatus.OK);
	}

	@RequestMapping(value = "/addDBConnection", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addDBConnection(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String connectionName = requestBody.get("connectionNames");
		String projName = requestBody.get("projName");

		String Result = "";
		try {
			/*
			 * String fileName = res_path + "configs/DBConfigs.txt";
			 * BufferedReader bbr = new BufferedReader(new
			 * FileReader(fileName)); String lineman = bbr.readLine(); String[]
			 * phalo = null; String[] phaloDB = null; String comp = ""; String
			 * database = ""; while (lineman != null) { phalo =
			 * lineman.split("##"); if
			 * (encryptDecrypt.decrypt(phalo[0]).equals(connectionNames)) {
			 * connStr = lineman; comp = encryptDecrypt.decrypt(phalo[1]);
			 * database = encryptDecrypt.decrypt(phalo[2]); } lineman =
			 * bbr.readLine(); } bbr.close();
			 * 
			 * String fileName1 = res_path + "jumbo_log/" + projName +
			 * "/Files.txt"; BufferedReader bbr11 = new BufferedReader(new
			 * FileReader(fileName1)); String lineman11 = bbr11.readLine(); int
			 * xyz = 0; while (lineman11 != null) { phalo =
			 * lineman11.split("##"); if (comp.equals(phalo[0]) &&
			 * database.equals(phalo[1])) { ++xyz; } lineman11 =
			 * bbr11.readLine(); } bbr11.close();
			 * 
			 * String logText = ""; String ipAddDetails = ""; if (xyz == 0) {
			 * BufferedWriter bufferedWriter = new BufferedWriter(new
			 * FileWriter(fileName1, true)); phalo = connStr.split("##"); for
			 * (int i = 1; i < phalo.length; i++){ if(i == 3){ phaloDB =
			 * phalo[3].split(","); if(phaloDB.length == 5) ipAddDetails =
			 * encryptDecrypt.decrypt(phaloDB[0])+","+encryptDecrypt.decrypt(
			 * phaloDB[1])+","+encryptDecrypt.decrypt(phaloDB[2])+","+
			 * encryptDecrypt.decrypt(phaloDB[3])+","+encryptDecrypt.decrypt(
			 * phaloDB[4]); logText = logText + ipAddDetails + "##"; }else
			 * logText = logText + encryptDecrypt.decrypt(phalo[i]) + "##"; }
			 * logText = logText.substring(0, logText.length() - 2);
			 * 
			 * bufferedWriter.write(logText); bufferedWriter.newLine();
			 * bufferedWriter.close(); Result = "Success"; }
			 * 
			 * else Result = "Already_Available";
			 */

			DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(connectionName);
			TestParamTO testParms = jdbcTemplateJobStatusDao.isParamsPresent(connectionName, projName);
			int valCount;
			if (dbDetails != null && testParms == null) {
				valCount = jdbcTemplateJobStatusDao.insertTestParam(projName, dbDetails.getType(),
						dbDetails.getDatabase(), "", "", "", dbDetails.getConnectionName());
				if (valCount == 1)
					Result = "Success";
				else
					Result = "Failure";
			} else {
				Result = "Already_Available";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in addDBConnection", e.getMessage());
			e.printStackTrace();
			String aps = e.toString().replaceAll("\n", " ");
			aps = e.toString().replaceAll("\"", "'");
			return new ResponseEntity<String>("{\"message\":\"" + aps + "\"}", HttpStatus.OK);
		}

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/addQTConnection", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addQTConnection(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String connectionName = requestBody.get("connectionNames");
		String projName = requestBody.get("projName");
		String PID = requestBody.get("PID");
		String modID = requestBody.get("modName");

		String Result = "";
		try {

			DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(connectionName);
			TestParamTO testParms = jdbcTemplateJobStatusDao.isParamsPresent(connectionName, projName);
			int valCount;
			if (dbDetails != null && testParms == null) {
				valCount = jdbcTemplateJobStatusDao.insertTestParam(projName, dbDetails.getType(), modID, "", "", PID,
						dbDetails.getConnectionName());
				if (valCount == 1)
					Result = "Success";
				else
					Result = "Failure";
			} else {
				Result = "Already_Available";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in addDBConnection", e.getMessage());
			e.printStackTrace();
			String aps = e.toString().replaceAll("\n", " ");
			aps = e.toString().replaceAll("\"", "'");
			return new ResponseEntity<String>("{\"message\":\"" + aps + "\"}", HttpStatus.OK);
		}

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/checkCreds", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> checkCreds(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String connectionName = requestBody.get("connectionNames");
		String username = requestBody.get("username");
		String password = requestBody.get("password");
		String Result = "**";

		DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(connectionName);
		try {
			if (dbDetails.getUser().equalsIgnoreCase(username)
					&& encryptDecrypt.decrypt(dbDetails.getPassword()).equalsIgnoreCase(password)) {
				Result = "Success";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Error while checking credentials in checkCreds:" + e.getMessage());
			String aps = e.toString().replaceAll("\n", " ");
			aps = e.toString().replaceAll("\"", "'");
			return new ResponseEntity<String>("{\"message\":\"" + aps + "\"}", HttpStatus.OK);
		}
		/*
		 * try { String fileName = res_path + "configs/DBConfigs.txt";
		 * BufferedReader bbr = new BufferedReader(new FileReader(fileName));
		 * String lineman = bbr.readLine(); String[] phalo = null; String[]
		 * phaloDB = null;
		 * 
		 * while (lineman != null) { phalo = lineman.split("##"); if
		 * (encryptDecrypt.decrypt(phalo[0]).equals(connectionNames)) { phaloDB
		 * = phalo[3].split(","); if
		 * (encryptDecrypt.decrypt(phaloDB[3]).equals(username) &&
		 * encryptDecrypt.decrypt(phaloDB[4]).equals(password)) { Result =
		 * "Success"; break; } } lineman = bbr.readLine(); } bbr.close(); }
		 * catch (Exception e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); String aps = e.toString().replaceAll("\n", " ");
		 * aps = e.toString().replaceAll("\"", "'"); return new
		 * ResponseEntity<String>("{\"message\":\"" + aps + "\"}",
		 * HttpStatus.OK); }
		 */

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/fetchConnectionDetails", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> fetchConnectionDetails(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String connectionName = requestBody.get("connectionNames");
		DBConfigTO dbConfigDetails = jdbcTemplateJobStatusDao.fetchSubConnection(connectionName);
		String Result = "**";

		if (dbConfigDetails != null) {
			if (dbConfigDetails.getDatabase() != null)
				Result = dbConfigDetails.getType() + " --> " + dbConfigDetails.getIp() + " -->"
						+ dbConfigDetails.getDatabase();
			else
				Result = dbConfigDetails.getType() + " --> " + dbConfigDetails.getIp();
		}
		/*
		 * try{ while (lineman != null) { phalo = lineman.split("##"); if
		 * (encryptDecrypt.decrypt(phalo[0]).equals(connectionNames)) {
		 * System.out.println(encryptDecrypt.decrypt(phalo[3])); phaloDB =
		 * phalo[3].split(","); Result = encryptDecrypt.decrypt(phalo[2]) +
		 * " --> " +
		 * encryptDecrypt.decrypt(phaloDB[0]).replaceAll(Pattern.quote("\\"), "
		 * -") + " --> " + encryptDecrypt.decrypt(phaloDB[2]); } lineman =
		 * bbr.readLine(); } bbr.close(); } catch (Exception e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); String aps =
		 * e.toString().replaceAll("\n", " "); aps =
		 * e.toString().replaceAll("\"", "'"); return new
		 * ResponseEntity<String>("{\"message\":\"" + aps + "\"}",
		 * HttpStatus.OK); }
		 */
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getStatus", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getStatistics(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("proName");
		String testID = requestBody.get("testID");
		Map<String, Object> response = manager.getStatistics(projectName, testID);
		return new ResponseEntity<Map<String, Object>>(response, HttpStatus.OK);

	}

	@RequestMapping(value = "/getresult1", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getResult1(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("prjName");
		String testID = requestBody.get("testID");
		String link = requestBody.get("link");
		Map<String, Object> tabledata = new HashMap<String, Object>();

		// System.out.println(projectName+"\n\n"+testID+"\n\n"+link);
		String resultpath = null;
		if (link.equals("val1"))
			resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/duplicate_keysrc1";
		else if (link.equals("val2"))
			resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/duplicate_keytgt1";
		else if (link.equals("val3"))
			resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/duplicate_entiresrc1";
		else if (link.equals("val4"))
			resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/duplicate_entiretgt1";
		else if (link.equals("val5"))
			resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/srckeyNotinTgt1";
		else if (link.equals("val6"))
			resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/tgtkeyNotinsrc1";

		String[] files = new File(resultpath).list();
		Arrays.sort(files);
		for (String asp : files) {
			if (asp.substring(asp.length() - 3, asp.length()).equals("csv"))
				resultpath = resultpath + "/" + asp;
		}
		System.out.println("resultpath: " + resultpath);

		String tabledata1 = "<tr>" + "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>"
				+ "<tr>" + "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>" + "<tr>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>" + "<tr>"
				+ "<td style=\"background-color: lightyellow;\"><font color=\"red\">1cd Total number of records in Source</font></td>"
				+ "<td>123456789</td>" + "</tr>";
		tabledata1 = jdbcHive.getValues(projectName, testID, resultpath);
		String xyz = "";
		if (link.equals("val1"))
			xyz = "Total number of duplicate key(s) in Source";
		else if (link.equals("val2"))
			xyz = "Total number of duplicate key(s) in Target";
		else if (link.equals("val3"))
			xyz = "Total number of duplicate records in Source";
		else if (link.equals("val4"))
			xyz = "Total number of duplicate records in Target";

		tabledata.put("tabledata1", tabledata1);
		tabledata.put("valLabel", xyz);
		return new ResponseEntity<Map<String, Object>>(tabledata, HttpStatus.OK);
	}

	@RequestMapping(value = "/getresult2", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getResult2(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("prjName");
		String testID = requestBody.get("testID");

		Map<String, Object> tabledata1 = new HashMap<String, Object>();
		Map<String, Object> tabledata2 = new HashMap<String, Object>();
		Map<String, Object> mismatchdata = new HashMap<String, Object>();

		String resultpath1 = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/mismatchsrc1";
		String resultpath2 = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/mismatchtgt1";
		String[] files1 = new File(resultpath1).list();
		String[] files2 = new File(resultpath2).list();
		Arrays.sort(files1);
		Arrays.sort(files2);
		for (String asp : files1) {
			if (asp.substring(asp.length() - 3, asp.length()).equals("csv"))
				resultpath1 = resultpath1 + "/" + asp;
		}
		for (String asp : files2) {
			if (asp.substring(asp.length() - 3, asp.length()).equals("csv"))
				resultpath2 = resultpath2 + "/" + asp;
		}

		String data1 = "<tr>" + "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>"
				+ "<tr>" + "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>" + "<tr>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>" + "<tr>"
				+ "<td style=\"background-color: lightyellow;\"><font color=\"red\">1cd Total number of records in Source</font></td>"
				+ "<td>123456789</td>" + "</tr>";
		String data2 = "<tr>" + "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>"
				+ "<tr>" + "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>" + "<tr>"
				+ "<td>1. Total number of records in Source</td>" + "<td>123456789</td>" + "</tr>" + "<tr>"
				+ "<td style=\"background-color: lightyellow;\"><font color=\"red\">1cd Total number of records in Source</font></td>"
				+ "<td>123456789</td>" + "</tr>";

		data1 = jdbcHive.getValuesXYX(projectName, testID, resultpath1);
		data2 = jdbcHive.getValuesXYX(projectName, testID, resultpath2);

		tabledata1.put("tabledata1", data1);
		tabledata2.put("tabledata2", data2);

		mismatchdata.put("mismatchdatasrc", tabledata1);
		mismatchdata.put("mismatchdatatgt", tabledata2);
		return new ResponseEntity<Map<String, Object>>(mismatchdata, HttpStatus.OK);
	}

	@RequestMapping(value = "/scrvariable", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getscrvariable(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException, InterruptedException, ClassNotFoundException {

		logger.info("Entering into scrvariable");
		String message = "";
		String srcCols_dt = "";
		String tgtCols_dt = "";
		String tid = "";
		String[] filteredParams = null;
		TestParamTO tpdata =null;
		String trID = null;
		long PID = -1,testid=-1,testvid=-1, MID=-1;
		try {
			String srcKeys = requestBody.get("srcKeys");
			String File1 = requestBody.get("file1");
			String prjName = requestBody.get("prjName");
			String tstName = requestBody.get("tstName");
			String srcrules = requestBody.get("srcRules");
			String srcfilters = requestBody.get("srcFilters");
			String srcClause = requestBody.get("srcClause");
			String srcMapcols = requestBody.get("srcMapCols");
			String srcRulesApplied = requestBody.get("srcRuleApp");
			String srcFiltersApplied = requestBody.get("srcFilterApp");
			String filterParam = requestBody.get("filterExecute");
			String ChgDt = requestBody.get("ChgDt");
			String username = requestBody.get("userName");
			String ssotoken = requestBody.get("ssotoken");
			// tid = jdbcHive.testIDgenerator(prjName);
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			Date date = new Date();
			tid = prjName + "_" + dateFormat.format(date);

			String[] srcTgtKeys = srcKeys.split("##");
			String srckeys = srcTgtKeys[0];
			String tgtkeys = srcTgtKeys[1];

			String[] srcTgtFile = File1.split("##");
			String file1 = srcTgtFile[0];
			String file2 = srcTgtFile[1];
			logger.info("SourceFile :" + file1);
			logger.info("TargetFile :" + file2);

			String[] srcTgtRules = srcrules.split("##");
			String srcRules = srcTgtRules[0];
			if (srcRules == " ")
				srcRules = "NA";
			String tgtRules = srcTgtRules[1];
			if (tgtRules == " ")
				tgtRules = "NA";

			logger.info("SourceRules :" + srcRules);
			logger.info("TargetRules :" + tgtRules);

			String[] srcTgtFilters = srcfilters.split("##");
			String srcFilters = srcTgtFilters[0];
			System.out.println(srcFilters.length());
			if (srcFilters.length() <= 1)
				srcFilters = "NA";
			String tgtFilters = srcTgtFilters[1];
			System.out.println(tgtFilters.length());
			if (tgtFilters.length() <= 1)
				tgtFilters = "NA";

			logger.info("SourceFilters :" + srcFilters);
			logger.info("TargetFilters :" + tgtFilters);

			String[] srcTgtClauses = srcClause.split("##");
			String srcClauses = srcTgtClauses[0];
			String tgtClauses = srcTgtClauses[1];
			if (srcClauses == " ")
				srcClauses = "NA";
			if (tgtClauses == " ")
				tgtClauses = "NA";

			logger.info("srcClauses :" + srcClauses);
			logger.info("tgtClauses :" + tgtClauses);

			String[] srcTgtCols = srcMapcols.split("##");
			String srcColsList = srcTgtCols[0];
			String tgtColsList = srcTgtCols[1];
			String srcCols = srcTgtCols[0];
			String tgtCols = srcTgtCols[1];

			logger.info("SourceCols :" + srcCols);
			logger.info("TargetCols :" + tgtCols);

			String[] srcTgtRulesApp = srcRulesApplied.split("##");
			String srcRuleApp = srcTgtRulesApp[0];
			String tgtRuleApp = srcTgtRulesApp[1];

			String[] ChDt = ChgDt.split("##");
			String srChgDt = ChDt[0];
			String tgtDt = ChDt[1];

			if (filterParam != "" && filterParam != null) {
				filteredParams = filterParam.split(",");
			}
			JobStatus js = JobStatus.create(tid, prjName, tstName, Calendar.getInstance().getTime().toString(), file1,
					file2, username, 0, null, 1, "New Test", null);
			jdbcTemplateJobStatusDao.save(js);
			jdbcTemplateJobStatusDao.updateProgress(tid, 5);

			String path1 = "";
			String path2 = "", delimiter1 = "", delimiter2 = "";
			String fileType1 = "";
			String fileType2 = "";
			/*
			 * String accessKeyId = ""; String accessSecretKey = ""; String
			 * awsSourceFileType=""; String awsTargetFileType="";
			 */
			srckeys = srckeys.substring(0, srckeys.length() - 1);
			tgtkeys = tgtkeys.substring(0, tgtkeys.length() - 1);

			String[] ak1 = srckeys.split(",");
			srckeys = "";
			for (String as : ak1) {
				String[] b = as.split("-->");
				srckeys = srckeys + b[0] + ",";
			}
			srckeys = srckeys.substring(0, srckeys.length() - 1);

			String[] ak2 = tgtkeys.split(",");
			tgtkeys = "";
			for (String as : ak2) {
				String[] b = as.split("-->");
				tgtkeys = tgtkeys + b[0] + ",";
			}
			tgtkeys = tgtkeys.substring(0, tgtkeys.length() - 1);

			//// To get the Source File parameters
			//// ************************************************************************
			TestParamTO testParms = null;
			String[] axe = file1.split("\\.");
			if(axe[0].equalsIgnoreCase("AWS-S3")) {
				String [] splitFile = axe[2].split("/");
				testParms = jdbcTemplateJobStatusDao.isParamsPresent(splitFile[1]+"-"+axe[1],prjName);
			}else if(!axe[0].equalsIgnoreCase("Join") && !axe[0].equalsIgnoreCase("CustomQuery") && !axe[0].equalsIgnoreCase("OwnQuery")) {
				testParms = jdbcTemplateJobStatusDao.isParamsPresent(axe[2],prjName);
			} else {
				testParms = jdbcTemplateJobStatusDao.isParamsPresent(axe[1], prjName);
			}
			
			if (!axe[0].equalsIgnoreCase("Files") && !file1.contains("AWS-S3") && !axe[0].equalsIgnoreCase("Join") && !axe[0].equalsIgnoreCase("CustomQuery") && !axe[0].equalsIgnoreCase("OwnQuery")) {	
				String[] splitType = axe[0].split("-");
				DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(splitType[1]);
				String getDBdetailStich = dbDetails.getIp() + "," + dbDetails.getPort() + "," + dbDetails.getDatabase()
						+ "," + dbDetails.getUser() + "," + eD.decrypt(dbDetails.getPassword());
				if (file1.contains("Oracle")) {
					if (splitType[1].equalsIgnoreCase(dbDetails.getConnectionName())) {
						path1 = getDBdetailStich + "," + axe[1] + "." + axe[2];
						fileType1 = dbDetails.getType();
					}
				} else if (file1.contains("PostgreSQL")) {
					if (splitType[1].equalsIgnoreCase(dbDetails.getConnectionName())) {
						path1 = getDBdetailStich + "," + axe[1] + "." + axe[2];
						fileType1 = dbDetails.getType();
					}
				} else if (dbDetails.getDatabase().equalsIgnoreCase(axe[1]) && axe[0].contains(dbDetails.getType())) {
					path1 = getDBdetailStich + "," + axe[2];
					fileType1 = dbDetails.getType();
				} else if (file1.contains("AWS Redshift") && axe[0].equalsIgnoreCase(dbDetails.getType())) {
					path1 = getDBdetailStich + "," + axe[2] + "," + axe[1];
					fileType1 = dbDetails.getType();
				}
			} else if (axe[0].equalsIgnoreCase("Files") && file1.contains(testParms.getConnectionName())) {				
				if(testParms.getType().equalsIgnoreCase("edi")) 
					path1 = testParms.getHeaderFileName();
				else 
					path1 = testParms.getPath();
				
				fileType1 = testParms.getType();
				
				if (fileType1.equals("csv") || fileType1.equals("dsv") || fileType1.equals("txt")
						|| fileType1.equals("excel"))
					delimiter1 = testParms.getDelimitter();					
			} else if((file1.contains("Join") && file1.contains(testParms.getConnectionName())) || (file1.contains("CustomQuery") && file1.contains(testParms.getConnectionName())) || (file1.contains("OwnQuery") && file1.contains(testParms.getConnectionName()))){
				JoinCustomTO paramDetails= jdbcTemplateJobStatusDao.fetchJoinCustomDetails(testParms.getConnectionName());
					path1 = paramDetails.getFileName();
				fileType1 = paramDetails.getType();
				if (fileType1.equalsIgnoreCase("Join") || fileType1.equalsIgnoreCase("CustomQuery") || fileType1.equalsIgnoreCase("OwnQuery")) {
					srcCols_dt = paramDetails.getColumns();
				}
			} else if (axe[0].equalsIgnoreCase("AWS-S3")) {
				String[] awsSplit = file1.split("\\.");
				if (testParms.getType().contains("AWSParquet") && awsSplit[0].contains(testParms.getConnectionName())) {
					if (awsSplit.length > 3) {
						path1 = "s3n://" + testParms.getPath() + "/" + awsSplit[2] + "." + awsSplit[3];
					} else {
						path1 = "s3n://" + testParms.getPath() + "/" + awsSplit[2];
					}
					fileType1 = testParms.getType();
				} else if (awsSplit[0].equalsIgnoreCase(testParms.getType())
						&& testParms.getPath().contains(awsSplit[2])) {
					delimiter1 = testParms.getDelimitter();
					fileType1 = testParms.getType();
					path1 = "s3n://" + testParms.getPath();

				}
			}

			logger.info("Source File Type : " + fileType1);
			logger.info("Source Path :" + path1);

			//// To get the Target File parameters
			//// ************************************************************************

			String[] axeTarget = file2.split("\\.");
			TestParamTO testParmsTarget = null;
			
			if(axeTarget[0].equalsIgnoreCase("AWS-S3")) {
				String [] splitFile = axeTarget[2].split("/");
				testParmsTarget = jdbcTemplateJobStatusDao.isParamsPresent(splitFile[1]+"-"+axeTarget[1],prjName);
			}else if(!axeTarget[0].equalsIgnoreCase("Join") && !axeTarget[0].equalsIgnoreCase("CustomQuery") && !axeTarget[0].equalsIgnoreCase("OwnQuery")) {
				testParmsTarget = jdbcTemplateJobStatusDao.isParamsPresent(axeTarget[2],prjName);
			} else {
				testParmsTarget = jdbcTemplateJobStatusDao.isParamsPresent(axeTarget[1], prjName);
			}
			if (!axeTarget[0].equalsIgnoreCase("Files") && !file2.contains("AWS-S3") && !axeTarget[0].equalsIgnoreCase("Join") && !axeTarget[0].equalsIgnoreCase("CustomQuery") && !axeTarget[0].equalsIgnoreCase("OwnQuery")) {
				String[] splitTypeTarget = axeTarget[0].split("-");
				DBConfigTO dbDetailsTarget = jdbcTemplateJobStatusDao.fetchSubConnection(splitTypeTarget[1]);
				String getDBdetailStichTarget = dbDetailsTarget.getIp() + "," + dbDetailsTarget.getPort() + ","
						+ dbDetailsTarget.getDatabase() + "," + dbDetailsTarget.getUser() + ","
						+ eD.decrypt(dbDetailsTarget.getPassword());
				if (file2.contains("Oracle")) {
					if (splitTypeTarget[1].equalsIgnoreCase(dbDetailsTarget.getConnectionName())) {
						path2 = getDBdetailStichTarget + "," + axeTarget[1] + "." + axeTarget[2];
						fileType2 = dbDetailsTarget.getType();
					}
				} else if (file2.contains("PostgreSQL")) {
					if (splitTypeTarget[1].equalsIgnoreCase(dbDetailsTarget.getConnectionName())) {
						path2 = getDBdetailStichTarget + "," + axeTarget[1] + "." + axeTarget[2];
						fileType2 = dbDetailsTarget.getType();
					}
				}  else if (dbDetailsTarget.getDatabase().equalsIgnoreCase(axeTarget[1])
						&& axeTarget[0].contains(dbDetailsTarget.getType())) {
					path2 = getDBdetailStichTarget + "," + axeTarget[2];
					fileType2 = dbDetailsTarget.getType();
				} else if (file2.contains("AWS Redshift") && axeTarget[0].equalsIgnoreCase(dbDetailsTarget.getType())) {
					path2 = getDBdetailStichTarget + "," + axeTarget[2] + "," + axeTarget[1];
					fileType2 = dbDetailsTarget.getType();
				}

			} else if (axeTarget[0].equalsIgnoreCase("Files") && file2.contains(testParmsTarget.getConnectionName())) {
				if(testParmsTarget.getType().equalsIgnoreCase("edi"))
					path2 = testParmsTarget.getHeaderFileName();
				else
					path2 = testParmsTarget.getPath();
				
				fileType2 = testParmsTarget.getType();
				if (fileType2.equals("csv") || fileType2.equals("dsv") || fileType2.equals("txt")
						|| fileType2.equals("excel"))
					delimiter2 = testParmsTarget.getDelimitter();					
			} else if((file2.contains("Join") && file2.contains(testParmsTarget.getConnectionName()))  || (file2.contains("CustomQuery") && file2.contains(testParmsTarget.getConnectionName())) || (file2.contains("OwnQuery") && file2.contains(testParmsTarget.getConnectionName()))){
				JoinCustomTO paramDetailsTarget = jdbcTemplateJobStatusDao.fetchJoinCustomDetails(testParmsTarget.getConnectionName());
				path2 = paramDetailsTarget.getFileName();
				fileType2 = paramDetailsTarget.getType();
				if (fileType2.equals("Join") || fileType2.equals("CustomQuery") || fileType2.equals("OwnQuery")) {
					srcCols_dt = paramDetailsTarget.getColumns();
				}
			} else if (axeTarget[0].equalsIgnoreCase("AWS-S3")) {
				String[] awsSplit = file2.split("\\.");
				if (testParmsTarget.getType().contains("AWSParquet")
						&& awsSplit[1].equalsIgnoreCase(testParmsTarget.getConnectionName())) {
					if (awsSplit.length > 3) {
						path2 = "s3n://" + testParmsTarget.getPath() + "/" + awsSplit[2] + "." + awsSplit[3];
					} else {
						path2 = "s3n://" + testParmsTarget.getPath() + "/" + awsSplit[2];
					}
					fileType2 = testParmsTarget.getType();
				} else if (awsSplit[0].equalsIgnoreCase(testParmsTarget.getType())
						&& testParmsTarget.getPath().contains(awsSplit[2])) {
					delimiter2 = testParmsTarget.getDelimitter();
					fileType2 = testParmsTarget.getType();
					path2 = "s3n://" + testParmsTarget.getPath();

				}

			}
			logger.info("Target File Type : " + fileType2);
			logger.info("Target Path :" + path2);

			String[] abc1 = srcCols.split(",");
			String[] abc2 = tgtCols.split(",");

			srcCols = "";
			tgtCols = "";
			String[] sp = null;
			for (String a : abc1) {
				sp = a.split(" --> ");
				srcCols = srcCols + sp[0] + ",";
			}
			for (String a : abc2) {
				sp = a.split(" --> ");
				tgtCols = tgtCols + sp[0] + ",";
			}
			timedifference a = new timedifference();

			if (srcCols.endsWith(","))
				srcCols = srcCols.substring(0, srcCols.length() - 1);

			if (tgtCols.endsWith(","))
				tgtCols = tgtCols.substring(0, tgtCols.length() - 1);

			if (srChgDt.endsWith(","))
				srChgDt = srChgDt.substring(0, srChgDt.length() - 1);

			if (tgtDt.endsWith(","))
				tgtDt = tgtDt.substring(0, tgtDt.length() - 1);

			String srcCols1 = srcCols, tgtCols1 = tgtCols;
			if (!srChgDt.equals("NA")) {
				srcCols1 = srcCols;
				String[] dtp = srChgDt.split(",");
				String[] cdp = null;
				for (String dt : dtp) {
					cdp = dt.split(" --> ");
					if (srcCols.contains(cdp[0])) {
						srcCols1 = srcCols1.replace(cdp[0], " cast(" + cdp[0] + " as " + cdp[1] + ")");
					}
				}
			}
			if (!tgtDt.equals("NA")) {
				tgtCols1 = tgtCols;
				String[] tgt = tgtDt.split(",");
				String[] tdp = null;
				for (String td : tgt) {
					tdp = td.split(" --> ");
					if (tgtCols.contains(tdp[0])) {
						tgtCols1 = tgtCols1.replace(tdp[0], " cast(" + tdp[0] + " as " + tdp[1] + ")");
					}
				}
			}

			if (fileType1.equalsIgnoreCase("xml") || fileType1.equalsIgnoreCase("tag")) {
				//srckeys = "RowID('0')";
				srcCols1 = srcCols ;
				srcClauses = "NA";
				//srcRules = srcRules + "," + srckeys;
				//srcCols = srcCols + "," + srckeys;
			}

			if (fileType2.equalsIgnoreCase("xml") || fileType2.equalsIgnoreCase("tag")) {
				//tgtkeys = "RowID('0')";
				tgtCols1 = tgtCols ;
				tgtClauses = "NA";
				//tgtRules = tgtRules + "," + srckeys;
				//tgtCols = tgtCols + "," + tgtkeys;
			}
			
			String[] startTime = Calendar.getInstance().getTime().toString().split(" ");
			long startTime1 = System.currentTimeMillis();
			jdbcTemplateJobStatusDao.updateProgress(tid, 10);
			System.out.println("srcCols:"+srcCols);
			System.out.println("tgtCols:"+tgtCols);
			boolean q =false;
			String envQ = null,tierQ=null,Precodn=null;
			for(String asdf : filteredParams)
			{
				if(asdf.equals("qtestchk"))
				{
				q=true;
				}
				if(asdf.contains("Environment_")){
					envQ =asdf;
				}
				if(asdf.contains("Priority_")){
					tierQ =asdf;
				}
				if(asdf.contains("QTestPrec")){
					Precodn =asdf.split("@@")[1];
				}
			}
			tpdata = jdbcTemplateJobStatusDao.fetchQTestConn(prjName);
			if(q && tpdata != null){
				DBConfigTO dbData = jdbcTemplateJobStatusDao.fetchSubConnection(tpdata.getConnectionName());				
				String ip_address = dbData.getIp();
				String uuid = dbData.getDatabase();
				String user_name = dbData.getUser();
				String pass = eD.decrypt(dbData.getPassword());
		   	 	PID = Long.parseLong(tpdata.getHeaderFileName());
		   	 	MID = Long.parseLong(tpdata.getPath());
		   	 	QTestClient qt = new QTestClient();
		   	 if(ssotoken != null && ssotoken.trim().length()>0 && !ssotoken.equals("NA")){
		   		if(ip_address.contains("http"))
			   	 	qt.serverUrl = ip_address + "/";
					else
					qt.serverUrl = "http://" + ip_address + "/";
			   	 	qt.accessToken = ssotoken;
		   	 }else{
		   	 	qt.LoginQTest(ip_address,user_name,pass,"NA");
		   	 }
		   	 	String testdid = QTestClient.createTestCase(PID,MID,tstName,File1,prjName,srcKeys,srcRulesApplied,srcFiltersApplied,srcClause,Precodn);
		   	 	testid = Long.parseLong(testdid.split("@@")[0]);
		   	    testvid = Long.parseLong(testdid.split("@@")[1]);
		   	    long parentid = Long.parseLong(testdid.split("@@")[2]);
		   	    String vers = testdid.split("@@")[3];
		   	 	System.out.println(" testid ???? "+testid);
		   	 	System.out.println("prjName ??? "+prjName);
		   	 	trID = QTestClient.createTestRun(PID, prjName, testid,tstName,MID);
		   	 	System.out.println(" test run  <><><> "+trID);
		   	 	System.out.println("parentid ??????? "+parentid);
		   	  String app = QTestClient.approveTestCase(PID,testid,parentid,testvid,vers);
		   	  System.out.println("app  >> "+app);
			}
			
			
			String res = csv2Object.execute(srcCols,tgtCols,srckeys,tgtkeys,path1,path2,prjName,tid,fileType1,fileType2,srcColsList,tgtColsList,file1,file2,srcRules,tgtRules,srcFilters,tgtFilters,srcClauses,tgtClauses,filteredParams,srcCols1,tgtCols1,sc,spark,jdbcTemplateJobStatusDao,"Execute");			
			
			if(res.substring(0, 1).equalsIgnoreCase("1")){
				String dashPath=res_path+"jumbo_log/"+prjName+"/DET.txt";
				BufferedReader dashReader=new BufferedReader(new FileReader(dashPath));
				List<String> dashDetails=new ArrayList<String>();
				dashDetails.add(dashReader.readLine());
				dashDetails.add(dashReader.readLine());
				int totalTestRuns = Integer.parseInt(dashReader.readLine()) + 1;
				dashDetails.add(Integer.toString(totalTestRuns));
				dashDetails.add(dashReader.readLine());
				if (res.substring(2).equalsIgnoreCase("1")) {
					int totalTestPassed = Integer.parseInt(dashReader.readLine()) + 1;
					dashDetails.add(Integer.toString(totalTestPassed));
				} else
					dashDetails.add(dashReader.readLine());
				if (res.substring(2).equalsIgnoreCase("0")) {
					int totalTestFailed = Integer.parseInt(dashReader.readLine()) + 1;
					dashDetails.add(Integer.toString(totalTestFailed));
				} else
					dashDetails.add(dashReader.readLine());
				dashReader.close();
				FileWriter fileWrite = new FileWriter(dashPath);
				fileWrite.write("");
				BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(dashPath, true));
				for (String ss : dashDetails) {
					bufferWriter.write(ss);
					bufferWriter.newLine();
				}
				bufferWriter.close();
				fileWrite.close();
			}

			message = "";
			String toc = null,toe=null,doc=null,doe=null,st=null;
			if (res.substring(0, 1).equalsIgnoreCase("1")) {
				String[] endTime = Calendar.getInstance().getTime().toString().split(" ");
				long endTime1 = System.currentTimeMillis();
				doc = startTime[2] + "-" + startTime[1] + "-" + startTime[5];
				toc = startTime[3].replace(':', '_');
				doe = endTime[2] + "-" + endTime[1] + "-" + endTime[5];
				toe = endTime[3].replace(':', '_');
				//String user = "administrator";
				String[] srcTgtClaSplit = srcClause.split("##");
				if (fileType1.equalsIgnoreCase("xml")) {
					srcClauses = srcTgtClaSplit[0];
				}
				if (fileType2.equalsIgnoreCase("xml")) {
					tgtClauses = srcTgtClaSplit[1];
				}

				String execTime = a.time(startTime1, endTime1);
				jdbcTemplateJobStatusDao.updateProgress(tid, 100);
				String logText = tid + "##" + tstName + "##" + doc + "##" + toc + "##" + doe + "##" + toe + "##"
						+ username + "##" + execTime + "##" + file1 + "##" + srckeys + "##" + srcRuleApp + "##"
						+ srcFilters + "##" + srcCols + "##" + file2 + "##" + tgtkeys + "##" + tgtRuleApp + "##"
						+ tgtFilters + "##" + tgtCols + "##" + fileType1 + "##" + fileType2 + "##" + srcColsList + "##"
						+ tgtColsList + "##" + srcRules + "##" + tgtRules + "##" + filterParam + "##" + srcCols1 + "##"
						+ tgtCols1 + "##" + srcClauses + "##" + tgtClauses;

				String fileName = res_path + "jumbo_log/" + prjName + "/CD00123.txt";
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName, true));
				bufferedWriter.write(logText);
				bufferedWriter.newLine();
				bufferedWriter.close();
				jdbcTemplateJobStatusDao.updateTestStatus(tstName, prjName, "Executed");
				message = tid;
			} else if (res.substring(0, 1).equalsIgnoreCase("3")) {
				message = "Maximum Count Reached";
			}

			else
				message = res;

			if (message.contains("Error")) {
				jdbcTemplateJobStatusDao.updateStatus(tid, 4);
				st="SKIP";
			} else if (message.contains("Maximum Count Reached")) {
				jdbcTemplateJobStatusDao.delete(tid);
				st="SKIP";
			} else if (jdbcTemplateJobStatusDao.load(tid) != null && res.substring(2, 3).equalsIgnoreCase("1")) {
				jdbcTemplateJobStatusDao.updateStatus(tid, 2);
				st = "PASS";
				System.out.println("*-*-*-*-*-*-*-*-*-*-*-* Test Passed");
			} else if (jdbcTemplateJobStatusDao.load(tid) != null && res.substring(2, 3).equalsIgnoreCase("0")) {
				jdbcTemplateJobStatusDao.updateStatus(tid, 3);
				st = "FAIL";
				System.out.println("*-*-*-*-*-*-*-*-*-*-*-* Test Failed");
			}
			if(trID != null){
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
				formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
				long t1 = Long.parseLong(trID.split("@@")[0]);
				//long t2 = Long.parseLong(trID.split("@@")[1]);
				java.util.Date date1 = new Date(doc+" "+toc.replaceAll("_", ":"));
				java.util.Date date2 = new Date(doe+" "+toe.replaceAll("_", ":"));
				pdfExport.pdfgenerate1(prjName, tid,sc,spark);
				String tss = QTestClient.createTestResult(PID,t1, testvid,tstName,formatter.format(date1),formatter.format(date2),st,tid,prjName,envQ,tierQ);
				System.out.println("tss >>>> "+tss);
				//QTestClient.updateStatus(PID, tstName,MID);

			}
		} catch (Exception e) {
			jdbcTemplateJobStatusDao.updateStatus(tid, 4);
			logger.info("Error in scrvariable: " + e.getMessage());
			String aspx = e.toString();
			aspx = aspx.replaceAll("\"", "'");
			aspx = aspx.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + aspx + "\"}", HttpStatus.OK);
		}
		message = message.replaceAll("\"", "'");
		message = message.replaceAll("\n", " ");
		logger.info("Leaving scrvariable");
		// FileUtils.deleteDirectory(new
		// File("C:/JumboFiles/all_files/temp/Xml2Csv"));
		// FileUtils.deleteDirectory(new
		// File("C:/JumboFiles/all_files/temp/Excel2Csv"));
		return new ResponseEntity<String>("{\"message\":\"" + message + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/scrvariablesave", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getscrvariablesave(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException, InterruptedException, ClassNotFoundException {

		logger.info("Entering into scrvariablesave");
		String message = "";
		String srcCols_dt = "";
		String tgtCols_dt = "";
		String[] filteredParams = null;
		try {
			String srcKeys = requestBody.get("srcKeys");
			String File1 = requestBody.get("file1");
			String prjName = requestBody.get("prjName");
			String tstName = requestBody.get("tstName");
			String srcrules = requestBody.get("srcRules");
			String srcfilters = requestBody.get("srcFilters");
			String srcClause = requestBody.get("srcClause");
			String srccols = requestBody.get("srcMapCols");
			String srcRulesApplied = requestBody.get("srcRuleApp");
			String srcFiltersApplied = requestBody.get("srcFilterApp");
			String filterParam = requestBody.get("filterExecute");
			String ChgDt = requestBody.get("ChgDt");
			String username = requestBody.get("userName");

			String[] srcTgtKeys = srcKeys.split("##");
			String srckeys = srcTgtKeys[0];
			String tgtkeys = srcTgtKeys[1];

			String[] srcTgtFile = File1.split("##");
			String file1 = srcTgtFile[0];
			String file2 = srcTgtFile[1];

			logger.info("SourceFile :" + file1);
			logger.info("TargetFile :" + file2);

			String[] srcTgtRules = srcrules.split("##");
			String srcRules = srcTgtRules[0];
			String tgtRules = srcTgtRules[1];

			logger.info("SourceRules :" + srcRules);
			logger.info("TargetRules :" + tgtRules);

			String[] srcTgtFilters = srcfilters.split("##");
			String srcFilters = srcTgtFilters[0];
			String tgtFilters = srcTgtFilters[1];

			logger.info("SourceFilter :" + srcFilters);
			logger.info("TargetFilter :" + tgtFilters);

			String[] srcTgtClauses = srcClause.split("##");
			String srcClauses = srcTgtClauses[0];
			String tgtClauses = srcTgtClauses[1];

			String[] srcTgtCols = srccols.split("##");
			String srcColsList = srcTgtCols[0];
			String tgtColsList = srcTgtCols[1];
			String srcCols = srcTgtCols[0];
			String tgtCols = srcTgtCols[1];

			String[] srcTgtRulesApp = srcRulesApplied.split("##");
			String srcRuleApp = srcTgtRulesApp[0];
			String tgtRuleApp = srcTgtRulesApp[1];

			String[] srcTgtFilterApp = srcFiltersApplied.split("##");
			String srcFilterApp = srcTgtFilterApp[0];
			String tgtFilterApp = srcTgtFilterApp[1];

			String[] ChDt = ChgDt.split("##");
			String srChgDt = ChDt[0];
			String tgtDt = ChDt[1];

			if (filterParam != "" && filterParam != null) {
				filteredParams = filterParam.split(",");
			}

			String path1 = "";
			String path2 = "";
			String fileType1 = "";
			String fileType2 = "";

			srckeys = srckeys.substring(0, srckeys.length() - 1);
			tgtkeys = tgtkeys.substring(0, tgtkeys.length() - 1);

			String[] ak1 = srckeys.split(",");
			srckeys = "";
			for (String as : ak1) {
				String[] b = as.split("-->");
				srckeys = srckeys + b[0] + ",";
			}
			srckeys = srckeys.substring(0, srckeys.length() - 1);

			String[] ak2 = tgtkeys.split(",");
			tgtkeys = "";
			for (String as : ak2) {
				String[] b = as.split("-->");
				tgtkeys = tgtkeys + b[0] + ",";
			}
			tgtkeys = tgtkeys.substring(0, tgtkeys.length() - 1);
			
		//// To get the Source File parameters ************************************************************************
				TestParamTO testParms = null;
				String[] axe = file1.split("\\.");
				if(axe.length >2)
					testParms = jdbcTemplateJobStatusDao.isParamsPresent(axe[2],prjName);
				else
					testParms = jdbcTemplateJobStatusDao.isParamsPresent(axe[1],prjName);
				if (axe.length >2 && !axe[0].equalsIgnoreCase("Files") && !axe[0].equalsIgnoreCase("AWS-S3") && !axe[0].equalsIgnoreCase("Join") && !axe[0].equalsIgnoreCase("CustomQuery") && !axe[0].equalsIgnoreCase("OwnQuery")) {	
					String[] splitType = axe[0].split("-");
					DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(splitType[1]);
					String getDBdetailStich = dbDetails.getIp()+","+dbDetails.getPort()+","+dbDetails.getDatabase()+","+dbDetails.getUser()+","+eD.decrypt(dbDetails.getPassword());
					if(file1.contains("Oracle")){											
						if(splitType[1].equalsIgnoreCase(dbDetails.getConnectionName())){
							path1 = getDBdetailStich + "," + axe[1]+"."+axe[2];
							fileType1 = dbDetails.getType();
						}						
					} else if(file1.contains("PostgreSQL")){											
						if(splitType[1].equalsIgnoreCase(dbDetails.getConnectionName())){
							path1 = getDBdetailStich + "," + axe[1]+"."+axe[2];
							fileType1 = dbDetails.getType();
						}						
					} else if(dbDetails.getDatabase().equalsIgnoreCase(axe[1]) && axe[0].contains(dbDetails.getType())){
						path1 = getDBdetailStich + "," +axe[2];
						fileType1 = dbDetails.getType();
					} else if(file1.contains("AWS Redshift") && axe[0].equalsIgnoreCase(dbDetails.getType())){
						path1 = getDBdetailStich + "," +axe[2]+","+axe[1];
						fileType1 = dbDetails.getType();
					}
				} else if (axe[0].equalsIgnoreCase("Files") && file1.contains(testParms.getConnectionName())) {
					path1 = testParms.getPath();
					fileType1 = testParms.getType();				
				} else if((file1.contains("Join") && file1.contains(testParms.getConnectionName())) || (file1.contains("CustomQuery") && file1.contains(testParms.getConnectionName())) || (file1.contains("OwnQuery") && file1.contains(testParms.getConnectionName()))){
					JoinCustomTO paramDetails= jdbcTemplateJobStatusDao.fetchJoinCustomDetails(testParms.getConnectionName());
					path1 = paramDetails.getFileName();
					fileType1 = paramDetails.getType();
					if (fileType1.equals("Join") || fileType1.equals("CustomQuery") || fileType1.equals("OwnQuery")) {
						srcCols_dt = paramDetails.getColumns();
					}
				}else if(axe[0].equalsIgnoreCase("AWS-S3")){
					String [] splitFile = axe[2].split("/");
					TestParamTO testParmsAws = jdbcTemplateJobStatusDao.isParamsPresent(splitFile[1]+"-"+axe[1],prjName);
					if (testParmsAws.getType().equalsIgnoreCase("AWSParquet")){
						path1 = "s3n://"+testParmsAws.getPath();	        		
						fileType1 = testParmsAws.getType();
					} else if(testParmsAws.getType().equalsIgnoreCase("AWS-S3") && testParmsAws.getPath().contains(axe[2])){
						fileType1 = testParmsAws.getType();						 
						path1 = "s3n://"+testParmsAws.getPath();
						
					}
				}
				
				logger.info("Source File Type : "+fileType1);
				logger.info("Source Path :"+path1);
					
			//// To get the Target File parameters ************************************************************************
				
				String[] axeTarget = file2.split("\\.");
				TestParamTO testParmsTarget = null;
				if(axeTarget.length >2)
				testParmsTarget = jdbcTemplateJobStatusDao.isParamsPresent(axeTarget[2],prjName);
				else
				testParmsTarget = jdbcTemplateJobStatusDao.isParamsPresent(axeTarget[1],prjName);
				if (axeTarget.length>2 && !axeTarget[0].equalsIgnoreCase("Files") && !axeTarget[0].equalsIgnoreCase("AWS-S3") && !axeTarget[0].equalsIgnoreCase("Join") && !axeTarget[0].equalsIgnoreCase("CustomQuery") && !axeTarget[0].equalsIgnoreCase("OwnQuery")) {
					String[] splitTypeTarget = axeTarget[0].split("-");
					DBConfigTO dbDetailsTarget = jdbcTemplateJobStatusDao.fetchSubConnection(splitTypeTarget[1]);
					String getDBdetailStichTarget = dbDetailsTarget.getIp()+","+dbDetailsTarget.getPort()+","+dbDetailsTarget.getDatabase()+","+dbDetailsTarget.getUser()+","+eD.decrypt(dbDetailsTarget.getPassword());
					if(file2.contains("Oracle")){
						if(splitTypeTarget[1].equalsIgnoreCase(dbDetailsTarget.getConnectionName())){
							path2 = getDBdetailStichTarget + "," + axeTarget[1]+"."+axeTarget[2];
							fileType2 = dbDetailsTarget.getType();
						}						
					}else if(file2.contains("PostgreSQL")){
						if(splitTypeTarget[1].equalsIgnoreCase(dbDetailsTarget.getConnectionName())){
							path2 = getDBdetailStichTarget + "," + axeTarget[1]+"."+axeTarget[2];
							fileType2 = dbDetailsTarget.getType();
						}						
					} else if(dbDetailsTarget.getDatabase().equalsIgnoreCase(axeTarget[1]) && axeTarget[0].contains(dbDetailsTarget.getType())){
						path2 = getDBdetailStichTarget + "," +axeTarget[2];
						fileType2 = dbDetailsTarget.getType();
					}else if(file2.contains("AWS Redshift") && axeTarget[0].equalsIgnoreCase(dbDetailsTarget.getType())){
						path2 = getDBdetailStichTarget + "," +axeTarget[2]+ "," +axeTarget[1];
						fileType2 = dbDetailsTarget.getType();
					}
					
				} else if (axeTarget[0].equalsIgnoreCase("Files") && file2.contains(testParmsTarget.getConnectionName())) {
					// System.out.println(a1[2]);
					path2 = testParmsTarget.getPath();
					fileType2 = testParmsTarget.getType();				
				} else if((axeTarget[0].contains("Join") && file2.contains(testParmsTarget.getConnectionName()))  || (axeTarget[0].contains("CustomQuery") && file2.contains(testParmsTarget.getConnectionName())) || (axeTarget[0].contains("OwnQuery") && file2.contains(testParmsTarget.getConnectionName()))){
					JoinCustomTO paramDetailsTarget = jdbcTemplateJobStatusDao.fetchJoinCustomDetails(testParmsTarget.getConnectionName());
					path2 = paramDetailsTarget.getFileName();
					fileType2 = paramDetailsTarget.getType();
					if (fileType2.equals("Join") || fileType2.equals("CustomQuery") || fileType2.equals("OwnQuery")) {
						srcCols_dt = paramDetailsTarget.getColumns();
					}
				} else if(axeTarget[0].equalsIgnoreCase("AWS-S3")){
					String [] splitFile = axe[2].split("/");
					TestParamTO testParmsAws = jdbcTemplateJobStatusDao.isParamsPresent(splitFile[1]+"-"+axe[1],prjName);
					if (testParmsAws.getType().equalsIgnoreCase("AWSParquet")){
		        		path2 = "s3n://"+testParmsAws.getPath(); 
						fileType2 = testParmsAws.getType();
					} else if(testParmsAws.getType().equalsIgnoreCase("AWS-S3") && testParmsAws.getPath().contains(axeTarget[2])) {
						fileType2 = testParmsAws.getType();
						path2 = "s3n://"+testParmsAws.getPath();
						
					}
					
			}

			logger.info("Target File Type : " + fileType2);
			logger.info("Target Path :" + path2);

			String[] abc1 = srcCols.split(",");
			String[] abc2 = tgtCols.split(",");

			srcCols = "";
			tgtCols = "";
			String[] sp = null;
			for (String a : abc1) {
				sp = a.split(" --> ");
				srcCols = srcCols + sp[0] + ",";
			}
			for (String a : abc2) {
				sp = a.split(" --> ");
				tgtCols = tgtCols + sp[0] + ",";
			}

			timedifference a = new timedifference();
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			Date date = new Date();
			String tid = prjName + "_" + dateFormat.format(date);
			srcCols = srcCols.substring(0, srcCols.length() - 1);
			tgtCols = tgtCols.substring(0, tgtCols.length() - 1);
			srChgDt = srChgDt.substring(0, srChgDt.length() - 1);
			tgtDt = tgtDt.substring(0, tgtDt.length() - 1);
			String srcCols1 = srcCols, tgtCols1 = tgtCols;
			if (!srChgDt.equals("NA")) {
				srcCols1 = srcCols;
				String[] dtp = srChgDt.split(",");
				String[] cdp = null;
				for (String dt : dtp) {
					cdp = dt.split(" --> ");
					if (srcCols.contains(cdp[0])) {
						srcCols1 = srcCols1.replace(cdp[0], " cast(" + cdp[0] + " as " + cdp[1] + ")");
					}
				}
			}
			if (!tgtDt.equals("NA")) {
				tgtCols1 = tgtCols;
				String[] tgt = tgtDt.split(",");
				String[] tdp = null;
				for (String td : tgt) {
					tdp = td.split(" --> ");
					if (tgtCols.contains(tdp[0])) {
						tgtCols1 = tgtCols1.replace(tdp[0], " cast(" + tdp[0] + " as " + tdp[1] + ")");
					}
				}
			}
			String[] startTime = Calendar.getInstance().getTime().toString().split(" ");
			String doc = startTime[2] + "-" + startTime[1] + "-" + startTime[5];
			String toc = startTime[3].replace(':', '_');
			// String user = "administrator";
			String logText = tid + "##" + tstName + "(Saved)" + "##" + doc + "##" + toc + "##NA##NA##" + username
					+ "##execTime##" + file1 + "##" + srckeys + "##" + srcRuleApp + "##" + srcFilters + "##" + srcCols
					+ "##" + file2 + "##" + tgtkeys + "##" + tgtRuleApp + "##" + tgtFilters + "##" + tgtCols + "##"
					+ fileType1 + "##" + fileType2 + "##" + srcColsList + "##" + tgtColsList + "##" + srcRules + "##"
					+ tgtRules + "##" + filterParam + "##" + srcCols1 + "##" + tgtCols1 + "##" + srcClauses + "##"
					+ tgtClauses;
			String fileName = res_path + "jumbo_log/" + prjName + "/CD00123.txt";
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName, true));
			bufferedWriter.write(logText);
			bufferedWriter.newLine();
			bufferedWriter.close();
			message = tid;
			jdbcTemplateJobStatusDao.updateTestStatus(tstName, prjName, "Executed");
			TimeUnit.SECONDS.sleep(1);
		} catch (Exception e) {
			logger.error("Error in scrVariableSave: " + e.getMessage());
			String aspx = e.toString();
			aspx = aspx.replaceAll("\"", "'");
			aspx = aspx.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + aspx + "\"}", HttpStatus.OK);
		}
		logger.info("Leaving scrVariableSave");
		return new ResponseEntity<String>("{\"message\":\"" + message + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getTestId_old", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getTestId_old(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		String histDate = requestBody.get("histDate");
		String histTime = requestBody.get("histTime");

		String[] histDate1 = histDate.split(" ");
		String creDate = histDate1[0];
		String creTime = histDate1[1].replaceAll(":", "_");

		String[] histTime1 = histTime.split(" ");
		String exeDate = histTime1[0];
		String exeTime = histTime1[1].replaceAll(":", "_");

		Map<String, Object> testIDVal = new HashMap<String, Object>();

		String path = res_path + "jumbo_log/" + projectName + "/CD00123.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a;
		while (line != null) {
			a = line.split("##");
			if (a[1].equals(testName) && a[2].equals(creDate) && a[3].equals(creTime) && a[4].equals(exeDate)
					&& a[5].equals(exeTime)) {
				testIDVal.put("testID", a[0]);
				System.out.println("********************" + a[0]);
			}
			line = br.readLine();
		}
		return new ResponseEntity<Map<String, Object>>(testIDVal, HttpStatus.OK);
	}

	@RequestMapping(value = "/getTestId", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getTestId(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");

		Map<String, Object> testIDVal = new HashMap<String, Object>();

		String path = res_path + "jumbo_log/" + projectName + "/CD00123.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a;
		while (line != null) {
			a = line.split("##");
			if (a[1].equals(testName)) {
				testIDVal.put("testID", a[0]);
				System.out.println("********************" + a[0]);
			}
			line = br.readLine();
		}
		br.close();
		return new ResponseEntity<Map<String, Object>>(testIDVal, HttpStatus.OK);
	}

	@RequestMapping(value = "/reRun", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> reRun(@RequestBody Map<String, String> requestBody)
			throws IOException, InterruptedException, ClassNotFoundException, SQLException, Exception {
		String prjName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		String testDate = requestBody.get("testDate");
		String testTime = requestBody.get("testTime");
		String newsrc = requestBody.get("newsrc");
		String newtgt = requestBody.get("newtgt");
		String username = requestBody.get("userName");
		String res = manager.processReRun(prjName, testName, testDate, testTime, newsrc, newtgt, sc, spark, username);
		return new ResponseEntity<String>("{\"message\":\"" + res + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/jumboVjuJenkinslmtyTestvopRun02h", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> jumboVjuJenkinslmtyTestvopRun02h(@RequestBody Map<String, String> requestBody)
			throws IOException, InterruptedException, ClassNotFoundException, SQLException, Exception {
		String prjName = requestBody.get("projectName");
		String testID = requestBody.get("testID");
		
		String test = "";
		String path = res_path + "jumbo_log/" + prjName + "/CD00123.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a = null;
		while (line != null) {
			a = line.split("##");
			if (a[0].equals(testID))
				test = line;
			line = br.readLine();
		}
		br.close();
		
		a = test.split("##");
		
		String testName = a[1];
		String testDate = a[2]+" "+a[3];
		String testTime = a[4]+" "+a[5];
		String res = manager.processReRun(prjName, testName, testDate, testTime, "", "", sc, spark, "jenkins");
		
		if(res.contains("Exception")){
			return new ResponseEntity<String>("{\"message\":\"" + res + "\"}", HttpStatus.OK);
		}
		else{
			Map<String, Object> statisticsMap = stats.getStats(prjName, res, sc, spark);
			Map<String, String> data = new HashMap<String, String>();

			for (Map.Entry<String, Object> entry : statisticsMap.entrySet()) {
				for (Map.Entry<String, Object> entry12 : ((Map<String, Object>) entry.getValue()).entrySet()) {
					data.put(entry12.getKey(), entry12.getValue().toString());
				}
			}
			
			int passFail = 0;
			if(Integer.parseInt(data.get("value1"))!=Integer.parseInt(data.get("value2")))
				passFail = 1;
			if(Integer.parseInt(data.get("value3")) > 0 || Integer.parseInt(data.get("value4")) > 0)
				passFail = 1;
			if(Integer.parseInt(data.get("value7")) > 0 || Integer.parseInt(data.get("value8")) > 0)
				passFail = 1;
			if((Integer.parseInt(data.get("value9"))==Integer.parseInt(data.get("value10"))) && (Integer.parseInt(data.get("value9")) > 0 && Integer.parseInt(data.get("value10")) > 0))
				passFail = 1;

			if(passFail==1)
				res = "Test Status : FAILED";
			else
				res = "Test Status : PASSED";
			
			res = res + "Project Name : " + data.get("proName") + "\n";
			res = res + "TestID/TestName : " + data.get("testName") + "\n";
			res = res + "Date of Creation : " + data.get("doc") + "\n";
			res = res + "Date of Execution : " + data.get("doe") + "\n";
			res = res + "Created By : " + data.get("createdBy") + "\n";
			res = res + "Execution Time : " + data.get("duration") + "\n";
			res = res + "1. Total number of records in Source : " + data.get("value1") + "\n";
			res = res + "2. Total number of records in Target : " + data.get("value2") + "\n";
			int aq = 3;
			if (data.containsKey("value3") && data.containsKey("value4")) {
				res = res + aq+".1 Total number of duplicate key(s) in Source : " + data.get("value3") + "\n";
				res = res + aq+".2 Total number of duplicate key(s) in Target : " + data.get("value4") + "\n";
				aq++;
			}
			if (data.containsKey("value5") && data.containsKey("value6")) {
				res = res + aq+".1 Total number of Unique key(s) in Source : " + data.get("value5") + "\n";
				res = res + aq+".2 Total number of Unique key(s) in Target : " + data.get("value6") + "\n";
				aq++;
			}

			if (data.containsKey("value7") && data.containsKey("value8")) {
				res = res + aq+".1 Total number of Orphan key(s) in Source : " + data.get("value7") + "\n";
				res = res + aq+".2 Total number of Orphan key(s) in Target : " + data.get("value8") + "\n";
				aq++;
			}

			if (data.containsKey("value9") && data.containsKey("value10")) {
				res = res + aq+".1 Total number of Mismatches in Source : " + data.get("value9") + "\n";
				res = res + aq+".2 Total number of Mismatches in Target : " + data.get("value10") + "\n";
				aq++;
			}

			return new ResponseEntity<String>("{\"message\":\"" + res + "\"}", HttpStatus.OK);
		}
	}

	@RequestMapping(value = "/sparkJoin", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getSparkJoin(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {

		String prjName = requestBody.get("prjName");
		String queryStr = requestBody.get("queryStr");
		String tblName = requestBody.get("tblName");
		String joinFileName = requestBody.get("filename");
		String type = requestBody.get("type");

		try {
			// testPad tP = new testPad();
			// String fileResult = abc.joinParm(prjName, queryStr,
			// tblName,joinFileName);
			String fileResult = joinSpark.customAndJoinTable(prjName, queryStr, tblName, joinFileName, type);
			fileResult = fileResult.replaceAll("\n", " ");
			fileResult = fileResult.replaceAll("\"", "'");
			return new ResponseEntity<String>("{\"message\":\"" + fileResult + "\"}", HttpStatus.OK);
			// return new ResponseEntity<Map<String, Object>>(colList,
			// HttpStatus.OK);
		} catch (Exception e) {
			logger.error("Exception in getSparkJoin", e.getMessage());
			e.printStackTrace();
			String ex = e.getMessage();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + ex + "\"}", HttpStatus.OK);
		}

	}

	// verifyJoin
	@RequestMapping(value = "/verifyJoin", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> verifyUnionQuery(@RequestBody Map<String, String> requestBody) throws Exception {

		String prjName = requestBody.get("prjName");
		String tblName = requestBody.get("tblName");
		tblName = tblName.substring(0, tblName.lastIndexOf(","));
		String fileResult = jdbcHive.getVerifyColumn(prjName, tblName);
		return new ResponseEntity<String>("{\"message\":\"" + fileResult + "\"}", HttpStatus.OK);
	}

	@RequestMapping(value = "/runBatch", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> runBatch(@RequestBody Map<String, String> requestBody)
			throws IOException, InterruptedException {
		String projectName = requestBody.get("projectName");
		String batname = requestBody.get("batname");
		String batchFiles = requestBody.get("batchFiles");
		String username = requestBody.get("userName");
		String projectResult = manager.processBatch(projectName, batname, batchFiles, sc, spark, username);
		return new ResponseEntity<String>("{\"message\":\"" + projectResult + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getBatchResult", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getBatchResult(@RequestBody Map<String, String> requestBody)
			throws IOException {

		String prjName = requestBody.get("projectName");
		String batchName = requestBody.get("batchName");

		Map<String, Object> batchResultData = new HashMap<String, Object>();
		String totalTests = "", doc = "", doe = "", user = "", execTime = "", passFail = "", testNames = "";
		int totalPass = 0;
		BatchDetailsTO batchDetails = jdbcTemplateJobStatusDao.fetchBatchDetails(batchName, prjName);
		if (batchDetails.getBatchName().equalsIgnoreCase(batchName) && batchDetails.getDateOfExecution() != ""
				&& batchDetails.getTimeOfExecution() != "") {
			totalTests = String.valueOf(batchDetails.getTotalTests());
			doc = batchDetails.getDateOfCreation() + " " + batchDetails.getTimeOfCreation().replaceAll("_", ":");
			user = batchDetails.getUserName();
			// totalPass = String.valueOf(batchDetails.getTotalTests());
			doe = batchDetails.getDateOfExecution() + " " + batchDetails.getTimeOfExecution().replaceAll("_", ":");
			execTime = batchDetails.getTimeTaken();
			passFail = batchDetails.getStatus();
			testNames = batchDetails.getTestName();
			// testNames = testNames.substring(0, testNames.length() - 1);
			String[] names = testNames.split("##");
			String[] partition = null;
			testNames = "";
			for (String s : names) {
				partition = s.split(" --> ");
				testNames = testNames + partition[0] + ",";
			}
			testNames = testNames.substring(0, testNames.length() - 1);

			String[] aa1 = testNames.split(",");
			String[] aa2 = passFail.split(",");

			for (int i = 0; i < aa2.length; i++) {

				String[] avs = aa2[i].split("-");
				if (avs[1].equals("1")) {
					totalPass = totalPass + 1;
					aa2[i] = avs[0] + "-Passed";
				} else if (avs[1].equals("0"))
					aa2[i] = avs[0] + "-Failed";

			}

			String tableData = "";
			int slNo = 0;
			for (int j = 0; j < aa1.length; j++) {
				slNo++;
				String avs[] = aa2[j].split("-");

				if (avs[1].equalsIgnoreCase("Passed") || avs[1].equalsIgnoreCase("Failed")) {
					tableData = tableData + "<tr><td><font size=\"3\"><b>" + slNo + ". " + aa1[j] + "</b></font></td>"
							+ "<td><b><font size=\"3\"><a id=\"val" + (j + 1) + "\" onclick=\"individualResult('"
							+ (j + 1) + "')\">" + avs[1] + "</a></font></b></td></tr>";
				} else {
					aa2[j] = aa2[j].replaceAll("\n", " ");
					aa2[j] = aa2[j].replaceAll("'`", "*");
					aa2[j] = aa2[j].replaceAll("`'", "*");
					tableData = tableData + "<tr><td><font size=\"3\"><b>" + slNo + ". " + aa1[j] + "</b></font></td>"
							+ "<td title='" + aa2[j] + "'><b><font size=\"3\">Error</font></b></td></tr>";
				}
			}

			batchResultData.put("projectName", prjName);
			batchResultData.put("batchName", batchName);
			batchResultData.put("totalTests", totalTests);
			batchResultData.put("totalPass", totalPass);
			batchResultData.put("totalFail", Integer.parseInt(totalTests) - totalPass);
			batchResultData.put("doc", doc);
			batchResultData.put("doe", doe);
			batchResultData.put("user", user);
			batchResultData.put("execTime", execTime);
			batchResultData.put("tableData", tableData);
		} else {
			batchResultData.put("message", "Batch Not Executed . Please execute the batch and check results.");
		}

		return new ResponseEntity<Map<String, Object>>(batchResultData, HttpStatus.OK);

	}

	@RequestMapping(value = "/getIndividualTestResult", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getIndividualTestResult(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("projectName");
		String batchName = requestBody.get("batchName");
		String value = requestBody.get("value");

		System.out.println(projectName);
		System.out.println(batchName);
		System.out.println(value);

		String testID = "";
		Map<String, Object> batchInfo = new HashMap<String, Object>();
		String info = "";
		BatchDetailsTO batchDetails = jdbcTemplateJobStatusDao.fetchBatchDetails(batchName, projectName);

		String[] abs = batchDetails.getStatus().split(",");

		int x = 1;
		for (String asde : abs) {
			if (x == Integer.parseInt(value)) {
				String[] awesome = asde.split("-");
				testID = awesome[0];
			}
			x++;
		}

		/*
		 * if (batchDetails.getBatchName().equalsIgnoreCase(batchName)) { String
		 * [] file = batchDetails.getTestName().split("##"); info =
		 * file[Integer.parseInt(value)-1];
		 * System.out.println("/////////---------Captured Info : " + info); }
		 * 
		 * 
		 * 
		 * String[] info1 = info.split(" --> "); String[] cInfo =
		 * info1[1].split(" "); String[] eInfo = info1[2].split(" ");
		 * 
		 * String creDate = cInfo[0]; String creTime = cInfo[1].replaceAll(":",
		 * "_"); String exeDate = eInfo[0]; String exeTime =
		 * eInfo[1].replaceAll(":", "_");
		 * 
		 * String path = res_path + "jumbo_log/" + projectName + "/CD00123.txt";
		 * BufferedReader br = new BufferedReader(new FileReader(path)); String
		 * line = br.readLine(); String[] a = null; String testID = "";
		 * System.out.println("/////////---------Reading File CD00123"); while
		 * (line != null) { a = line.split("##"); if (a[1].contains(info1[0]) &&
		 * a[2].equals(creDate) && a[3].equals(creTime)) { testID = a[0];
		 * System.out.println("/////////---------Captured Test Id : " + testID);
		 * } line = br.readLine(); System.out.println("**"); } br.close();
		 * System.out.println("/////////---------Reading File CD00123 Complete"
		 * );
		 */
		batchInfo.put("testID", testID);

		return new ResponseEntity<Map<String, Object>>(batchInfo, HttpStatus.OK);
	}

	@RequestMapping(value = "/getDBTables", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getDBTables(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException, ClassNotFoundException {

		String ip_address = requestBody.get("ip_address");
		String port_number = requestBody.get("port_number");
		String schema_name = requestBody.get("schema_name");
		String databaseType = requestBody.get("databaseType");
		String user_name = requestBody.get("user_name");
		String pass = requestBody.get("pass");
		String tbllist = "";
		Connection con = null;
		SybDriver sybDriver = null;
		
		if(databaseType.contains("Oracle"))
			databaseType = "Oracle";

		try {
			if (databaseType.equalsIgnoreCase("mysql")) {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection("jdbc:mysql://" + ip_address + "/" + schema_name, user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}
			if (databaseType.equalsIgnoreCase("PostgreSQL")) {
				Class.forName("org.postgresql.Driver");
				// "jdbc:postgresql://172.25.224.110:5432/testdb","postgres",
				// "psql"
				con = DriverManager.getConnection(
						"jdbc:postgresql://" + ip_address + ":" + port_number + "/" + schema_name, user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}
			if (databaseType.equalsIgnoreCase("hive")) {

				Class.forName("com.cloudera.hive.jdbc4.HS2Driver");

				con = DriverManager.getConnection("jdbc:hive2://" + ip_address + ":" + port_number + "/" + schema_name
						+ ";AuthMech=3;SSL=1;SSLTrustStore=C:\\Program Files\\Java\\jre1.8.0_51\\lib\\security\\jssecacerts;UID="
						+ user_name + ";PWD=" + pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}

			}
			if (databaseType.equalsIgnoreCase("oracle")) {
				Class.forName("oracle.jdbc.OracleDriver");
				String orcURL = "";
				String pport = String.valueOf(port_number);
				if(pport.length()==5)
					orcURL = "jdbc:oracle:thin:@//" + ip_address + ":" + pport.substring(0,pport.length()-1) + "/" + schema_name;
				else
					orcURL = "jdbc:oracle:thin:@" + ip_address + ":" + port_number + ":" + schema_name;
				con = DriverManager.getConnection(orcURL, user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}

			if (databaseType.equalsIgnoreCase("ms sql(s)")) {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				con = DriverManager.getConnection(
						"jdbc:sqlserver://" + ip_address + ":" + port_number + ";DatabaseName=" + schema_name,
						user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}
			if (databaseType.equalsIgnoreCase("ms sql(w)")) {
				Class.forName("net.sourceforge.jtds.jdbc.Driver");

				System.out.println(ip_address);
				// String[] ipsplit = ip_address.split(Pattern.quote("\\"));
				String IPAddr = "";
				String instance = "";

				if (ip_address.contains("\\")) {
					IPAddr = ip_address.split(Pattern.quote("\\"))[0];
					instance = ip_address.split(Pattern.quote("\\"))[1];
				} else {
					IPAddr = ip_address;
					instance = "";
				}
				System.out.println(IPAddr);
				System.out.println(instance);

				System.out.println(user_name);
				String[] unameSplit = user_name.split(Pattern.quote("\\"));
				String domain = unameSplit[0];
				String uname = unameSplit[1];
				System.out.println(domain);
				System.out.println(uname);

				con = DriverManager.getConnection(
						// "jdbc:sqlserver://" + ip_address + ":" + port_number
						// + ";DatabaseName=" +
						// schema_name+";integratedSecurity=true;");
						"jdbc:jtds:sqlserver://" + IPAddr + ":" + port_number + "/" + schema_name + ";instance="
								+ instance + ";useNTLMv2=true;domain=" + domain + ";user=" + uname + ";password="
								+ pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}
			if (databaseType.equalsIgnoreCase("Azure MSSQL")) {

				System.out.println("*********Fetching Azure MSSQL tables*********");
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				con = DriverManager.getConnection("jdbc:sqlserver://" + ip_address + ":" + port_number
						+ ";DatabaseName=" + schema_name + ";user=" + user_name + ";password=" + pass
						+ ";encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;");
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}

			}

			if (databaseType.equalsIgnoreCase("Sybase")) {

				System.out.println("*********Fetching Sybase tables*********");
				sybDriver = (SybDriver) Class.forName("com.sybase.jdbc4.jdbc.SybDriver").newInstance();
				con = DriverManager.getConnection(
						"jdbc:sybase:Tds:" + ip_address + ":" + port_number + "/" + schema_name, user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}

			}
			if (databaseType.equalsIgnoreCase("AWS Redshift")) {
				String dbURL = "jdbc:redshift://" + ip_address + ":" + port_number + "/" + schema_name;
				/*
				 * String uname = "jumboredshift"; String password =
				 * "Password123";
				 */

				System.out.println("*********Fetching AWS Redshift tables*********");
				Class.forName("com.amazon.redshift.jdbc4.Driver");
				Properties props = new Properties();
				props.setProperty("user", user_name);
				props.setProperty("password", pass);
				con = DriverManager.getConnection(dbURL, props);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}
			if (databaseType.equalsIgnoreCase("Netezza")) {

				System.out.println("*********Fetching Netezza tables*********");
				Class.forName("org.netezza.Driver");
				con = DriverManager.getConnection(
						"jdbc:netezza://" + ip_address + ":" + port_number + "/" + schema_name, user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}

			}
			if (databaseType.equalsIgnoreCase("Cassandra")) {

				System.out.println("*********Fetching Cassandra tables*********");
				Class.forName("com.github.cassandra.jdbc.CassandraDriver");
				con = DriverManager.getConnection(
						"jdbc:c*:datastax://" + ip_address + ":" + port_number + "/" + schema_name, user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}

			}
			if (databaseType.equalsIgnoreCase("db2")) {

				System.out.println("*********Fetching Db2 tables*********");
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				con = DriverManager.getConnection("jdbc:db2://" + ip_address + ":" + port_number + "/" + schema_name,
						user_name, pass);
				if (con != null) {
					tbllist = "Success";
				} else {
					tbllist = "Failed to Connect";
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in getDBTables", e.getMessage());
			e.printStackTrace();
			String ex = e.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			tbllist = "Error : " + ex;
			return new ResponseEntity<String>(tbllist, HttpStatus.OK);
		}

		return new ResponseEntity<String>("{\"message\":\"" + tbllist + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/addDBTables", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addDBTables(@RequestBody Map<String, Object> requestBody) throws IOException {

		String ip_address = requestBody.get("ip_address").toString();
		String port_number = requestBody.get("port_number").toString();
		String schema_name = requestBody.get("schema_name").toString();
		String databaseType = requestBody.get("databaseType").toString();
		String user_name = requestBody.get("user_name").toString();
		String pass = requestBody.get("pass").toString();
		String connectionName = requestBody.get("connectionName").toString();
		String Result = "";
		int count = 0;
		
		if(databaseType.contains("Oracle"))
			databaseType = "Oracle";
		
		List<String> dbNameList = null;
		try {
			DBConfigTO dbDetails = new DBConfigTO();
			dbDetails.setConnectionName(connectionName);
			dbDetails.setType(databaseType);
			dbDetails.setIp(ip_address);
			dbDetails.setPort(Integer.parseInt(port_number));
			dbDetails.setDatabase(schema_name);
			dbDetails.setUser(user_name);
			dbDetails.setPassword(pass);
			dbNameList = jdbcTemplateJobStatusDao.searchDBNameConnection();
			if (dbNameList != null) {
				if (!dbNameList.contains(connectionName))
					count = jdbcTemplateJobStatusDao.insertDBConfig(dbDetails);
				else
					Result = "Already_Available";
			} else
				count = jdbcTemplateJobStatusDao.insertDBConfig(dbDetails);

			if (count == 1) {
				Result = "Success";
			} else {
				Result = "Failure";
			}
			/*
			 * List<String> tableNames = (List<String>)
			 * requestBody.get("tableNames");
			 * System.out.println("Table Names  : " + tableNames); // String[]
			 * tableName=tableNames.split(",");
			 * 
			 * // String projectName =
			 * requestBody.get("projectName").toString(); String logText = "";
			 * if (tableNames.equals("Table Names")) Result =
			 * "Check for the available tables in the Schema"; else { String
			 * fileName = res_path + "configs/DBConfigs.txt"; String[] phalo =
			 * null; int count = 0; File f = new File(fileName); if(f.exists())
			 * { BufferedReader bbr = new BufferedReader(new
			 * FileReader(fileName)); String lineman = bbr.readLine(); while
			 * (lineman != null) { phalo = lineman.split("##"); if
			 * (encryptDecrypt.decrypt(phalo[0]).equals(connectionName)) {
			 * count++; } lineman = bbr.readLine(); } bbr.close(); } if (count
			 * == 0) { BufferedWriter bufferedWriter = null; if(f.exists()) {
			 * bufferedWriter = new BufferedWriter(new FileWriter(fileName,
			 * true)); } else bufferedWriter = new BufferedWriter(new
			 * FileWriter(fileName)); if(databaseType.equals("Oracle")) logText
			 * = logText + encryptDecrypt.encrypt(connectionName) + "##" +
			 * encryptDecrypt.encrypt(user_name) + "##" +
			 * encryptDecrypt.encrypt(databaseType) + "##" +
			 * encryptDecrypt.encrypt(ip_address) + "," +
			 * encryptDecrypt.encrypt(port_number) + "," +
			 * encryptDecrypt.encrypt(schema_name) + "," +
			 * encryptDecrypt.encrypt(user_name) + "," +
			 * encryptDecrypt.encrypt(pass); else logText = logText +
			 * encryptDecrypt.encrypt(connectionName) + "##" +
			 * encryptDecrypt.encrypt(schema_name) + "##" +
			 * encryptDecrypt.encrypt(databaseType) + "##" +
			 * encryptDecrypt.encrypt(ip_address) + "," +
			 * encryptDecrypt.encrypt(port_number) + "," +
			 * encryptDecrypt.encrypt(schema_name) + "," +
			 * encryptDecrypt.encrypt(user_name) + "," +
			 * encryptDecrypt.encrypt(pass); bufferedWriter.write(logText);
			 * bufferedWriter.newLine(); bufferedWriter.close(); Result =
			 * "Success"; } else Result = "Already_Available"; }
			 */
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in addDBTables", e.getMessage());
			e.printStackTrace();
			String ex = e.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + ex + "\"}", HttpStatus.OK);
		}

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/updateDBTables", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> updateDBTables(@RequestBody Map<String, Object> requestBody) throws IOException {

		String pwd = requestBody.get("pwd").toString();
		String type = requestBody.get("type").toString();
		String schema_name = requestBody.get("schema").toString();
		type = type.split("-")[0];
		String Result = "";
		try {
			int r = jdbcTemplateJobStatusDao.updatePwd(pwd, type, schema_name);
			if (r > 0)
				Result = "Success";
			else
				Result = "Failure";
		} catch (Exception e) {
			logger.error("Exception in updateDBTables :", e.getMessage());
			Result = e.getMessage();
		}
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);
	}

	@RequestMapping(value = "/retrieveQuery", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> retrieveQuery(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException, ClassNotFoundException {
		String prjName = requestBody.get("prjName");
		String fileName = requestBody.get("fileName");

		String Result = "";
		try {

			String path = res_path + "jumbo_log/" + prjName + "/Files.txt";
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = br.readLine();
			String[] a;
			String srcCols = "";
			CSV_Reader generate = new CSV_Reader();
			// EXCEL_Reader generate1 = new EXCEL_Reader();

			if (fileName.contains(" --> ")) {
				String[] ppp = fileName.split(" --> ");
				while (line != null) {
					a = line.split("##");
					if (a[0].equals(ppp[2])) {
						srcCols = jdbcHive.dbTableColumns(ppp[1], a[2] + "," + ppp[0]);
					}
					line = br.readLine();
				}
				fileName = ppp[0];
			} else {
				while (line != null) {
					a = line.split("##");
					if (a[0].equals(fileName)) {
						if (a[1].equalsIgnoreCase("csv") || a[1].equalsIgnoreCase("dsv")
								|| a[1].equalsIgnoreCase("txt")) {
							if (a.length > 4) {
								srcCols = CSV_Reader.getFileColumnswithDataType(a[2], a[4], a[3], sc, spark, a[5]);
							} else {
								srcCols = CSV_Reader.getFileColumnswithDataType(a[2], "", a[3], sc, spark, "");
							}
						}
						
						else if (a[1].equalsIgnoreCase("Join") || a[1].equalsIgnoreCase("CustomQuery") || a[1].equalsIgnoreCase("OwnQuery"))
							srcCols = a[3];
					}
					line = br.readLine();
				}
			}

			System.out.println("*****" + srcCols);
			br.close();
			String[] splitter = srcCols.split("!");
			srcCols = "";
			for (String ab : splitter) {
				String[] ask = ab.split(" --> ");
				srcCols = srcCols + ask[0] + ",";
			}
			Result = "Select " + srcCols.substring(0, srcCols.length() - 1) + " from " + fileName;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in retrieveQuery", e.getMessage());
			e.printStackTrace();
			String ex = e.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + ex + "\"}", HttpStatus.OK);
		}
		System.out.println(Result);

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/loadSrcTgtDetails", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> loadSrcTgtDetails(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		String testDate = requestBody.get("testDate");
		String testTime = requestBody.get("testTime");

		String[] split1 = testDate.split(" ");
		String[] split2 = testTime.split(" ");

		String creDate = split1[0];
		String creTime = split1[1].replaceAll(":", "_");
		String exeDate = split2[0];
		String exeTime = split2[1].replaceAll(":", "_");

		Map<String, Object> values = new HashMap<String, Object>();

		String path = res_path + "jumbo_log/" + projectName + "/CD00123.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a;
		while (line != null) {
			a = line.split("##");
			if (a[1].equals(testName) && a[2].equals(creDate) && a[3].equals(creTime) && a[4].equals(exeDate)
					&& a[5].equals(exeTime)) {
				values.put("sFileName", a[8]);
				values.put("tFileName", a[13]);
				values.put("sKeys", a[9]);
				values.put("tKeys", a[14]);
				values.put("sRules", a[10]);
				values.put("tRules", a[15]);
				values.put("sFilters", a[11]);
				values.put("tFilters", a[16]);
			}
			line = br.readLine();
		}
		br.close();
		return new ResponseEntity<Map<String, Object>>(values, HttpStatus.OK);
	}

	@RequestMapping(value = "/createBatchTask", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> createBatchTask(@RequestBody Map<String, String> requestBody) {
		String Result = "";
		String projectName = requestBody.get("projectName");
		String batName = requestBody.get("batName");
		String batDate = requestBody.get("batDate");
		String batTime = requestBody.get("batTime");
		String etype = requestBody.get("etype");
		String days = requestBody.get("days");
		String sdate = requestBody.get("sdate");
		String edate = requestBody.get("edate");
		String user = requestBody.get("userName");
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = new Date();
		String tid = batName + "_" + dateFormat.format(date);

		SchedulerTO schedulerDetails = new SchedulerTO();
		schedulerDetails.setTestID(tid);
		schedulerDetails.setType("Batch");
		schedulerDetails.setProjectName(projectName);
		schedulerDetails.setBatchTestName(batName);
		schedulerDetails.setBatchDate(batDate);
		schedulerDetails.setBatchTime(batTime);
		schedulerDetails.setFrequencyType(etype);
		schedulerDetails.setFrequencyDays(days);
		schedulerDetails.setScheduledStartDate(sdate);
		schedulerDetails.setScheduledEndDate(edate);
		schedulerDetails.setUser(user);
		int count = jdbcTemplateJobStatusDao.insertSchedulerDetails(schedulerDetails);
		if (count == 1) {
			Result = "Success";
		} else {
			Result = "Failure";
		}
		/*
		 * String fileName = res_path + "Scheduler.txt"; BufferedWriter
		 * bufferedWriter = null; File f = new File(fileName); if(f.exists()) {
		 * bufferedWriter = new BufferedWriter(new FileWriter(fileName, true));
		 * } else bufferedWriter = new BufferedWriter(new FileWriter(fileName));
		 * //String tid = jdbcHive.taskIDgenerator(); DateFormat dateFormat =
		 * new SimpleDateFormat("yyyyMMddHHmmss"); Date date = new Date();
		 * String tid = batName+"_"+dateFormat.format(date); String logText
		 * =tid+ "##Batch##" + projectName + "##" + batName + "##" + batDate +
		 * "##" + batTime + "##" + etype + "##" + days + "##" + sdate + "##" +
		 * edate+ "##" + user; bufferedWriter.write(logText);
		 * bufferedWriter.newLine(); bufferedWriter.close(); Result = "Success";
		 */

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/createTestTask", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> createTestTask(@RequestBody Map<String, String> requestBody) {
		String Result = "";
		String projectName = requestBody.get("projectName");
		String batName = requestBody.get("testName");
		String batDate = requestBody.get("testDate");
		String batTime = requestBody.get("testTime");
		String etype = requestBody.get("etype");
		String days = requestBody.get("days");
		String sdate = requestBody.get("sdate");
		String edate = requestBody.get("edate");
		String user = requestBody.get("userName");
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = new Date();
		String tid = batName + "_" + dateFormat.format(date);

		SchedulerTO schedulerDetails = new SchedulerTO();
		schedulerDetails.setTestID(tid);
		schedulerDetails.setType("Test");
		schedulerDetails.setProjectName(projectName);
		schedulerDetails.setBatchTestName(batName);
		schedulerDetails.setBatchDate(batDate);
		schedulerDetails.setBatchTime(batTime);
		schedulerDetails.setFrequencyType(etype);
		schedulerDetails.setFrequencyDays(days);
		schedulerDetails.setScheduledStartDate(sdate);
		schedulerDetails.setScheduledEndDate(edate);
		schedulerDetails.setUser(user);
		int count = jdbcTemplateJobStatusDao.insertSchedulerDetails(schedulerDetails);
		if (count == 1) {
			Result = "Success";
		} else {
			Result = "Failure";
		}
		/*
		 * String fileName = res_path + "Scheduler.txt"; BufferedWriter
		 * bufferedWriter = null; File f = new File(fileName); if(f.exists()) {
		 * bufferedWriter = new BufferedWriter(new FileWriter(fileName, true));
		 * } else bufferedWriter = new BufferedWriter(new FileWriter(fileName));
		 * //String tid = jdbcHive.taskIDgenerator(); DateFormat dateFormat =
		 * new SimpleDateFormat("yyyyMMddHHmmss"); Date date = new Date();
		 * String tid = batName+"_"+dateFormat.format(date); String logText =
		 * tid+"##Test##" + projectName + "##" + batName + "##" + batDate + "##"
		 * + batTime + "##" + etype + "##" + days + "##" + sdate + "##" + edate+
		 * "##" + user; bufferedWriter.write(logText); bufferedWriter.newLine();
		 * bufferedWriter.close();
		 */

		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getFileDetails", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getFileDetails(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException {

		Map<String, Object> fileDetail = new HashMap<String, Object>();
		String proName = requestBody.get("proName");
		String filename = requestBody.get("filename");
		String srcTgtType = requestBody.get("srcTgtType");
		String path = res_path + "jumbo_log/" + proName + "/Files.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a = null;
		String[] abcd = null;
		while (line != null) {
			a = line.split("##");
			if (a[0].equals(filename)) {
				if (a[1].equalsIgnoreCase("csv") || a[1].equalsIgnoreCase("dsv") || a[1].equalsIgnoreCase("txt")
						|| a[1].equalsIgnoreCase("excel")) {
					if (srcTgtType.equalsIgnoreCase("source"))
						fileDetail.put("type", "Source Type : " + a[1]);
					else
						fileDetail.put("type", "Target Type : " + a[1]);
					fileDetail.put("dbName", " ");
					fileDetail.put("ipaddr", " ");
					fileDetail.put("schema", " ");
					fileDetail.put("details", " ");
					// details

				}
				if (a[1].equalsIgnoreCase("Join")) {
					abcd = a[2].split(",");
					String tblName = "";
					int fj = 0;
					for (String ab : abcd) {
						fj++;
						if (fj == 1)
							tblName = tblName + ab + "<br>";
						if (fj > 1)
							tblName = tblName
									+ "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
									+ ab + "<br>";
					}
					if (srcTgtType.equalsIgnoreCase("source"))
						fileDetail.put("type", "Source Type : " + a[1]);
					else
						fileDetail.put("type", "Target Type : " + a[1]);
					fileDetail.put("dbName", " ");
					fileDetail.put("ipaddr", " ");
					fileDetail.put("schema", " ");
					fileDetail.put("details", "Details :  " + tblName);
				}

			}
			if (filename.contains(" --> ")) {
				abcd = filename.split(" --> ");

				if (a[0].equals(abcd[2])) {
					String[] ab = a[2].split(",");
					if (srcTgtType.equalsIgnoreCase("source"))
						fileDetail.put("type", "Source Type : Database Table");
					else
						fileDetail.put("type", "Target Type : Database Table");
					// fileDetail.put("type","Source Type : Database Table");
					fileDetail.put("dbName", "DB Name     : " + a[1]);
					fileDetail.put("ipaddr", "IP Address  : " + ab[0]);
					fileDetail.put("schema", "Schema Name : " + abcd[2]);
					fileDetail.put("details", " ");

				}
			}
			line = br.readLine();
		}
		br.close();
		return new ResponseEntity<Map<String, Object>>(fileDetail, HttpStatus.OK);

	}

	@RequestMapping(value = "/downloadPDF/{proName}/{testID}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = "application/pdf")
	public FileSystemResource downloadPDF(@PathVariable("proName") String proName,
			@PathVariable("testID") String testID, HttpServletResponse response) throws Exception {
		pdfExport.pdfgenerate(proName, testID, sc, spark);
		File file = new File(res_path + "jumbo_log/" + proName + "/Result/" + testID + ".pdf");
		if (file.exists()) {
			response.setContentType("application/pdf");
			response.setHeader("Content-Length", String.valueOf(file.length()));
			response.setHeader("Content-Disposition", "inline; filename=" + file.getName());
		}
		return new FileSystemResource(file);

	}

	@RequestMapping(value = "/downloadEXCEL/{proName}/{testID}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	public FileSystemResource downloadEXCEL(@PathVariable("proName") String proName,
			@PathVariable("testID") String testID, HttpServletResponse response) throws Exception {
		excelExport.excelReport(proName, testID, sc, spark);
		File file = new File(res_path + "jumbo_log/" + proName + "/Result/" + testID + ".xlsx");
		if (file.exists()) {
			response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
			response.setHeader("Content-Length", String.valueOf(file.length()));
			response.setHeader("Content-Disposition", "inline; filename=" + file.getName());
		}
		return new FileSystemResource(file);

	}

	// Get All User Details
	@RequestMapping(value = "/allUsers", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<User>> getAllUserDetails() throws Exception {

		List<User> allUserList = jdbcHive.getAllUserDetails();
		if (allUserList.isEmpty()) {
			return new ResponseEntity<List<User>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<User>>(allUserList, HttpStatus.OK);

	}

	@RequestMapping(value = "/users/add", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addUserDetails(@RequestBody User user) throws Exception {

		String Result = jdbcHive.AddUserDetails(user);
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/users/emailIdCheck", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> updateUserEmailCheck(@RequestBody User user) throws Exception {

		String Result = jdbcHive.updateUserEmailCheck(user);
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/users/update", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> updateUserDetails(@RequestBody User user) throws Exception {

		String Result = jdbcHive.updateUserDetails(user);
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/users/delete", method = RequestMethod.POST)
	public ResponseEntity<String> removeUser(@RequestBody Map<String, Object> requestBody) throws Exception {
		boolean isRemoved = jdbcHive.deleteUserDetails(requestBody.get("username").toString());
		if (isRemoved == true) {
			return new ResponseEntity<String>("{\"message\":\"User Deleted\"}", HttpStatus.OK);
		} else {
			return new ResponseEntity<String>("{\"message\":\"User is not available\"}", HttpStatus.NOT_FOUND);
		}
	}

	// Get Specific User by Name and Password
	@RequestMapping(value = "/user/{id}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<User> getUserByUsername(@PathVariable("id") String userName) throws Exception {
		System.out.println("UserName for update  :   " + userName);
		User user = jdbcHive.getUserDetailsByName(userName);

		if (user == null) {
			return new ResponseEntity<User>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<User>(user, HttpStatus.OK);
	}

	@RequestMapping(value = "/isAuthenticated", method = RequestMethod.GET)
	@ResponseBody
	public Map<String, Object> currentUserDetails(Principal principal) {
		logger.info("Entering isAuthenticated");
		User activeUser = (User) ((Authentication) principal).getPrincipal();
		Map<String, Object> userDetails = new HashMap<String, Object>();
		userDetails.put("username", activeUser.getusername());
		userDetails.put("userrole", activeUser.getUserrole());
		userDetails.put("projectname", activeUser.getProjectname());
		userDetails.put("ssotoken", activeUser.getSsotoken());
		logger.info("Leaving isAuthenticated");
		return userDetails;

	}

	// Email configuration stored in emailConfig.txt
	@RequestMapping(value = "admin/addEmailConfig", method = RequestMethod.POST)
	public ResponseEntity<String> addEmailConfig(@RequestBody Map<String, Object> requestBody) throws Exception {
		String fromMailAdd = requestBody.get("fromMailId").toString();
		String smtpServerName = requestBody.get("smtpServer").toString();
		String smtpPortName = requestBody.get("smtpPort").toString();
		String SMTPUsername = requestBody.get("SMTPUsername").toString();
		String SMTPPassword = requestBody.get("SMTPPassword").toString();

		String isAdded = jdbcHive.addEmailDetails(fromMailAdd, smtpServerName, smtpPortName, SMTPUsername,
				SMTPPassword);

		return new ResponseEntity<String>("{\"message\":\"" + isAdded + "\"}", HttpStatus.OK);
	}

	// Send Email
	@RequestMapping(value = "sendEmailToAddr", method = RequestMethod.POST)
	public ResponseEntity<String> sendEmailTo(@RequestBody Map<String, Object> requestBody) throws Exception {
		String toMailAdd = requestBody.get("toMailId").toString();
		String prjName = requestBody.get("projectName").toString();
		String testId = requestBody.get("testId").toString();
		String result = jdbcHive.sendEmailDetails(toMailAdd, prjName, testId,sc,spark);
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
	}

	// Display EmailId
	@RequestMapping(value = "displayEmailId", method = RequestMethod.POST)
	public ResponseEntity<String> displayEmailId(@RequestBody Map<String, Object> requestBody) throws Exception {
		String projectName = requestBody.get("proName").toString();
		String emailIds = jdbcHive.displayEmailid(projectName);
		return new ResponseEntity<String>(emailIds, HttpStatus.OK);
	}
	
	// Get ReRun Data
	@RequestMapping(value = "/get/reRunData_working", method = RequestMethod.POST)
	public ResponseEntity<List<ProjectTO>> getRerunData_working(@RequestBody Map<String, Object> requestBody)
			throws Exception {
		ProjectTO projects = null;
		RunTimeTO time = null;
		List<RunTimeTO> timeList = null;
		RunDateTO date = null;
		List<RunDateTO> dateList = null;
		ChildNodeTO childNode = null;
		List<ChildNodeTO> childList = null;
		List<String> projectFiles = null;
		List<String> testDateList = null;
		List<String> testTimeList = null;
		List<ProjectTO> projectList = new ArrayList<ProjectTO>();
		RerunResponse response = new RerunResponse();
		String role = requestBody.get("role").toString();
		String userName = requestBody.get("loginName").toString();
		System.out.println("Role is :" + role);
		List<String> dbName = jdbcHive.projectNameList(userName);
		try {
			if (dbName != null && !dbName.isEmpty()) {
				for (String projectName : dbName) {
					projectFiles = new ArrayList<String>();
					projectFiles = jdbcHive.projectIDnames(projectName);
					if (projectFiles != null && !projectFiles.isEmpty()) {
						childList = new ArrayList<ChildNodeTO>();
						for (String fileName : projectFiles) {
							if (!fileName.equalsIgnoreCase("--No Test Names Available--")) {
								testDateList = jdbcHive.fetch_dates(projectName, fileName);
								if (testDateList != null && !testDateList.isEmpty()) {
									for (String testDate : testDateList) {
										if (!testDate.equalsIgnoreCase("--No Dates Available--")) {
											dateList = new ArrayList<RunDateTO>();
											testTimeList = jdbcHive.fetchtimestamp(projectName, fileName, testDate);
											timeList = new ArrayList<RunTimeTO>();
											for (String testTime : testTimeList) {
												if (!testTime.equalsIgnoreCase("--No Time Stamps Available--")) {
													time = new RunTimeTO("#" + testTime, testTime, projectName,
															fileName, testDate);
													timeList.add(time);
												}
											}
											date = new RunDateTO(testDate, "#" + testDate, timeList);
											dateList.add(date);
										}
									}
								}
								childNode = new ChildNodeTO(fileName, "#" + fileName, projectName, fileName);
								childList.add(childNode);
							}
						}
					}
					projects = new ProjectTO(projectName, "#" + projectName, childList);
					projectList.add(projects);
				}
				response.setProjectList(projectList);
			}
		} catch (Exception e) {
			logger.error("Exception occured in getRerunData : " + e.getMessage());
		}
		return new ResponseEntity<List<ProjectTO>>(projectList, HttpStatus.OK);
	}

	@RequestMapping(value = "/get/reRunData", method = RequestMethod.POST)
	public ResponseEntity<List<ProjectTO>> getRerunData(@RequestBody Map<String, Object> requestBody) throws Exception {
		ProjectTO projects = null;
		RunTimeTO time = null;
		List<RunTimeTO> timeList = null;
		RunDateTO date = null;
		List<RunDateTO> dateList = null;
		ChildNodeTO childNode = null;
		List<ChildNodeTO> childList = null;
		List<String> testDateList = null;
		List<String> testTimeList = null;
		List<ProjectTO> projectList = new ArrayList<ProjectTO>();
		RerunResponse response = new RerunResponse();
		String role = requestBody.get("role").toString();

		System.out.println("Role is :" + role);
		String projectName = (String) requestBody.get("prjName");
		List<String> testNameList = jdbcHive.projectIDnames(projectName);
		if (testNameList.size() > 0) {
			try {
				childList = new ArrayList<ChildNodeTO>();
				for (String fileName : testNameList) {
					if (!fileName.equalsIgnoreCase("--No Test Names Available--")) {
						testDateList = jdbcHive.fetch_dates(projectName, fileName);
						if (testDateList != null && !testDateList.isEmpty()) {
							for (String testDate : testDateList) {
								if (!testDate.equalsIgnoreCase("--No Dates Available--")) {
									dateList = new ArrayList<RunDateTO>();
									testTimeList = jdbcHive.fetchtimestamp(projectName, fileName, testDate);
									timeList = new ArrayList<RunTimeTO>();
									for (String testTime : testTimeList) {
										if (!testTime.equalsIgnoreCase("--No Time Stamps Available--")) {
											time = new RunTimeTO("#" + testTime, testTime, projectName, fileName,
													testDate);
											timeList.add(time);
										}
									}
									date = new RunDateTO(testDate, "#" + testDate, timeList);
									dateList.add(date);
								}
							}
						}
						childNode = new ChildNodeTO(fileName, "#" + fileName, projectName, fileName);
						childList.add(childNode);
					}
				}
				projects = new ProjectTO(projectName, "#" + projectName, childList);
				projectList.add(projects);

				response.setProjectList(projectList);

			} catch (Exception e) {
				logger.error("Exception occured in getRerunData : " + e.getMessage());
			}
		}
		return new ResponseEntity<List<ProjectTO>>(projectList, HttpStatus.OK);
	}

	// Get ReRun Data
	@RequestMapping(value = "/get/reRunData_old", method = RequestMethod.POST)
	public ResponseEntity<List<ProjectTO>> getRerunData_old(@RequestBody Map<String, Object> requestBody)
			throws Exception {
		ProjectTO projects = null;
		ChildNodeTO childNode = null;
		List<ChildNodeTO> childList = null;
		List<String> projectFiles = null;
		List<ProjectTO> projectList = new ArrayList<ProjectTO>();
		RerunResponse response = new RerunResponse();
		String role = requestBody.get("role").toString();
		String userName = requestBody.get("loginName").toString();
		System.out.println("Role is :" + role);
		List<String> dbName = jdbcHive.projectNameList(userName);
		try {
			if (dbName != null && !dbName.isEmpty()) {
				for (String projectName : dbName) {
					projectFiles = new ArrayList<String>();
					projectFiles = jdbcHive.projectIDnames(projectName);
					if (projectFiles != null && !projectFiles.isEmpty()) {
						childList = new ArrayList<ChildNodeTO>();
						for (String fileName : projectFiles) {
							if (!fileName.equalsIgnoreCase("--No Test Names Available--")) {
								childNode = new ChildNodeTO(fileName, "#" + fileName, projectName, fileName);
								childList.add(childNode);
							}
						}
					}
					projects = new ProjectTO(projectName, "#" + projectName, childList);
					projectList.add(projects);
				}
				response.setProjectList(projectList);
			}
		} catch (Exception e) {
			logger.error("Exception occured in getRerunData : " + e.getMessage());
		}
		return new ResponseEntity<List<ProjectTO>>(projectList, HttpStatus.OK);
	}

	// Check the selected batch is already executed or not
	@RequestMapping(value = "/getExecutedBatchResult/{projectName}/{batchName}", method = RequestMethod.GET)
	public ResponseEntity<String> getExecutedBatchResult(@PathVariable String projectName,
			@PathVariable String batchName) throws Exception {
		String batchResult = jdbcHive.executedBatchResult(projectName, batchName);
		return new ResponseEntity<String>("{\"message\":\"" + batchResult + "\"}", HttpStatus.OK);
	}

	/********************************
	 * FOR DASHBOARD
	 ******************************************/

	@RequestMapping(value = "/getProjectCount/{role}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getProjectCount(@PathVariable String role) throws SQLException {
		logger.info("Entering getProjectCount");
		int cnt = 0;
		System.out.println("Fetching Project Names");
		String fixedWidthPath = res_path + "all_files" + "/FixedWidth";
		File dir = new File(fixedWidthPath);
		if (!dir.exists())
			dir.mkdirs();

		List<String> projectList = jdbcHive.projectnames(role);
		if (!projectList.isEmpty()) {
			cnt = projectList.size();
		}

		logger.info("Leaving getProjectCount");
		return new ResponseEntity<String>("{\"message\":\"" + cnt + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getUserCount", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getUserCount() throws SQLException, IOException {

		int cnt = 0;
		try {
			String path = res_path + "configs/CD.txt";
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = br.readLine();
			cnt = 0;
			while (line != null) {
				++cnt;
				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
			logger.error("Exception in getUserCount", e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			String ex = e.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + ex + "\"}", HttpStatus.OK);
		}
		return new ResponseEntity<String>("{\"message\":\"" + cnt + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/loadProjectDetails", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> loadProjectDetails() throws SQLException, IOException {

		String tableData = "";
		List<String> list = new ArrayList<String>();

		String[] files = new File(res_path + "jumbo_log").list();
		Arrays.sort(files);
		if (files.length > 0) {
			list = Arrays.asList(files);
		} else
			list.add("--NO PROJECTS AVAILABLE--");

		tableData = "<tr>" + "<th>Project Name</th>" + "<th align=center>Created On</th>" + "<th>Total Users</th>"
				+ "<th>Total Test Runs</th>" + "<th>Total Batch Runs</th>" + "<th>Total Tests Passed</th>"
				+ "<th>Total Tests Failed</th>" + "</tr>";

		for (String aps : list) {
			if (aps.equals("--NO PROJECTS AVAILABLE--")) {
				tableData = tableData + "<tr><th colspan='7'>--NO PROJECTS AVAILABLE--</th></tr>";
				break;
			} else {
				String path = res_path + "jumbo_log/" + aps + "/DET.txt";
				boolean isDETPresent = new File(path).isFile();
				if (isDETPresent) {
					BufferedReader br = new BufferedReader(new FileReader(path));
					String line = br.readLine();
					tableData = tableData + "<tr>";
					tableData = tableData + "<td align=left>" + aps + "</td>";
					while (line != null) {
						tableData = tableData + "<td align=center>" + line + "</td>";
						line = br.readLine();
					}
					tableData = tableData + "</tr>";
					br.close();
				}
			}
		}
		return new ResponseEntity<String>("{\"message\":\"" + tableData + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/fetchPassValue", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> fetchPassValue() throws SQLException, IOException {

		String tableData = "";
		List<String> list = new ArrayList<String>();

		String[] files = new File(res_path + "jumbo_log").list();
		Arrays.sort(files);
		if (files.length > 0) {
			list = Arrays.asList(files);
		} else
			list.add("--NO PROJECTS AVAILABLE--");
		int sum = 0;
		for (String aps : list) {
			if (aps.equals("--NO PROJECTS AVAILABLE--")) {
				tableData = tableData + "0";
				break;
			} else {
				String path = res_path + "jumbo_log/" + aps + "/DET.txt";
				boolean isDETPresent = new File(path).isFile();
				if (isDETPresent) {
					BufferedReader br = new BufferedReader(new FileReader(path));
					String line = br.readLine();
					int a = 0;
					while (line != null) {
						++a;
						if (a == 5) {
							sum = sum + Integer.parseInt(line);
						}
						line = br.readLine();
					}
					br.close();
				}
			}
		}
		return new ResponseEntity<String>("{\"message\":\"" + sum + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/fetchFailValue", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> fetchFailValue() throws SQLException, IOException {

		String tableData = "";
		List<String> list = new ArrayList<String>();
		String[] files = new File(res_path + "jumbo_log").list();
		Arrays.sort(files);
		if (files.length > 0) {
			list = Arrays.asList(files);
		} else
			list.add("--NO PROJECTS AVAILABLE--");
		int sum = 0;
		for (String aps : list) {
			if (aps.equals("--NO PROJECTS AVAILABLE--")) {
				tableData = tableData + "0";
				break;
			} else {
				String path = res_path + "jumbo_log/" + aps + "/DET.txt";
				boolean isDETPresent = new File(path).isFile();
				if (isDETPresent) {
					BufferedReader br = new BufferedReader(new FileReader(path));
					String line = br.readLine();
					int a = 0;
					while (line != null) {
						++a;
						if (a == 6) {
							sum = sum + Integer.parseInt(line);
						}
						line = br.readLine();
					}
					br.close();
				}

			}
		}
		return new ResponseEntity<String>("{\"message\":\"" + sum + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getPassedValues", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getPassedValues() throws SQLException, IOException {

		List<String> list = new ArrayList<String>();
		String passedValue = "";
		String listVal = "";
		String[] files = new File(res_path + "jumbo_log").list();
		Arrays.sort(files);
		if (files.length > 0) {
			list = Arrays.asList(files);
			for (String aps : list) {
				String path = res_path + "jumbo_log/" + aps + "/DET.txt";
				boolean isDETPresent = new File(path).isFile();
				if (isDETPresent) {
					passedValue = Files.readAllLines(Paths.get(path)).get(4);
					listVal = listVal + passedValue + ",";
				}
			}
			listVal = listVal.substring(0, listVal.lastIndexOf(","));
		}
		return new ResponseEntity<String>("{\"message\":\"" + listVal + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getFailedValues", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getFailedValues() throws SQLException, IOException {

		List<String> list = new ArrayList<String>();
		String listVal = "";
		String failedValue = "";
		String[] files = new File(res_path + "jumbo_log").list();
		Arrays.sort(files);
		if (files.length > 0) {
			list = Arrays.asList(files);
			for (String aps : list) {
				String path = res_path + "jumbo_log/" + aps + "/DET.txt";
				boolean isDETPresent = new File(path).isFile();
				if (isDETPresent) {
					failedValue = Files.readAllLines(Paths.get(path)).get(5);
					listVal = listVal + failedValue + ",";
				}
			}
			listVal = listVal.substring(0, listVal.lastIndexOf(","));
		}
		return new ResponseEntity<String>("{\"message\":\"" + listVal + "\"}", HttpStatus.OK);

	}

	/***************************************************************************************/

	@RequestMapping(value = "/getFilesForDeleting", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getFilesForDeleting(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {

		String prjName = requestBody.get("projectName");
		List<String> fileName = new ArrayList<String>();
		if (prjName != "" && prjName != null) {
			fileName = jdbcHive.getFilesForDeleting(prjName);
		} else {
			fileName.add("File Not Available");
		}
		return new ResponseEntity<List<String>>(fileName, HttpStatus.OK);

	}

	@RequestMapping(value = "/deleteBatchFromProject", method = RequestMethod.POST)
	public ResponseEntity<String> deleteBatchFromProject(@RequestBody Map<String, Object> requestBody)
			throws Exception {

		String projectName = requestBody.get("projectName").toString();
		String batchName = requestBody.get("batchName").toString();
		String batchResult = jdbcHive.deleteBatchFromProject(projectName, batchName);
		System.out.println(batchResult + "*-*-*-*-*-*-*-*-*-*-*-*-*");
		return new ResponseEntity<String>("{\"message\":\"" + batchResult + "\"}", HttpStatus.OK);
	}

	@RequestMapping(value = "/getProjectLabels", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getProjectLabels() throws SQLException {

		System.out.println("Fetching Project Names");
		String dbName = jdbcHive.getProjectLabels();

		return new ResponseEntity<String>("{\"message\":\"" + dbName + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/verify", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> verify(@RequestBody Map<String, String> requestBody) throws SQLException {

		logger.info("Entering into Verify Rerun");
		String prjName = requestBody.get("projectName");
		String srcfileName = requestBody.get("srcfileName");
		String tgtfileName = requestBody.get("tgtfileName");
		String testID = requestBody.get("testID");
		Map<String, Object> srcColList = null;
		Map<String, Object> tgtcolList = null;
		String result = "success";
		try {
			if (StringUtils.hasText(srcfileName)) {
				srcColList = jdbcHive.getSrcTgtFileList(prjName, srcfileName, "", sc, spark, "");
			} else if (StringUtils.hasText(tgtfileName)) {
				tgtcolList = jdbcHive.getSrcTgtFileList(prjName, tgtfileName, "", sc, spark, "");
			}
			String path = res_path + "jumbo_log/" + prjName + "/CD00123.txt";
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = br.readLine();
			String[] a;
			String[] split;
			String[] srcArray;
			String[] tgtArray;
			List<String> srcList = new ArrayList<String>();
			List<String> tgtList = new ArrayList<String>();
			split = testID.split("/");
			while (line != null) {
				a = line.split("##");
				String completeSrcCols = a[20];
				String completeTgtCols = a[21];
				srcArray = completeSrcCols.split(",");
				tgtArray = completeTgtCols.split(",");
				if (a[0].equalsIgnoreCase(split[0])) {
					if (srcColList != null && !srcColList.isEmpty()) {
						srcList = (List<String>) srcColList.get("colsList");
						for (int i = 0; i <= srcList.size(); i++) {
							if (!srcList.get(i).equalsIgnoreCase(srcArray[i])) {
								result = "Verify failure: Selected Source is not matching with the project Source";
								break;
							}
						}
					}
					if (tgtcolList != null && !tgtcolList.isEmpty()) {
						tgtList = (List<String>) tgtcolList.get("colsList");
						for (int i = 0; i <= tgtList.size(); i++) {
							if (!srcList.get(i).equalsIgnoreCase(tgtArray[i])) {
								result = "Verify failure: Selected Target is not matching with the project Target";
								break;
							}
						}
					}
				}
				line = br.readLine();
			}

			br.close();
		} catch (IOException ioex) {
			// TODO Auto-generated catch block
			String ex = ioex.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			logger.error("Exception Occurred in verify: " + ioex.getMessage());
			return new ResponseEntity<String>("{\"message\":\"" + ex + "\"}", HttpStatus.OK);
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			logger.error("Exception Occurred in verify: " + ex.getMessage());
			String exp = ex.toString();
			exp = exp.replaceAll("\"", "'");
			exp = exp.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + exp + "\"}", HttpStatus.OK);
		}

		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/deleteConnName/{projectName}/{connectionName}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> deleteConnName(@PathVariable String projectName, @PathVariable String connectionName)
			throws IOException, ClassNotFoundException, SQLException {
		String result = "";
		try {
			int val = jdbcTemplateJobStatusDao.deleteDBConnection(connectionName);
			if (val == 1) {
				jdbcTemplateJobStatusDao.deleteFileForProject(projectName, connectionName);
				result = "Connection deleted successfully";
			} else
				result = "Error while deletion";
			/*
			 * BufferedReader br = new BufferedReader(new FileReader(res_path +
			 * "configs/DBConfigs.txt")); String line = br.readLine(); String[]
			 * ap = null; List<String> connList = new ArrayList<String>();
			 * while(line != null) { ap = line.split("##");
			 * if(encryptDecrypt.decrypt(ap[0]).equals(connectionName)){} else
			 * connList.add(line); line = br.readLine(); } br.close();
			 * 
			 * BufferedWriter bw = new BufferedWriter(new FileWriter(res_path +
			 * "configs/DBConfigs.txt")); for(String a : connList) {
			 * bw.write(a); bw.newLine(); } bw.close();
			 */
			return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in deleteConnName", e.getMessage());
			e.printStackTrace();
			result = "Error : " + e.toString();
			result = result.replaceAll("\"", "'");
			result = result.replaceAll("\n", " ");
			return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
		}
	}

	@RequestMapping(value = "/getStatus22", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getStatus22(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException, InterruptedException {

		String projectName = requestBody.get("proName");
		String testID = requestBody.get("testID");
		String[] filterParam = null;
		String path = res_path + "jumbo_log/" + projectName + "/CD00123.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a = null;
		String srcDetails = "", tgtDetails = "";
		Map<String, Object> statisticsMap = new HashMap<String, Object>();
		Map<String, Object> resultDetails = new HashMap<String, Object>();
		Map<String, Object> sourceDetails = new HashMap<String, Object>();
		Map<String, Object> targetDetails = new HashMap<String, Object>();
		Map<String, Object> statsData = new HashMap<String, Object>();
		try {
			while (line != null) {
				a = line.split("##");
				// System.out.println(a[0]);
				if (a[0].equals(testID))
					break;
				line = br.readLine();
			}
			if (a.length > 24) {
				filterParam = a[24].split(",");
			}

			br.close();
			String resultpath = res_path + "jumbo_log/" + projectName + "/Result/" + testID + "/";

			resultDetails.put("proName", projectName);
			resultDetails.put("testName", testID + "/" + a[1]);
			resultDetails.put("doc", a[2] + "  " + a[3].replaceAll("_", ":"));
			resultDetails.put("doe", a[4] + "  " + a[5].replaceAll("_", ":"));
			resultDetails.put("createdBy", a[6]);
			resultDetails.put("duration", a[7]);

			List<String> getSrcTgt = JumboUtils.getSourceTarget(projectName, a[8], a[13], jdbcTemplateJobStatusDao);
			if (getSrcTgt.size() > 0) {
				srcDetails = getSrcTgt.get(0);
				tgtDetails = getSrcTgt.get(1);
			}

			/*
			 * br=new BufferedReader(new
			 * FileReader(res_path+"jumbo_log/"+projectName+"/Files.txt"));
			 * String line1=br.readLine(); String[] a12 = null;
			 * while(line1!=null) { try { a12=line1.split("##");
			 * if(a12[0].equals(a[8]) && a12[1].equalsIgnoreCase(a[18])) {
			 * if(a12[1].equalsIgnoreCase("mysql") ||
			 * a12[1].equalsIgnoreCase("ms sql(s)") ||
			 * a12[1].equalsIgnoreCase("oracle") ||
			 * a12[1].equalsIgnoreCase("hive") ||
			 * a12[1].equalsIgnoreCase("Azure MSSQL")) { String[]
			 * abcd=a12[2].split(",");
			 * srcDetails="Type   : "+a12[1]+", IP   : "+abcd[0]+", Schema   : "
			 * +abcd[2]; } else if(a12[1].equalsIgnoreCase("ms sql(w)")) {
			 * String[] abcd=a12[2].split(",");
			 * srcDetails="Type   : "+a12[1]+", IP   : "+abcd[0].replaceAll(
			 * Pattern.quote("\\"),"-")+", Schema : "+abcd[2]; } else
			 * srcDetails="Type   : "+a12[1]; } line1=br.readLine(); } catch
			 * (Exception e) { e.printStackTrace(); } } br.close();
			 */

			/*
			 * br=new BufferedReader(new
			 * FileReader(res_path+"jumbo_log/"+projectName+"/Files.txt"));
			 * line1=br.readLine(); a12 = null; while(line1!=null) { try {
			 * a12=line1.split("##"); //System.out.println(a[0]);
			 * if(a12[0].equals(a[13]) && a12[1].equalsIgnoreCase(a[19])) {
			 * if(a12[1].equalsIgnoreCase("mysql") ||
			 * a12[1].equalsIgnoreCase("ms sql(s)") ||
			 * a12[1].equalsIgnoreCase("oracle") ||
			 * a12[1].equalsIgnoreCase("hive") ||
			 * a12[1].equalsIgnoreCase("Azure MSSQL")) { String[]
			 * abcd=a12[2].split(",");
			 * tgtDetails="Type   : "+a12[1]+", IP   : "+abcd[0]+", Schema   : "
			 * +abcd[2]; } else if(a12[1].equalsIgnoreCase("ms sql(w)")) {
			 * String[] abcd=a12[2].split(",");
			 * tgtDetails="Type   : "+a12[1]+", IP   : "+abcd[0].replaceAll(
			 * Pattern.quote("\\"),"-")+", Schema : "+abcd[2]; } else
			 * tgtDetails="Type   : "+a12[1]; } line1=br.readLine(); } catch
			 * (Exception e) { e.printStackTrace(); } } br.close();
			 */

			int srccolsCount = a[12].split(",").length;
			int tgtcolsCount = a[17].split(",").length;

			String[] srccolumns = a[12].split(",");
			String[] tgtcolumns = a[17].split(",");

			System.out.println("srcFile : " + a[8]);
			System.out.println("srcKey : " + a[9]);
			System.out.println("srcRules : " + a[10]);
			System.out.println("srcFilters : " + a[11]);
			System.out.println("srcDetails : " + srcDetails);
			System.out.println("srccolsCount : " + srccolsCount);

			System.out.println("tgtFile : " + a[13]);
			System.out.println("tgtKey : " + a[14]);
			System.out.println("tgtRules : " + a[15]);
			System.out.println("tgtFilters : " + a[16]);
			System.out.println("tgtDetails : " + tgtDetails);
			System.out.println("tgtcolsCount : " + tgtcolsCount);

			sourceDetails.put("srcFile", a[8]);
			sourceDetails.put("srcKey", a[9]);
			sourceDetails.put("srcRules", a[10]);
			sourceDetails.put("srcFilters", a[11]);
			sourceDetails.put("srcDetails", srcDetails);
			sourceDetails.put("srccolsCount", srccolsCount);

			targetDetails.put("tgtFile", a[13]);
			targetDetails.put("tgtKey", a[14]);
			targetDetails.put("tgtRules", a[15]);
			targetDetails.put("tgtFilters", a[16]);
			targetDetails.put("tgtDetails", tgtDetails);
			targetDetails.put("tgtcolsCount", tgtcolsCount);

			String values = parquetReader.getValues(resultpath, filterParam, sourceDetails, targetDetails, "", a[1]);
			String[] val = values.split("@@@@");

			statsData.put("tableData", val[0]);

			if (!a[1].contains("(Saved)")) {
				String[] sRc = val[1].split("-");
				String[] tGt = val[2].split("-");

				statsData.put("columnLevelMismatchSrc", sRc);
				statsData.put("value15cols", srccolumns);
				statsData.put("columnLevelMismatchTgt", tGt);
				statsData.put("value16cols", tgtcolumns);
			}
			statisticsMap.put("resultDetails", resultDetails);
			statisticsMap.put("sourceDetails", sourceDetails);
			statisticsMap.put("targetDetails", targetDetails);
			statisticsMap.put("statsData", statsData);
		} catch (Exception ex) {
			logger.error("Exception in getStatus22", ex.getMessage());
		}

		return new ResponseEntity<Map<String, Object>>(statisticsMap, HttpStatus.OK);

	}

	@RequestMapping(value = "/getRecords", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getRecords(@RequestBody Map<String, String> requestBody)
			throws IOException, InterruptedException {

		Map<String, Object> records = new HashMap<String, Object>();
		String projectName = requestBody.get("projectName").toString();
		String testID = requestBody.get("testID").toString();
		String value = requestBody.get("value").toString();
		String page = requestBody.get("page").toString();
		try {
			records = showValues.getRecords(projectName, testID, value, page, sc, spark);
			return new ResponseEntity<Map<String, Object>>(records, HttpStatus.OK);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in getRecords : ", e.getMessage());
			e.printStackTrace();
			String ex = e.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			records.put("resultDetails", "Error : " + ex);
			return new ResponseEntity<Map<String, Object>>(records, HttpStatus.OK);
		}
	}

	@RequestMapping(value = "/verifyLicense", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> verifyLicense() throws SQLException, IOException, ParseException {

		String result = "";

		try {
			File f = new File(res_path + "configs/licenseInfo.txt");
			if (f.exists()) {
				BufferedReader br = new BufferedReader(new FileReader(res_path + "configs/licenseInfo.txt"));
				String line = br.readLine();
				if (line != null) {
					String[] asa = line.split("!");
					String startDate = encryptDecrypt.decrypt(asa[2]);
					String validFor = encryptDecrypt.decrypt(asa[3]);
					SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy");
					Calendar c = Calendar.getInstance();

					Date now = new Date();
					String efgh = df.format(now);
					Date todaysDate = df.parse(efgh);

					String licenseStartDate = startDate;
					Date date1 = df.parse(licenseStartDate);
					c.setTime(date1);
					c.add(Calendar.DATE, Integer.parseInt(validFor));
					date1 = c.getTime();
					String abcd = df.format(date1);
					Date expiryDate = df.parse(abcd);

					long diff = expiryDate.getTime() - todaysDate.getTime();
					int daysRemaining = (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
					if (daysRemaining >= 0 && daysRemaining <= 6) {
						switch (daysRemaining) {
						case 0:
							result = "License Expiring Today";
							break;
						case 1:
							result = "License Expiring in " + daysRemaining + " day";
							break;
						case 2:
							result = "License Expiring in " + daysRemaining + " days";
							break;
						case 3:
							result = "License Expiring in " + daysRemaining + " days";
							break;
						case 4:
							result = "License Expiring in " + daysRemaining + " days";
							break;
						case 5:
							result = "License Expiring in " + daysRemaining + " days";
							break;
						case 6:
							result = "License Expiring in " + daysRemaining + " days";
							break;
						default:

						}
					} else if (daysRemaining >= 0) {
						result = "Accepted";
					} else if (daysRemaining < 0) {
						result = "Expired";
					} else if (todaysDate.compareTo(expiryDate) == 0) {
						result = "Expiring Today";
					} else {
						result = "Error";
					}
				}
			} else {
				result = "No License Details found. Contact OEG_Jumbo@hexaware.com";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in verifyLicense", e.getMessage());
			e.printStackTrace();
			if (e.toString().equals(
					"javax.crypto.IllegalBlockSizeException: Input length must be multiple of 16 when decrypting with padded cipher"))
				result = "Invalid License Key.";
			else {
				String ex = e.toString();
				ex = ex.replaceAll("\"", "'");
				ex = ex.replaceAll("\n", " ");
				result = "Error : " + ex;
			}
			return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
		}
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/viewLicenseDetails", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> viewLicenseDetails() throws SQLException {

		String result = "";

		try {

			File f = new File(res_path + "configs/licenseInfo.txt");
			if (f.exists()) {
				BufferedReader br = new BufferedReader(new FileReader(res_path + "configs/licenseInfo.txt"));
				String line = br.readLine();
				if (line != null) {
					String[] asa = line.split("!");
					String licensedTo = encryptDecrypt.decrypt(asa[0]);
					String licenseType = encryptDecrypt.decrypt(asa[1]);
					String startDate = encryptDecrypt.decrypt(asa[2]); // dd-mMM-yyyy
					String validFor = encryptDecrypt.decrypt(asa[3]);
				
					SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy");
					Calendar c = Calendar.getInstance();

					String licenseStartDate = startDate;
					Date date1 = df.parse(licenseStartDate);
					c.setTime(date1);
					c.add(Calendar.DATE, Integer.parseInt(validFor));
					date1 = c.getTime();
					String abcd = df.format(date1);
					result = "<b>Licensed To : " + licensedTo + " -- License Type : " + licenseType
							+ " -- Expiry Date : " + abcd + "</br>";
				}
				br.close();
			} else {
				result = "No License Details found. Contact OEG_Jumbo@hexaware.com";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in viewLicenseDetails", e.getMessage());
			e.printStackTrace();
			if (e.toString().equals(
					"javax.crypto.IllegalBlockSizeException: Input length must be multiple of 16 when decrypting with padded cipher"))
				result = "Invalid License Key.";
			else {
				String ex = e.toString();
				ex = ex.replaceAll("\"", "'");
				ex = ex.replaceAll("\n", " ");
				result = "Error : " + ex;
			}
			return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
		}
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/updateLicenseDetails", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> updateLicenseDetails(@RequestBody Map<String, String> requestBody)
			throws IOException, InterruptedException {

		String result = "";
		String username = requestBody.get("username").toString();
		String password = requestBody.get("password").toString();
		String licenseKey = requestBody.get("licenseKey").toString();

		try {
			BufferedReader br = new BufferedReader(new FileReader(res_path + "configs/CD.txt"));
			String line = br.readLine();
			int x = 10;
			while (line != null) {
				String[] abacus = line.split("##");
				System.out.println(encryptDecrypt.decrypt(abacus[0]));
				System.out.println(encryptDecrypt.decrypt(abacus[1]));
				if (username.equals(encryptDecrypt.decrypt(abacus[0]))) {
					if (password.equals(encryptDecrypt.decrypt(abacus[1]))) {
						x = 1;
						break;
					}
				} else
					x = 0;
				line = br.readLine();
			}
			if (x == 1) {
				BufferedWriter bw = new BufferedWriter(new FileWriter(res_path + "configs/licenseInfo.txt"));
				bw.write(licenseKey);
				bw.newLine();
				bw.close();
				result = "License Key Added Successfully";
			} else {
				result = "Check User Credentials";
			}

			return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in updateLicenseDetails", e.getMessage());
			e.printStackTrace();
			String ex = e.toString();
			ex = ex.replaceAll("\"", "'");
			ex = ex.replaceAll("\n", " ");
			result = "Error : " + ex;
			return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
		}
	}

	@RequestMapping(value = "/logout", method = RequestMethod.GET)
	public ResponseEntity<String> logoutPage(HttpServletRequest request, HttpServletResponse response) {
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (auth != null) {
			new SecurityContextLogoutHandler().logout(request, response, auth);
		}
		String Result = "Success";
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);
	}

	@RequestMapping(value = "/getTypes/{proName}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getProjectTypes(@PathVariable String proName) throws IOException {
		Set<String> fileList = jdbcHive.getProjectTypes(proName);
		return new ResponseEntity<Set<String>>(fileList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getProjectSchemaFile/{proName}/{type}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getProjectSchemaFile(@PathVariable String proName, @PathVariable String type)
			throws Exception {
		Set<String> fileList = jdbcHive.getProjectSchemaFiles(proName, type);
		return new ResponseEntity<Set<String>>(fileList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getProjectSchemaFile1/{proName}/{type}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getProjectSchemaFile1(@PathVariable String proName, @PathVariable String type)
			throws Exception {
		Set<String> fileList = jdbcHive.getProjectSchemaFiles1(proName, type);
		return new ResponseEntity<Set<String>>(fileList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getDataList", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getDataList(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		logger.info("Entering  getDataList");
		String prjName = requestBody.get("projectName");
		String type = requestBody.get("type");
		String schema = requestBody.get("schema");
		List<String> fileName = jdbcHive.getDBFileList(prjName, type, schema);
		logger.info("Leaving  getDataList");
		return new ResponseEntity<List<String>>(fileName, HttpStatus.OK);

	}

	@RequestMapping(value = "/getAwsConnection", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getAwsConnection(@RequestBody Map<String, String> requestBody) throws IOException {
		logger.info("Entering  getAwsConnection");
		String projectName = requestBody.get("proName");
		String awsFileType = requestBody.get("fileType");
		String awsFilePath = requestBody.get("awsPath");
		String delimiter = requestBody.get("delimeter");
		String connName = requestBody.get("connName");
		String isHeader = requestBody.get("isHeader");
		String headerFileName = requestBody.get("headerFileName");
		String isConnected = "Failure";
		if (JumboUtils.isNullorEmpty(projectName) && JumboUtils.isNullorEmpty(awsFilePath)) {
			isConnected = jdbcHive.getAwsConnection(projectName, delimiter, awsFileType, awsFilePath, connName,
					isHeader, headerFileName, spark, sc);
		}

		logger.info("Leaving  getAwsConnection");
		return new ResponseEntity<String>("{\"message\":\"" + isConnected + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getRunningTestList", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getRunningTestList(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		int status = Integer.parseInt(requestBody.get("testStatus"));
		List<JobStatus> testStatus = jdbcHive.getRunningTaskList(status);
		List<String> runningStatus = new ArrayList<String>();
		String divVal = "";
		if (testStatus.size() > 0) {
			for (JobStatus jb : testStatus) {
				divVal = "<div class=\"test-block\"><div class=\"col-lg-12\"><div class=\"form-group col-lg-3\"><label>Project Name : </label>"
						+ jb.getProjectname() + "</div> <div class=\"form-group col-lg-3\"><label>Test Name : </label>"
						+ jb.getTestname() + "</div>" + "<div class=\"form-group col-lg-3\"><label>User : </label>"
						+ jb.getUserName() + "</div><div class=\"form-group col-lg-3\">"
						+ "<label>Current Status :</label>Running</div></div><div class=\"col-lg-12\"><div><p><span class=\"pull-right text-muted\">"
						+ jb.getProgress() + "% Complete</span>"
						+ "</p><div class=\"progress progress-striped active\"><div class=\"progress-bar progress-bar-success\" role=\"progressbar\" aria-valuenow="
						+ jb.getProgress() + " aria-valuemin=\"0\" aria-valuemax=\"100\" style=\"width: "
						+ jb.getProgress() + "%\">" + "<span class=\"sr-only\">" + jb.getProgress()
						+ "% Complete (success)</span></div></div></div></div><div style=\"clear:both;\"></div></div> ";
				runningStatus.add(divVal);
			}
		}
		return new ResponseEntity<List<String>>(runningStatus, HttpStatus.OK);

	}

	/*
	 * Since we are regressively calling the getRunningTestList() to see the
	 * progress, unable to use for complete. That's why created the below method
	 * seperately.
	 */
	@RequestMapping(value = "/getCompleteTestList", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getCompleteTestList(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		logger.info("Entering  getCompleteTestList");
		String prjName = requestBody.get("prjName");
		String testName = requestBody.get("testName");
		int status = Integer.parseInt(requestBody.get("testStatus"));
		List<JobStatus> testStatus = null;
		if (prjName.length() > 0 && testName.length() > 0)
			testStatus = jdbcHive.getCompletedTaskList(prjName, testName, status);
		else
			testStatus = jdbcHive.getCompletedTaskList(status);

		List<String> completeStatus = new ArrayList<String>();
		String divVal = "<tbody>";
		String stat = "";
		if (testStatus.size() > 0) {
			for (JobStatus jb : testStatus) {
				if (jb.getStatus() == 2) {
					stat = "<td><span class='label label-success'>Pass</span></td>";
				} else if (jb.getStatus() == 3) {
					stat = "<td><span class='label label-danger'>&nbsp;Fail&nbsp;</span></td>";
				} else if (jb.getStatus() == 4) {
					stat = "<td>Error.Please check the log (../JumboFiles/Jumbo_Logs.txt)</td>";
				}
				/*
				 * divVal ="<tr><td>"+jb.getProjectname()+"</td>"+
				 * "<td> <a href=\"#\" onclick=\"showHistory('"+jb.
				 * getProjectname()+"','"+jb.getTestid()+"','"+jb.getSrcFile()+
				 * "')\">"+jb.getTestid()+"</a></td>"+
				 * "<td>"+jb.getTestname()+"</td>"+
				 * "<td>"+jb.getUserName()+"</td>"+
				 * "<td>"+jb.getCreatedtime()+"</td>"+
				 * "<td>"+jb.getRuntype()+"</td>"+stat+"</tr>";
				 */
				String[] sourceFile = jb.getSrcFile().split("\\.");
				String[] targetFile = jb.getTgtFile().split("\\.");
				divVal = "<tr><td>" + jb.getProjectname() + "</td>" + "<td> <a href=\"#\" onclick=\"showHistory('"
						+ jb.getProjectname() + "','" + jb.getTestid() + "','" + jb.getSrcFile() + "')\">"
						+ jb.getTestname() + "</a></td>" + "<td>" + sourceFile[sourceFile.length - 1] + "</td>" + "<td>"
						+ targetFile[targetFile.length - 1] + "</td>" + "<td>" + jb.getCreatedtime() + "</td>" + "<td>"
						+ jb.getRuntype() + "</td>" + stat + "</tr>";
				completeStatus.add(divVal);
			}
			divVal += "</tbody>";
			System.out.println("****DivVal: " + divVal);
		}
		logger.info("Leaving  getCompleteTestList");
		return new ResponseEntity<List<String>>(completeStatus, HttpStatus.OK);

	}

	@RequestMapping(value = "/getScheduleTaskList", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getScheduleTaskList() throws IOException, ClassNotFoundException, SQLException {
		logger.info("Entering  getScheduleTaskList");

		List<String> testStatus = jdbcHive.getScheduledTaskList();
		logger.info("Leaving  getScheduleTaskList");
		return new ResponseEntity<List<String>>(testStatus, HttpStatus.OK);

	}

	@RequestMapping(value = "/addCloudConnection", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addCloudConnection(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		logger.info("Entering  addCloudConnection");
		String instance = requestBody.get("instance");
		String accessKey = requestBody.get("accessKey");
		String secretAccessKey = requestBody.get("secretAccessKey");
		String bucketName = requestBody.get("bucketName");
		String connName = requestBody.get("connectionName");
		String fileName = jdbcHive.addCloudConn(instance, accessKey, secretAccessKey, bucketName, connName);
		logger.info("Leaving  addCloudConnection");
		return new ResponseEntity<String>("{\"message\":\"" + fileName + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/addQTestConnection", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> addQTestConnection(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		logger.info("Entering  addQTestConnection");
		// String proName = requestBody.get("proName");
		// String testID = requestBody.get("testID");
		String qturl = requestBody.get("qturl");
		String uname = requestBody.get("uname");
		String pwd = requestBody.get("pwd");
		String connName = requestBody.get("connectionName");
		String uuid = requestBody.get("uuid");
		// String pID = requestBody.get("pID");
		String fileName = jdbcHive.addQTestConn(qturl, uname, pwd, connName,uuid);
		logger.info("Leaving  addCloudConnection");
		return new ResponseEntity<String>("{\"message\":\"" + fileName + "\"}", HttpStatus.OK);

	}
	@RequestMapping(value = "/testQTestConnection", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> testQTestConnection(@RequestBody Map<String, String> requestBody){
		logger.info("Entering  testQTestConnection");
		String qturl = requestBody.get("qturl");
		String uname = requestBody.get("uname");
		String pwd = requestBody.get("pwd");
		String connName = requestBody.get("connectionName");
		String ssotoken = requestBody.get("ssotoken");
		String res = "";
		try{
			System.out.println(qturl+"/"+uname+"/"+pwd);
			QTestClient qt = new QTestClient();
			qt.LoginQTest(qturl, uname, pwd,ssotoken);
			res="Success";
		}catch(Exception e){			
				res=e.getMessage();
			}
		logger.info("Leaving  addCloudConnection"+res);
		return new ResponseEntity<String>("{\"message\":\"" + res + "\"}", HttpStatus.OK);
	}
	@RequestMapping(value = "/getQTestFields/{projectName}/{ssotoken}", method = RequestMethod.GET)
	public ResponseEntity<Map<String,ArrayList<String>>> getQTestFields(@PathVariable String projectName,@PathVariable String ssotoken) throws Exception{
		logger.info("Entering  getQTestFields");
		Map<String,ArrayList<String>> res = null;
		TestParamTO tpdata = jdbcTemplateJobStatusDao.fetchQTestConn(projectName);
				if(tpdata != null){
								DBConfigTO dbData = jdbcTemplateJobStatusDao.fetchSubConnection(tpdata.getConnectionName());				
								String ip_address = dbData.getIp();
								String uuid = dbData.getDatabase();
								String user_name = dbData.getUser();
								String pass = eD.decrypt(dbData.getPassword());
						   	 	long PID = Long.parseLong(tpdata.getHeaderFileName());
						   	 	//MID = Long.parseLong(tpdata.getPath());
						   	 	QTestClient qt = new QTestClient();
						   	 if(ssotoken != null && ssotoken.trim().length()>0 && !ssotoken.equals("NA")){
						   		if(ip_address.contains("http"))
							   	 	qt.serverUrl = ip_address + "/";
									else
									qt.serverUrl = "http://" + ip_address + "/";
							   	 	qt.accessToken = ssotoken;
						   	 }else{
						   	 	qt.LoginQTest(ip_address,user_name,pass,"NA");
						   	 }
						   	res =  qt.getFieldLists(PID, "test-runs");
				}
		logger.info("Leaving  getQTestFields"+res);
		return new ResponseEntity<Map<String,ArrayList<String>>>(res, HttpStatus.OK);
	}
	@RequestMapping(value = "/getCloudConnection/{connName}", method = RequestMethod.GET)
	public ResponseEntity<String> getCloudConnection(@PathVariable String connName) throws IOException {
		logger.info("Entering  getCloudConnection");
		String Result = "**";
		DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(connName);
		if (dbDetails.getConnectionName().equalsIgnoreCase(connName)
				&& dbDetails.getType().equalsIgnoreCase("AWS-S3")) {
			Result = dbDetails.getType() + " --> " + dbDetails.getIp();
		}
		/*
		 * String fileName = res_path + "configs/CloudConfig.txt";
		 * BufferedReader bbr = new BufferedReader(new FileReader(fileName));
		 * String lineman = bbr.readLine(); String[] phalo = null; String Result
		 * = "**"; try{ while (lineman != null) { phalo = lineman.split("##");
		 * if (encryptDecrypt.decrypt(phalo[0]).equals(connName)) { Result =
		 * encryptDecrypt.decrypt(phalo[4]) + " --> " +
		 * encryptDecrypt.decrypt(phalo[3]); } lineman = bbr.readLine(); }
		 * bbr.close(); } catch (Exception e) { // TODO Auto-generated catch
		 * block logger.error("Exception in getCloudConnection",e.getMessage()
		 * ); e.printStackTrace(); String aps = e.toString().replaceAll("\n",
		 * " "); aps = e.toString().replaceAll("\"", "'"); return new
		 * ResponseEntity<String>("{\"message\":\"" + aps + "\"}",
		 * HttpStatus.OK); }
		 */
		logger.info("Leaving  getCloudConnection");
		return new ResponseEntity<String>("{\"message\":\"" + Result + "\"}", HttpStatus.OK);
	}

	@RequestMapping(value = "/getCloudConnectionNames", method = RequestMethod.GET)
	public ResponseEntity<List<String>> getCloudConnectionNames() throws IOException {
		List<String> cloudList = new ArrayList<String>();
		List<DBConfigTO> connList = jdbcTemplateJobStatusDao.searchCloudConnection();
		for (DBConfigTO getConnName : connList) {
			if (getConnName.getType().equalsIgnoreCase("AWS-S3")) {
				cloudList.add(getConnName.getConnectionName());
			}
		}
		/*
		 * String fileName = res_path + "configs/CloudConfig.txt"; List<String>
		 * list = new ArrayList<String>(); File f = new File(fileName); String[]
		 * phalo = null; if(!f.exists()) { java.nio.file.Path newFilePath =
		 * Paths.get(fileName); Files.createFile(newFilePath); } BufferedReader
		 * bbr = new BufferedReader(new FileReader(fileName)); String lineman =
		 * bbr.readLine(); try { while (lineman != null) { phalo =
		 * lineman.split("##"); list.add(encryptDecrypt.decrypt(phalo[0]));
		 * lineman = bbr.readLine(); }
		 * 
		 * } catch (Exception e) {
		 * logger.error("Error in getCloudConnectionNames:" +e.getMessage());
		 * }finally{ bbr.close(); }
		 */
		return new ResponseEntity<List<String>>(cloudList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getQtestConnectionNames", method = RequestMethod.GET)
	public ResponseEntity<List<String>> getQTestConnectionNames() throws IOException {
		List<String> QTList = new ArrayList<String>();
		List<DBConfigTO> connList = jdbcTemplateJobStatusDao.fetchQTestConnection("QTest");
		for (DBConfigTO getConnName : connList) {
			if (getConnName.getType().equalsIgnoreCase("QTEST")) {
				QTList.add(getConnName.getConnectionName());
			}
		}
		return new ResponseEntity<List<String>>(QTList, HttpStatus.OK);
	}

	@RequestMapping(value = "/getQtestProjectNames/{conName}", method = RequestMethod.GET)
	public ResponseEntity<List<String>> getQtestProjectNames(@PathVariable String conName) throws Exception {

		System.out.println("connName ?????????????????????? " + conName);
		// String conName = "";
		List<String> QTList = new ArrayList<String>();
		try {
			DBConfigTO dbDetails = jdbcTemplateJobStatusDao.fetchSubConnection(conName);
			QTestClient qt = new QTestClient();
			qt.LoginQTest(dbDetails.getIp(), dbDetails.getUser(), eD.decrypt(dbDetails.getPassword()),dbDetails.getDatabase());
			QTList = QTestClient.getProjectList();
			// QTestClient.LogoutQTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new ResponseEntity<List<String>>(QTList, HttpStatus.OK);
	}

	@RequestMapping(value = "/fetchQTPMDetails/{prjID}", method = RequestMethod.GET)
	public ResponseEntity<List<String>> fetchQTPMDetails(@PathVariable String prjID) throws Exception {

		System.out.println("prjID ?????????????????????? " + prjID);
		// String conName = "";
		List<String> QTList = new ArrayList<String>();
		try {
			// DBConfigTO dbDetails =
			// jdbcTemplateJobStatusDao.fetchSubConnection(conName);
			// QTestClient.LoginQTest(dbDetails.getIp(),dbDetails.getUser(),eD.decrypt(dbDetails.getPassword()));
			QTList = QTestClient.getModuleList(Long.parseLong(prjID));
			QTestClient.LogoutQTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new ResponseEntity<List<String>>(QTList, HttpStatus.OK);
	}

	@RequestMapping(value = "/deleteInprogressStatus", method = RequestMethod.DELETE)
	public ResponseEntity<String> deleteInprogressStatus() throws IOException {
		String result = "";
		int isDelete = jdbcHive.deleteInprogressRecords(1, jdbcTemplateJobStatusDao);
		if (isDelete == 0) {
			result = "Success";
		} else {
			result = "Failure";
		}
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);
	}

	@RequestMapping(value = "/getTestDetails", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Map<String, Object>> getTestDetails(@RequestBody Map<String, String> requestBody)
			throws SQLException, IOException, InterruptedException {
		String projectName = requestBody.get("proName");
		String testName = requestBody.get("testName");
		Map<String, Object> testDetailMap = manager.editTestDetails(projectName, testName);
		return new ResponseEntity<Map<String, Object>>(testDetailMap, HttpStatus.OK);
	}

	@RequestMapping(value = "/upload", headers = "content-type=multipart/*", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String upload(MultipartHttpServletRequest request, HttpServletResponse response) {
		//logger.info("Entering  upload"); //Code change by Kiruthiga
		System.out.println("Entering  upload"); //Code change by Kiruthiga
		String result = "Failure";
		Iterator<String> itr = request.getFileNames();
		MultipartFile mpf = null;
		while (itr.hasNext()) {
			mpf = request.getFile(itr.next());
			try {
				Path path = Paths.get(res_path + "all_files/HeaderFiles");
				if (!Files.exists(path)) {
					Files.createDirectory(path);
				}
				FileCopyUtils.copy(mpf.getBytes(),
						new FileOutputStream(res_path + "all_files/HeaderFiles/" + mpf.getOriginalFilename()));
				result = "Success";
			} catch (IOException e) {
				logger.error("Exception in upload", e.getMessage());
			}
		}
		logger.info("Leaving  upload");
		return result;
	}

	@RequestMapping(value = "/getHeaders", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getHeaders() throws IOException, ClassNotFoundException, SQLException {
		logger.info("Entering  getHeaders");
		String filePath = res_path + "all_files/HeaderFiles";
		List<String> fileList = new ArrayList<String>();
		Path path = Paths.get(filePath);
		if (Files.exists(path)) {
			Files.list(Paths.get(filePath)).map(Path::getFileName).collect(toList())
					.forEach(a -> fileList.add(a.toString().substring(0, a.toString().length())));
		}
		logger.info("Leaving  getHeaders");
		return new ResponseEntity<List<String>>(fileList, HttpStatus.OK);

	}

	@RequestMapping(value = "/getProjectConnections", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Set<String>> getProjectConnections(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {

		String prjName = requestBody.get("projectName");
		Set<String> fileName = jdbcHive.getProjectConnectionsType(prjName);
		return new ResponseEntity<Set<String>>(fileName, HttpStatus.OK);

	}

	@RequestMapping(value = "/generateMapperTable", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> generateMapperTable(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {

		String projectName = requestBody.get("projectName");
		String Stype = requestBody.get("Stype");
		String Sschema = requestBody.get("Sschema");
		String Ttype = requestBody.get("Ttype");
		String Tschema = requestBody.get("Tschema");
		String tabType = requestBody.get("tabType");
		String shwtbl = requestBody.get("shwtbl");
		String shwview = requestBody.get("shwview");
		
		List<String> SourceList = jdbcHive.getDBFileList1(projectName, Stype, Sschema,shwtbl,shwview);
		List<String> TargetList = jdbcHive.getDBFileList1(projectName, Ttype, Tschema,shwtbl,shwview);
		String mapperTable = "";
		if(tabType.equals("CMV"))
			mapperTable = jdbcHive.createMapperTable(SourceList, TargetList, tabType);
		if(tabType.equals("DTV"))
			mapperTable = jdbcHive.fetchTableStats(SourceList, TargetList);
		return new ResponseEntity<String>("{\"message\":\"" + mapperTable + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getTableColumns", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getTableColumns(@RequestBody Map<String, String> requestBody) throws Exception {

		String projectName = requestBody.get("projectName");
		String type = requestBody.get("type");
		String schema = requestBody.get("schema");
		String table = requestBody.get("table");

		String path = jdbcHive.getFilePath(projectName, type + "." + schema + "." + table);

		String[] asdf = path.split("##");
		System.out.println("Type : " + type + "\nPath : " + asdf[0] + "," + table);

		String columnNames = jdbcHive.dbTableColumns(type, asdf[0] + "," + table);
		String[] base1 = columnNames.split("!");
		List<String> base = new ArrayList<String>();
		for (String s : base1)
			base.add(s);
		return new ResponseEntity<List<String>>(base, HttpStatus.OK);

	}

	@RequestMapping(value = "/getTestTreeView", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getTestTreeView(@RequestBody Map<String, String> requestBody)
			throws IOException, ClassNotFoundException, SQLException {
		String prjName = requestBody.get("prjName");
		List<String> testNameList = jdbcHive.projectIDnames(prjName);
		List<String> runningStatus = new ArrayList<String>();
		String divVal = "";
		int j = 2;
		if (testNameList.size() > 0) {
			for (String testName : testNameList) {
				divVal = "<li role=\"treeitem\" aria-selected=\"false\" aria-level=\"2\" aria-labelledby=\"j1_" + j
						+ "_anchor\" id=\"j1_" + j + "\" class=\"jstree-node  jstree-leaf\">"
						+ "<i class=\"jstree-icon jstree-ocl\" role=\"presentation\"></i>"
						+ "<a class=\"jstree-anchor music\" style=\"padding-left:4px;\" href=\"#\" tabindex=\"-1\" id=\"j1_"
						+ j + "_anchor\" onclick=getRerunTestDet('" + testName + "',\"j1_" + j + "\",\"j1_" + j
						+ "_anchor\");>"
						/* +"<i class=\"jstree-icon jstree-themeicon\" role=\"presentation\"></i>" */
						+ testName + "</a></li>";
				runningStatus.add(divVal);
				j++;
			}
		}
		return new ResponseEntity<List<String>>(runningStatus, HttpStatus.OK);

	}

	// Save Test Name.
	@RequestMapping(value = "/saveTestName", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> saveTestName(@RequestBody Map<String, String> requestBody) {

		String prjName = requestBody.get("prjName");
		String testName = requestBody.get("testName");
		String projectResult = jdbcHive.saveTestName(jdbcTemplateJobStatusDao, prjName, testName);
		return new ResponseEntity<String>("{\"message\":\"" + projectResult + "\"}", HttpStatus.OK);

	}

	// Get Test Id to rerun the latest test.
	@RequestMapping(value = "/getLatestTestId", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getLatestTestId(@RequestBody Map<String, String> requestBody) throws IOException {

		String prjName = requestBody.get("prjName");
		String testName = requestBody.get("testName");
		// String[] testId = null;
		String lastLine = "";
		String path = res_path + "jumbo_log/" + prjName + "/CD00123.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		String[] a = null;
		while (line != null) {
			a = line.split("##");
			// System.out.println(a[0]);
			if (a[1].equals(testName))
				lastLine = line;
			line = br.readLine();
		}
		br.close();
		// testId = lastLine.split("##");

		// return new ResponseEntity<String>("{\"testId\":\"" + testId + "\"}",
		// HttpStatus.OK);
		return new ResponseEntity<String>("{\"latestTestDetails\":\"" + lastLine + "\"}", HttpStatus.OK);

	}

	@RequestMapping(value = "/getAssoBatchNames/{projectName}/{testName}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> getAssoBatchNames(@PathVariable String projectName,
			@PathVariable String testName) throws IOException {
		List<String> testNameList = jdbcHive.associatedBatchNames(projectName, testName);
		return new ResponseEntity<List<String>>(testNameList, HttpStatus.OK);
	}

	@RequestMapping(value = "/displayPDF/{projectName}/{testID}", method = RequestMethod.GET, produces = "application/pdf")
	public ResponseEntity<byte[]> getPdf(@PathVariable String projectName, @PathVariable String testID,HttpServletResponse response) throws Exception
	{
		String path =res_path+"jumbo_log/";
		ResponseEntity<byte[]> responseData = null;
	    String result = manager.convertToPdf(projectName, testID);		
	    if(result == "Success") {
	    	Path filePath = Paths.get(path+projectName+"/Result/"+testID+"/pdf/result.pdf");
		    byte[] pdfContents = pdfContents = Files.readAllBytes(filePath);
		    HttpHeaders headers = new HttpHeaders();
		    headers.add("content-disposition", "inline;filename=result.pdf");
		    responseData = new ResponseEntity<byte[]>(
		    		pdfContents, headers, HttpStatus.OK);
	    } else {
	    	responseData = new ResponseEntity<byte[]>(
		    		null, null, HttpStatus.INTERNAL_SERVER_ERROR);
	    }
	    
	    
	    return responseData;
	}
	
	
	@RequestMapping(value = "/comparePDF", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> comparePDF(@RequestBody Map<String, String> requestBody) throws IOException {
		
		String sourcePDF = requestBody.get("sourcePdf");
		String targetPDF = requestBody.get("targetPdf");
		String proName = requestBody.get("proName");
		String testName = requestBody.get("testName");
		String result = manager.getPDFFilePath(sourcePDF,targetPDF,proName,testName);				        	
		return new ResponseEntity<String>("{\"message\":\"" + result + "\"}", HttpStatus.OK);

	}
	
	
	@RequestMapping(value = "/getVersion", method = RequestMethod.GET ,produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getVersion () throws IOException {
	    String version = manager.getVersionNumber();
	    return new ResponseEntity<String>("{\"message\":\"" + version + "\"}", HttpStatus.OK);
	}
		
	@RequestMapping(value = "/executeCustomQuery", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> executeCustomQuery(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException {

		String prjName = requestBody.get("prjName");
		String custTestName = requestBody.get("custTestName");
		String tblName = requestBody.get("tblName");
		String queryText = requestBody.get("queryStr").replace("\n", " ");
		String type = requestBody.get("type");
		String response ="";
		try {
			response = manager.processCustomQuery(prjName, custTestName, tblName, spark, queryText,type);
			return new ResponseEntity<String>("{\"message\":\"" + response + "\"}", HttpStatus.OK);
		} catch (Exception e) {
			logger.error("Exception in getSparkJoin", e.getMessage());
			e.printStackTrace();			
			return new ResponseEntity<String>("{\"message\":\"" + e.getMessage() + "\"}", HttpStatus.OK);
		}

	}
	
	@RequestMapping(value = "/ddlValidation", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> ddlValidation(@RequestBody Map<String, String> requestBody)
			throws Exception {
		String prjName = requestBody.get("prjName");
		String sourceConnection = requestBody.get("srcCon");
		String targetConnection = requestBody.get("tgtCon");
		String sourceSchema = requestBody.get("srcSch");
		String targetSchema = requestBody.get("tgtSch");
		String valTypes = requestBody.get("valTypes");
		String ddlTestName = requestBody.get("ddlTestName");
		String username = requestBody.get("username");
		String response = jdbcHive.ddlComparison(prjName, sourceConnection, targetConnection, sourceSchema, targetSchema, valTypes, ddlTestName, username);
		return new ResponseEntity<String>("{\"message\":\"" + response + "\"}", HttpStatus.OK);
	}
	
	@RequestMapping(value = "/validateDDLTestName", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> validateDDLTestName(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		File fileName = new File(res_path + "jumbo_log/" + projectName + "/Result");
		
		File[] filesList = fileName.listFiles();
		int ax = 0;
		
		if(filesList.length != 0){
			for(File dd : filesList){
				if(dd.getName().equals(testName + "_DDL"))
					ax++;
			}
		}
		
		String response ="";
		if(ax==0)
			response = "SUCCESS";
		else
			response = "Test Exists";
		
		return new ResponseEntity<String>("{\"message\":\"" + response + "\"}", HttpStatus.OK);

	}
	
	@RequestMapping(value = "/getDDLTESTS", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getDDLTESTS(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException {

		String projectName = requestBody.get("projectName");
		File fileName = new File(res_path + "jumbo_log/" + projectName + "/Result");
		File[] filesList = fileName.listFiles();
		int ax = 1;
		String tabData = "";
		if(filesList.length != 0){
			for(File dd : filesList){
				String ppt = dd.getName().substring(dd.getName().length()-4,dd.getName().length());
				if(ppt.equals("_DDL")){
					BufferedReader br=new BufferedReader(new FileReader(res_path + "jumbo_log/" + projectName + "/Result/"+ dd.getName() + "/" + dd.getName().substring(0,dd.getName().length()-4) + ".txt"));
					String line = br.readLine();
					String[] lineData = line.split("@@");
					br.close();
					if(lineData[15].equals("PASSED"))
						tabData = tabData + "<tr><td>" +ax+ "</td><td><a>" +dd.getName().substring(0,dd.getName().length()-4)+ "</a></td><td>" + lineData[6] + "</td><td>" + lineData[7] + "</td><td><span class='label label-success'>" + lineData[15] + "</span></td></tr>";
					else if(lineData[15].equals("FAILED"))
						tabData = tabData + "<tr><td>" +ax+ "</td><td><a>" +dd.getName().substring(0,dd.getName().length()-4)+ "</a></td><td>" + lineData[6] + "</td><td>" + lineData[7] + "</td><td><span class='label label-danger'>" + lineData[15] + "</span></td></tr>";
					ax++;
				}
			}
		}
		return new ResponseEntity<String>("{\"message\":\"" + tabData + "\"}", HttpStatus.OK);
	}
	
	@RequestMapping(value = "/retrieveDDLResult", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<String>> retrieveDDLResult(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		List<String> response = new ArrayList<String>();
		BufferedReader br=new BufferedReader(new FileReader(res_path + "jumbo_log/" + projectName + "/Result/"+ testName + "_DDL/" + testName + ".txt"));
		String line = br.readLine();
		String[] lineData;
		int lineNum = 1;
		while (line != null) {
			if(lineNum == 1){
				lineData = line.split("@@");
				for(String k : lineData){
					response.add(k);
				}
			}
			else{
				response.add(line);
			}
			lineNum++;
			line = br.readLine();
		}
		br.close();
		return new ResponseEntity<List<String>>(response, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/pullRecordsForResults", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> pullRecordsForResults(@RequestBody Map<String, String> requestBody)
			throws IOException, SQLException {

		String projectName = requestBody.get("projectName");
		String testName = requestBody.get("testName");
		String type = requestBody.get("type");
		
		BufferedReader br=new BufferedReader(new FileReader(res_path + "jumbo_log/" + projectName + "/Result/"+ testName + "_DDL/" + testName + ".txt"));
		String line = br.readLine();
		
		String tableData = "";
		
		if(type.equals("ORPSRC")){
			String[] xyz = line.split("@@");
			if(xyz[10].length() > 0){
				tableData = tableData + "<table class='table table-striped'>"+
											"<thead>"+
												"<tr bgcolor='#8cb8ff'>"+
													"<th>Sl No.</th>"+
													"<th>Table Names</th>"+
												"</tr>"+
											"</thead>"+
											"<tbody>";
				int aa = 1;
				for(String x : xyz[10].split(",")){
						tableData = tableData + "<tr>"+
													"<td>"+aa+"</td>"+
													"<td>"+x+"</td>"+
												"</tr>";
						aa++;
				}
					tableData = tableData + "</tbody>"+
										"</table>";
			}
			else{
				tableData = tableData + "<table class='table table-striped'>"+
											"<tr bgcolor='#8cb8ff'>"+
												"<th>No Table Data Available</th>"+
											"</tr>"+
										"</table>";
			}
		}
		if(type.equals("ORPTGT")){
			String[] xyz = line.split("@@");
			if(xyz[11].length() > 0){

				tableData = tableData + "<table class='table table-striped'>"+
											"<thead>"+
												"<tr bgcolor='#8cb8ff'>"+
													"<th>Sl No.</th>"+
													"<th>Table Names</th>"+
												"</tr>"+
											"</thead>"+
											"<tbody>";
				int aa = 1;
				for(String x : xyz[11].split(",")){
						tableData = tableData + "<tr>"+
													"<td>"+aa+"</td>"+
													"<td>"+x+"</td>"+
												"</tr>";
						aa++;
				}
					tableData = tableData + "</tbody>"+
										"</table>";
			}
			else{
				tableData = tableData + "<table class='table table-striped'>"+
											"<tr bgcolor='#8cb8ff'>"+
												"<th>No Table Data Available</th>"+
											"</tr>"+
										"</table>";
			}
		}
		br.close();
		
		return new ResponseEntity<String>("{\"message\":\"" + tableData + "\"}", HttpStatus.OK);
	}
//Code Added by Kiruthiga for Folder type selection - Start	
	
	
	@RequestMapping(value = "/uploadFolder", headers = "content-type=multipart/*", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String uploadFolder(MultipartHttpServletRequest request, HttpServletResponse response) {
		logger.info("Entering  upload"); //Code change by Kiruthiga
		System.out.println("Entering  upload folder"); //Code change by Kiruthiga
		String result = "Failure";
		Iterator<String> itr = request.getFileNames();
		MultipartFile mpf = null;
		while (itr.hasNext()) {
			mpf = request.getFile(itr.next());
			try {
				Path path = Paths.get(res_path + "all_files/FolderFiles");
				if (!Files.exists(path)) {
					Files.createDirectory(path);
				}
				FileCopyUtils.copy(mpf.getBytes(),
						new FileOutputStream(res_path + "all_files/FolderFiles/" + mpf.getOriginalFilename()));
				result = "Success";
			} catch (IOException e) {
				logger.error("Exception in upload", e.getMessage());
			}
		}
		logger.info("Leaving  upload folder");
		return result;
	}
	
		//@RequestMapping(value = "/addFolderToProj", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
		//public ResponseEntity<String> addFolderToProj(@RequestBody Map<String, String> requestBody) throws IOException {
	/*@RequestMapping(value = "/addFolderToProj", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)		
	public void addFolderToProj(@RequestBody File requestBody) throws IOException {
			//String type = "";
			//String rowTag = "";
			String projName = requestBody.get("projName");
			String firstFilePath = requestBody.get("firstFilePath");
			System.out.println("firstFilePath:"+ firstFilePath);
			int slashIndex = firstFilePath.lastIndexOf("\\");
			System.out.println("last index of \\:"+slashIndex);
			String folderPath = firstFilePath.substring(0,slashIndex);
			System.out.println("Folder Path:"+folderPath);
			File fold = new File(folderPath);
			System.out.println("folder exists:"+fold.exists());
			System.out.println("folder is directory:"+fold.isDirectory());
		
			//File[] files = requestBody.get("data");
			
			//String delimiter = requestBody.get("delimiter");
			
			//String a = jdbcHive.addFoldToProj(projName, fileName, type, delimiter, isHeader, sc , spark, headerFileName, rowTag);
	
			//return new ResponseEntity<String>("{\"message\":\"" + a + "\"}", HttpStatus.OK);
	
		}*/
//Code Added by Kiruthiga for Folder type selection - Start		
	
	
	
}