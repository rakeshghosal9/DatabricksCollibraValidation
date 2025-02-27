package org.databricks;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.builder.ResponseSpecBuilder;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.restassured.specification.ResponseSpecification;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static io.restassured.RestAssured.given;

public class ReusableCommonMethods {

    /**
     * This method will return a Properties file object that helps to load properties of a Properties file by passing
     * the file path of the Properties file. If the file path is invalid, then it returns null.
     *
     * @param filePath - Filepath of the properties file
     * @return Properties
     */
    public static Properties getPropertiesFileObject(String filePath) {
        try {
            FileInputStream fis = null;
            Properties prop = null;
            try {
                fis = new FileInputStream(filePath);
                prop = new Properties();
                prop.load(fis);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
            return prop;
        } catch (Exception e) {
            System.out.println("Exception occurred while reading properties file : " + e);
            return null;
        }
    }

    public static String readSQLQuery(String sqlPath) {
        try {
            if (sqlPath == null || sqlPath.trim().isEmpty()) {
                System.out.println("Please enter the sql_path property at the table level properties file");
                System.exit(0);
            }
            sqlPath = "src/test/resources/" + sqlPath;
            String sqlQuery = new String(Files.readAllBytes(Paths.get(sqlPath)));
            if (sqlQuery.isEmpty()) {
                return null;
            } else {
                return sqlQuery;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static Properties readGlobalProperty() {
        try {
            String globalPropertyFilePath = System.getProperty("user.dir") +
                    File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + "config"
                    + File.separator + "GLOBAL_PROPERTIES.properties";
            return ReusableCommonMethods.getPropertiesFileObject(globalPropertyFilePath);

        } catch (Exception e) {
            System.out.println("Exception occurred while reading global property file");
            return null;
        }
    }

    public static Properties readTableProperty(String tablePropertiesFileName) {
        try {
            String tablePropertyFilePath = System.getProperty("user.dir") +
                    File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + "config"
                    + File.separator + tablePropertiesFileName;
            return ReusableCommonMethods.getPropertiesFileObject(tablePropertyFilePath);
        } catch (Exception e) {
            System.out.println("Exception occurred while reading global property file");
            return null;
        }
    }

    public static ResultSet getDBConnection(Properties prop, String sqlQuery) {
        try {
            String jdbcURL = prop.getProperty("jdbcurl");
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", "token");
            connectionProperties.put("password", prop.getProperty("password"));
            Connection connection = DriverManager.getConnection(jdbcURL, connectionProperties);
            if (connection != null) {
                Statement statement = connection.createStatement();
                return statement.executeQuery(sqlQuery);
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println("Exception Occurred while establishing connection with database : " + e);
            return null;
        }
    }

    public static HashMap<String, HashMap<String, String>> getDataFromDB(ResultSet resultSet, String primaryKeyColumnName) {
        HashMap<String, HashMap<String, String>> dataFromDatabricks = new HashMap<>();
        try {
            String primaryKeyValue = null;
            ResultSetMetaData md = resultSet.getMetaData();
            int totalColumn = md.getColumnCount();
            while (resultSet.next()) {
                HashMap<String, String> temp = new HashMap<>();
                for (int i = 0; i <= totalColumn; i++) {
                    String columnName = md.getColumnName(i).toUpperCase();
                    if (columnName.contains(".")) {
                        int index = columnName.indexOf(".");
                        columnName = columnName.substring((index + 1));
                    }
                    if (columnName.equalsIgnoreCase(primaryKeyColumnName)) {
                        primaryKeyValue = resultSet.getObject(i).toString();
                    }
                    if (resultSet.getObject(i) != null) {
                        temp.put(columnName.toUpperCase(), resultSet.getObject(i).toString());
                    } else {
                        temp.put(columnName.toUpperCase(), "NULL");
                    }
                }
                dataFromDatabricks.put(primaryKeyValue, temp);
                if (dataFromDatabricks.size() % 10000 == 0) {
                    System.out.println("Total Records read from Databricks so far :" + dataFromDatabricks.size());
                }
            }
            System.out.println("Total Records read from Databricks : " + dataFromDatabricks.size());
            return dataFromDatabricks;

        } catch (Exception e) {
            System.out.println("Exception Occurred while fetching data from databricks : " + e);
            return dataFromDatabricks;
        }
    }

    public static String getRenewedAccessToken(Properties prop) {
        try {
            // Spotify API Token Renewal URL
            String tokenUrl = prop.getProperty("token_url");
            // Making the POST request
            Response response = given().relaxedHTTPSValidation().proxy(prop.getProperty("proxy"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Connection", "keep-alive")// Required header
                    .formParam("grant_type", prop.get("grant_type"))
                    .formParam("client_id", prop.get("client_id"))
                    .formParam("client_secret", prop.get("client_secret"))
                    .when()
                    .post(tokenUrl)
                    .then()
                    .statusCode(200)  // Validate success response
                    .extract()
                    .response();

            // Extract the new access token
            String accessToken = response.jsonPath().getString("access_token");
            // Print the new token
            System.out.println("New Access Token: " + accessToken);
            return accessToken;

        } catch (Exception e) {
            System.out.println("Exception Occurred while fetching the access token : " + e);
            return null;
        }
    }

    public static boolean performComparisonBetweenDBAndAPI
            (Properties globalProp, Properties tableProp, String accessToken, int expectedTotalRecordsToValidate,
             HashMap<String, HashMap<String, String>> dataFromDatabricks, String validationTableName) {
        try {
            String endPoint;
            String mappingFileName;
            String primaryKeyInJSONResponse;
            //Define the Request Specification
            RequestSpecBuilder reqBuilder = new RequestSpecBuilder().
                    setBaseUri(globalProp.getProperty("base_uri")).
                    setBasePath(globalProp.getProperty("base_path")).
                    addHeader("Authorization", "Bearer " + accessToken).
                    addHeader("Accept", "application/json").
                    addHeader("Content-Type", "application/json").
                    setContentType(ContentType.JSON).
                    log(LogDetail.URI);
            RequestSpecification requestSpecification = reqBuilder.build();

            //Define the Response Specification
            ResponseSpecBuilder resBuilder = new ResponseSpecBuilder()
                    .log(LogDetail.STATUS);
            ResponseSpecification responseSpecification = resBuilder.build();


            endPoint = tableProp.getProperty("asset_table_end_point");
            mappingFileName = tableProp.getProperty("asset_table_db_json_mapping_file_name");
            primaryKeyInJSONResponse = tableProp.getProperty("primary_key_in_json_response");

            String mappingFilePath = System.getProperty("user.dir") + File.separator + "src" + File.separator
                    + "test" + File.separator + "resources" + File.separator + mappingFileName;
            Properties mappingFileProperty = ReusableCommonMethods.getPropertiesFileObject(mappingFilePath);
            if (mappingFileProperty == null) {
                System.out.println("No Property file is available in the name of " +
                        "[" + mappingFileName + "] under folder DB_JSON_MAPPING");
            }

            int totalIteration = 1;
            int offset = 0;
            int limit = 1000;
            boolean overallValidationStatus = true;
            int totalRecordValidated = 0;
            int totalPass = 0;
            int totalFail = 0;
            int recordsLeft = 0;
            int calculatedLimit;
            HashMap<String, String> failureReport = new HashMap<>();
            List<HashMap<String, String>> successReport = new ArrayList<>();
            for (int i = 0; i < totalIteration; i++) {
                Response response = given(requestSpecification).proxy(globalProp.getProperty("proxy")).
                        when().param("offset", offset).param("limit", limit).
                        get(endPoint).
                        then().spec(responseSpecification).
                        assertThat().
                        statusCode(200).extract().response();
                if (expectedTotalRecordsToValidate <= 0) {
                    totalIteration = (response.jsonPath().getInt("total") / limit + 1);
                    if (i == 0) {
                        recordsLeft = response.jsonPath().getInt("total");
                    } else {
                        recordsLeft = recordsLeft - limit;
                    }
                } else {
                    totalIteration = expectedTotalRecordsToValidate / limit + 1;
                    if (i == 0) {
                        recordsLeft = expectedTotalRecordsToValidate;
                    } else {
                        recordsLeft = expectedTotalRecordsToValidate - limit;
                    }
                }
                if (recordsLeft > 0 && recordsLeft < 1000) {
                    calculatedLimit = recordsLeft;
                } else if (recordsLeft < 0) {
                    calculatedLimit = 0;
                } else {
                    calculatedLimit = limit;
                }
                for (int jsonIndex = 0; jsonIndex < calculatedLimit; jsonIndex++) {
                    StringBuilder result = new StringBuilder();
                    boolean eachJSONRecordValidationStatus = true;
                    totalRecordValidated++;
                    String primaryValue =
                            response.jsonPath().getString(("results[" + jsonIndex + "]." + primaryKeyInJSONResponse));
                    if (dataFromDatabricks.containsKey(primaryValue)) {
                        assert mappingFileProperty != null;
                        for (String jsonKey : mappingFileProperty.stringPropertyNames()) {
                            String valueFromJSON = response.jsonPath().getString("results[" + jsonIndex + "]." + jsonKey);
                            String valueFromDB = dataFromDatabricks.get(primaryValue).get(mappingFileProperty.getProperty(jsonKey).toUpperCase());
                            if (valueFromDB.compareTo(valueFromJSON) != 0) {
                                result.append("Value not matching for [").append(jsonKey).append("], ").append("Expected value from JSON [").append(valueFromJSON).append("],Actual value from DB [").append(valueFromDB).append("]").append("\n");
                                overallValidationStatus = false;
                                eachJSONRecordValidationStatus = false;
                            }
                        }

                    } else {
                        result.append("Primary Value  [").append(primaryValue).append("] is not present in Databricks Database");
                        overallValidationStatus = false;
                        eachJSONRecordValidationStatus = false;
                    }
                    if (eachJSONRecordValidationStatus) {
                        totalPass++;
                        successReport.add(dataFromDatabricks.get(primaryValue));
                    } else {
                        failureReport.put(primaryValue, result.toString());
                        totalFail++;
                    }
                }
                offset = offset + limit;
                System.out.println("Total Records Validate : [" + totalRecordValidated + "], Pass [" + totalPass + "], Fail [" + totalFail + "]");
            }
            //Write the failure report in an excel
            if (!failureReport.isEmpty()) {
                writeFailureReport(failureReport, globalProp.getProperty("reportPath"), validationTableName);
            }
            //Write the Success report in an excel
            if (!successReport.isEmpty()) {
                writeSuccessfulReport(mappingFilePath, globalProp.getProperty("reportPath"), validationTableName, successReport);
            }

            return overallValidationStatus;

        } catch (Exception e) {
            System.out.println("Exception Occurred while validating value between DB and JSON : " + e);
            return false;
        }
    }

    public static void writeFailureReport(HashMap<String, String> failureReport, String filePath, String validationTableName) {
        filePath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator + filePath;
        String[] arr = validationTableName.split("\\.");
        filePath = filePath + File.separator + arr[0] + File.separator + "FailureReport_" + getCurrentDateAndTime() + ".xlsx";
        System.out.println("Failure Report Path : " + failureReport);
        Workbook workbook = new XSSFWorkbook();
        // Create a sheet
        Sheet sheet = workbook.createSheet("FailureReport");
        Row row = sheet.createRow(0);
        row.createCell(0).setCellValue("Primary Key");
        row.createCell(1).setCellValue("Failure Description");
        int rowNum = 1;
        for (String primaryValue : failureReport.keySet()) {
            row = sheet.createRow(rowNum);
            row.createCell(0).setCellValue(primaryValue);
            row.createCell(1).setCellValue(failureReport.get(primaryValue));

        }
        // Save the Excel file
        try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
            workbook.write(fileOut);
            System.out.println("Failure Report generated successfully: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                workbook.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getCurrentDateAndTime() {
        // Define the format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy_HHmmss");
        // Get the current date and time
        // Print the formatted date and time
        return LocalDateTime.now().format(formatter);
    }

    public static void writeSuccessfulReport(String mappingFilePath, String reportPath,
                                             String validationTableName, List<HashMap<String, String>> successReport) {
        try {
            List<String> reportOrder = getPropertiesFileValueInListWithOrder(mappingFilePath);
            reportPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator + reportPath;
            String[] arr = validationTableName.split("\\.");
            reportPath = reportPath + File.separator + arr[0] + File.separator + "SuccessReport_" + getCurrentDateAndTime() + ".xlsx";
            System.out.println("Success Report Path : " + reportPath);
            Workbook workbook = new XSSFWorkbook();
            // Create a sheet
            Sheet sheet = workbook.createSheet("SuccessReport");
            Row row = sheet.createRow(0);
            //Set the header
            for (int i = 0; i < reportOrder.size(); i++) {
                Cell cell = row.createCell(i);
                cell.setCellValue(reportOrder.get(i).toUpperCase());
                CellStyle style = workbook.createCellStyle();
                Font font = workbook.createFont();
                font.setBold(true);
                style.setFont(font);
                cell.setCellStyle(style);
            }
            //Set the value of all the records
            for (int excelRow = 1; excelRow <= successReport.size(); excelRow++) {
                row = sheet.createRow(excelRow);
                for (int excelCell = 0; excelCell < reportOrder.size(); excelCell++) {
                    Cell cell = row.createCell(excelCell);
                    cell.setCellValue(successReport.get((excelRow - 1)).get(reportOrder.get(excelCell)));
                }
            }
            // Save the Excel file
            try (FileOutputStream fileOut = new FileOutputStream(reportPath)) {
                workbook.write(fileOut);
                System.out.println("Success Report generated successfully: " + reportPath);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    workbook.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println("Exception Occurred while generating success report : " + e);
        }

    }

    public static List<String> getPropertiesFileValueInListWithOrder(String mappingFilePath) {
        List<String> reportOrder = new ArrayList<>();
        try {
            LinkedHashMap<String, String> orderedProperties = new LinkedHashMap<>();
            // Read file manually
            BufferedReader reader = new BufferedReader(new FileReader(mappingFilePath));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("=")) { // Ignore empty lines
                    String[] parts = line.split("=", 2);
                    orderedProperties.put(parts[0].trim(), parts[1].trim());
                }
            }
            reader.close();
            // Printing keys in original order
            for (Map.Entry<String, String> entry : orderedProperties.entrySet()) {
                reportOrder.add(entry.getValue().toUpperCase());
            }
            return reportOrder;
        } catch (Exception e) {
            System.out.println("Exception occurred while fetching the order of report : " + e);
            return reportOrder;
        }
    }
}
