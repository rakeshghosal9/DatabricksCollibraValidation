package org.databricks;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Properties;

public class DatabricksCollibraValidation {

    //Enter the properties file name under resources/config folder for which you want to perform the validation
    static String validationTableName = "ASSET.properties";
    static int totalRecordsWishToValidate = 2000;

    public static void main(String args[]) {
        //Read the global properties
        Properties globalProp = ReusableCommonMethods.readGlobalProperty();
        //If the global properties are not read, fail
        if (globalProp == null) {
            System.exit(0);
        }
        Properties tableProp = ReusableCommonMethods.readTableProperty(validationTableName);
        //If the table properties are not read, fail
        if (tableProp == null) {
            System.exit(0);
        }
        //Read the SQL file using value from table properties file
        String sqlQuery = ReusableCommonMethods.readSQLQuery(tableProp.getProperty("sql_query"));

        //Establish DB connection and perform the SQL Query
        ResultSet resultSet = ReusableCommonMethods.getDBConnection(globalProp, sqlQuery);

        if (resultSet == null) {
            System.out.println("DB Connection is not successful");
            System.exit(0);
        }

        //Get the entire DB data in a HashMap
        HashMap<String, HashMap<String, String>> dataFromDatabricks =
                ReusableCommonMethods.getDataFromDB(resultSet, tableProp.getProperty("primary_key"));

        //Get the access token
        String accessToken = ReusableCommonMethods.getRenewedAccessToken(globalProp);

        //Perform the comparison
        boolean overallValidation =
                ReusableCommonMethods.performComparisonBetweenDBAndAPI
                        (globalProp, tableProp, accessToken, totalRecordsWishToValidate, dataFromDatabricks);
        if (!overallValidation) {
            System.out.println("Overall validation failed, please check logs for more details");
            System.exit(0);
        }


    }
}
