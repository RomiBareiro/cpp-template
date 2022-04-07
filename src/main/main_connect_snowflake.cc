#include <ctime>
#include <string>
#include <iostream>
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <cstring>

#include "glog/logging.h"
#include "glog/stl_logging.h"
using namespace std;
int testConection();
int getWebsiteID(SQLHDBC handler, SQLCHAR* szWebsiteID);

int main(int argc, char **argv) 
{
    return testConection();
}

int testConection()
 {

    LOG(INFO) << "Program had started.." << endl;

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;

    SQLRETURN ret;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    LOG(INFO) << "Attempting Connection " << "dbc :" << dbc << endl;

    string sqlConnectionString = "DSN=videoamp-vid;";

    ret = SQLDriverConnect(dbc, NULL, (SQLCHAR*)sqlConnectionString.c_str(), SQL_NTS,
                           NULL, 0, NULL,
                           SQL_DRIVER_COMPLETE) ; 
    if (SQL_SUCCEEDED(ret)) 
    {
        SQLCHAR dbms_name[256], dbms_ver[256], szWebsiteID[1024];
        SQLUINTEGER getdata_support = 0;
        SQLUSMALLINT max_concur_act = 0;

        memset(dbms_name, 0, sizeof(dbms_name));
        memset(dbms_ver, 0, sizeof(dbms_ver));
        memset(szWebsiteID, 0, sizeof(szWebsiteID));

        LOG(INFO)<< "Connected\n" << endl;
            
         /*  Find something out about the driver.*/

        SQLGetInfo(dbc, SQL_DBMS_NAME, (SQLPOINTER)dbms_name,
                   sizeof(dbms_name), NULL);
        SQLGetInfo(dbc, SQL_DBMS_VER, (SQLPOINTER)dbms_ver,
                   sizeof(dbms_ver), NULL);
        SQLGetInfo(dbc, SQL_GETDATA_EXTENSIONS, (SQLPOINTER)&getdata_support,
                   0, 0);
        SQLGetInfo(dbc, SQL_MAX_CONCURRENT_ACTIVITIES, &max_concur_act, 0, 0);

        LOG(INFO) << "DBMS Name: " << dbms_name << endl;
        LOG(INFO) << "DBMS Version: " <<  dbms_ver << endl;
        if (max_concur_act == 0) 
            LOG(ERROR) << "SQL_MAX_CONCURRENT_ACTIVITIES - no limit or undefined" << endl;
        else 
            LOG(INFO) << "SQL_MAX_CONCURRENT_ACTIVITIES" << max_concur_act << endl;
        if (getdata_support & SQL_GD_ANY_ORDER)
            LOG(INFO) << "SQLGetData - columns can be retrieved in any order" << endl;
        else
            LOG(INFO) << "SQLGetData - columns must be retrieved in order" << endl;
        if (getdata_support & SQL_GD_ANY_COLUMN)
            LOG(INFO) << "SQLGetData - can retrieve columns before last bound one" << endl;
        else
            LOG(INFO) << "SQLGetData - columns must be retrieved after last bound one" << endl;
        
        getWebsiteID(dbc, szWebsiteID);    
        SQLDisconnect(dbc);               /* disconnect from driver */
    } 
    else 
    {
        LOG(ERROR) << "Failed to connect"<< endl;
    }
    /* free up allocated handles */
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    return 0;
}

/**
 * @brief Get the  WEB_SITE_ID data from SNOWFLAKE_SAMPLE_DATA
 * 
 * @param handler : ODBC Handler
 * @param szWebsiteID : Output WEB_SITE_ID first row data from WEB_SITE table
 * @return int : Query result
 */
int getWebsiteID(SQLHDBC handler, SQLCHAR* szWebsiteID)
{
    SQLHSTMT  hstmt = SQL_NULL_HSTMT;  
    int result = 0 ;

    string statement = "SELECT * FROM \"SNOWFLAKE_SAMPLE_DATA\".\"TPCDS_SF100TCL\".\"WEB_SITE\"";
    SQLAllocHandle(SQL_HANDLE_STMT, handler, &hstmt);

    result = SQLExecDirect(hstmt,(UCHAR*)statement.c_str(), SQL_NTS);

    if (result == SQL_ERROR) 
        LOG(ERROR) << "Operation failed: " << hstmt << endl;
    
    else if (result != SQL_SUCCESS && result != SQL_SUCCESS_WITH_INFO && result != SQL_NO_DATA_FOUND) 
        LOG(ERROR) << "Operation returned unknown code " << result << endl;
    else
    { 
        result = SQLFetch(hstmt);
        if(!SQL_SUCCEEDED(result))
        { 
            LOG(ERROR) << "Couldnt fetch results"<< endl;
            return result;
        }
        
        SQLLEN  sWebID = 0;  
        //WEB_SITE_ID =  2
        result = SQLGetData(hstmt, 2, SQL_C_CHAR, szWebsiteID, sizeof(szWebsiteID), &sWebID);
   
        if (!SQL_SUCCEEDED(result)) 
        { 
            LOG(ERROR) << "Couldnt get data" << endl;
            return result; 
        }

        /* Print the row of data */  
        LOG(INFO) << szWebsiteID;
    }
    
    SQLFreeHandle(SQL_HANDLE_ENV, hstmt);
    return result;
         
}