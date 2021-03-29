package org.airtel.ug;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import org.apache.log4j.Logger;

/**
 *
 * @author Benjamin E Ndugga
 */
public class CDRWriter {

    private static final Logger LOGGER = Logger.getLogger(CDRWriter.class.getName());

    private static final int MAX_ROW_SIZE = 1000;
    private static final String CDR_FILE_LOC = "cdrs/";
    private static final int PRIVATE_APN_FTP = 13;

    public static void main(String[] args) {
        Connection conn = null;
        BufferedWriter bw = null;
        //PreparedStatement updateMasterStatement = null;
        try {

            LOGGER.info("starting CDR write at " + new Date());

            //get a connection to the database
            conn = DBConnector.connect();

            //String partitionName = getCurrentPartitionName();
            /**
             * set up the query that will include the data provider property of
             * charge flag and Generate CDRs for those whose charge flag is 0
             * and the mode of payment is airtime.
             */
            String query = "SELECT A.ID,"
                    + "A.MSISDN,"
                    + "A.SESSIONID,"
                    + "TO_CHAR(A.TIME_ID,'YYYY-MM-DD'),"
                    + "TO_CHAR(A.DATETIME,'HH24:MI:SS'),"
                    + "A.PRICE,"
                    + "A.BUNDLE,"
                    + "A.IMSI,"
                    + "A.FTP,"
                    + "B.CHARGE_FLAG "
                    + "FROM  AIRTEL_SUBSCRIPTIONS_PP A  "
                    + "INNER JOIN "
                    + "PCRF_DATA_PROVIDERS B "
                    + "ON A.FTP = B.FTP_ID  "
                    + "WHERE "
                    + "A.FTP2 =0  AND A.SUBSCRIBER_TYPE = 0 "
                    + "AND A.PRICE !=0 "
                    + "AND A.BUNDLE !='SOCIAL_MEDIA_TAX' "
                    + "AND A.RETN=0 AND A.MOBIQUITY_CODE IS NULL";

            //prepare the select statement
            PreparedStatement selectPrepareStatement = conn.prepareStatement(query);

            String updateQuery = "UPDATE AIRTEL_SUBSCRIPTIONS_PP SET FTP2 = 1 WHERE  FTP2 =0 AND ID = ?";

            //prepare the update statement
            PreparedStatement updatePrepareStatement = conn.prepareStatement(updateQuery);
            conn.setAutoCommit(true);

            //execute statement to the database
            ResultSet resultSet = selectPrepareStatement.executeQuery();
            resultSet.setFetchSize(MAX_ROW_SIZE);

            //get CDR FileName
            String currentCDRFileName = getCurrentCDRFileName();

            //initialise the FileWrite Object 
            bw = new BufferedWriter(new FileWriter(CDR_FILE_LOC + currentCDRFileName), 1 * 1024);
            //loop through the result set and generate the CDRs under the CDR_FILE_LOC
            while (resultSet.next()) {
                Formatter formatter = new Formatter();

                int id = resultSet.getInt(1);
                String msisdn = resultSet.getString(2);
                String sessionid = resultSet.getString(3);
                String date = resultSet.getString(4);
                String time = resultSet.getString(5);
                int price = resultSet.getInt(6);
                String bundle = resultSet.getString(7);
                String imsi = resultSet.getString(8);
                int ftp = resultSet.getInt(9);
                int chargeFlag = resultSet.getInt(10);

                //create file under the location named checking the ftp number and the charge flag
                if (chargeFlag == 1) {
                    //if the charge flag is not 0 then generate the cdr line by checking for PRIVATE APNS and GENERAL INTERNET APNS subscribers based on the ftp 
                    if (ftp == PRIVATE_APN_FTP) {
                        bw.append("05," + id + "," + msisdn + "," + imsi + ",2," + date + "," + time + ",CT.UG.CELTEL.COM,SUCCESS,0," + formatter.format("%.1f", price / 1.18).toString() + ",,0");
                    } else {
                        //bw.append("05," + id + "," + msisdn + "," + imsi + ",2," + date + "," + time + ",web.ug.zain.com,SUCCESS,0," + formatter.format("%.1f", price / 1.3).toString() + ",,0");
                        bw.append("05," + id + "," + msisdn + "," + imsi + ",2," + date + "," + time + ",web.ug.zain.com,SUCCESS,0," + formatter.format("%.1f", price / 1.18).toString() + ",,0");
                    }
                }

                //append a new line
                bw.newLine();
                LOGGER.info("writen off transaction {" + id + "," + sessionid + "," + bundle + "," + msisdn + "}");

                //flush the buffered write
                bw.flush();

                updatePrepareStatement.setInt(1, id);
                updatePrepareStatement.addBatch();
            }

            //close the buffered writter
            bw.close();

            LOGGER.info("updating transactions found on DB...");

            //update the database transaction as a finished CDR write off
            int[] executeBatch = updatePrepareStatement.executeBatch();

            LOGGER.info("UPDATED_RESP_COUNT " + executeBatch.length);
            LOGGER.info("COMMITING TRANSACTIONS");
            conn.commit();

            /**
             *
             *********************************************************
             *
             * THIS SECTION WILL RUN A CDR WRITE FOR HYBRID CUSTOMERS
             *
             * *******************************************************
             */
//            query = "SELECT ID,MSISDN,"
//                    + "TO_CHAR(DATECREATED,'YYYY-MM-DD'),"
//                    + "TO_CHAR(DATECREATED,'HH24:MI:SS'),"
//                    + "(DATACOST+RENTALFEE+USAGE_AC),"
//                    + "IMSI FROM HYBRID_SUBSCRIPTIONS "
//                    + "WHERE PCRF_RESP_CODE = '0'  AND SUBTYPE = 1 AND FTP = 0 AND MASTER_MSISDN IS NULL";
//
//            updateQuery = "UPDATE HYBRID_SUBSCRIPTIONS SET FTP = 1 WHERE ID = ?";
//
//            updatePrepareStatement = conn.prepareStatement(updateQuery);
//
//            selectPrepareStatement = conn.prepareStatement(query);
//
//            resultSet = selectPrepareStatement.executeQuery();
//            resultSet.setFetchSize(MAX_ROW_SIZE);
//
//            //get CDR FileName
//            currentCDRFileName = getCurrentCDRFileNameForHYB();
//
//            //initialise the FileWrite Object 
//            bw = new BufferedWriter(new FileWriter(CDR_FILE_LOC + currentCDRFileName), 1 * 1024);
//            //loop through the result set and generate the CDRs under the CDR_FILE_LOC
//
//            while (resultSet.next()) {
//                Formatter formatter = new Formatter();
//
//                int id = resultSet.getInt(1);
//                String msisdn = resultSet.getString(2);
//                String date = resultSet.getString(3);
//                String time = resultSet.getString(4);
//                int price = resultSet.getInt(5);
//                String imsi = resultSet.getString(6);
//
//                bw.append("05," + id + "," + msisdn + "," + imsi + ",2," + date + "," + time + ",Corporate Hybrid,SUCCESS,0," + formatter.format("%.1f", price / 1.3).toString() + ",,0");
//
//                //append a new line
//                bw.newLine();
//                logger.log(Level.INFO, "WRITTEN OFF TRANSACTION {0} {1} ", new Object[]{id, msisdn});
//
//                updatePrepareStatement.setInt(1, id);
//                int i = updatePrepareStatement.executeUpdate();
//                logger.log(Level.INFO, "UPDATED_SELF ID RESP | {0} ", new Object[]{i});
//                conn.commit();
//            }
//
//            /**
//             ***********************
//             * MASTER MSISDN QUERY
//             *
//             *********************
//             */
//            query = "SELECT ID,MASTER_MSISDN,TO_CHAR(DATECREATED,'YYYY-MM-DD'),TO_CHAR(DATECREATED,'HH24:MI:SS'),(DATACOST+RENTALFEE+USAGE_AC),IMSI,MSISDN "
//                    + "FROM HYBRID_SUBSCRIPTIONS WHERE PCRF_RESP_CODE = '0' AND SUBTYPE = 1 AND FTP =0  AND MASTER_MSISDN IS NOT NULL";
//
//            selectPrepareStatement = conn.prepareStatement(query);
//
//            updateMasterStatement = conn.prepareStatement(updateQuery);
//
//            resultSet = selectPrepareStatement.executeQuery();
//            resultSet.setFetchSize(MAX_ROW_SIZE);
//
//            while (resultSet.next()) {
//                Formatter formatter = new Formatter();
//
//                int id = resultSet.getInt(1);
//                String master_msisdn = resultSet.getString(2);
//                String date = resultSet.getString(3);
//                String time = resultSet.getString(4);
//                int price = resultSet.getInt(5);
//                String imsi = resultSet.getString(6);
//                String msisdn = resultSet.getString(7);
//
//                bw.append("05," + id + "," + master_msisdn + "," + imsi + ",2," + date + "," + time + "," + msisdn + "_Corporate Hybrid,SUCCESS,0," + formatter.format("%.1f", price / 1.3).toString() + ",,0");
//
//                //append a new line
//                bw.newLine();
//                logger.log(Level.INFO, "WRITTEN OFF TRANSACTION {0} {1} ", new Object[]{id, msisdn});
//
//                updateMasterStatement.setInt(1, id);
//                int i = updateMasterStatement.executeUpdate();
//                logger.log(Level.INFO, "UPDATED_MASTER ID RESP | {0} ", new Object[]{i});
//                conn.commit();
//            }
        } catch (SQLException | IOException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
        } finally {
            if (bw != null) {
                LOGGER.info("flusing output stream...");
                bw = null;
            }
            if (conn != null) {
                try {
                    LOGGER.info("closing database connection...");
                    conn.close();
                } catch (SQLException ex) {
                    LOGGER.error(ex.getLocalizedMessage(), ex);
                }
            }
        }
    }

    private static String getCurrentCDRFileName() {

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");

        String fileName = "DAT_" + df.format(new Date()) + ".cdr";

        LOGGER.info("filename created: " + fileName);
        return fileName;
    }

    /**
     *
     * @return the current file name to be written to
     */
    private static String getCurrentCDRFileNameForHYB() {

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHm");

        String fileName = "HYB_" + df.format(new Date()) + ".cdr";

        LOGGER.info("FILE NAME CREATED {" + fileName);
        return fileName;
    }

    private static String getCurrentPartitionName() {

        //AIRTEL_SUBS_201601
        SimpleDateFormat dateFormatPart = new SimpleDateFormat("yyyyMM");

        String partitionName = "AIRTEL_SUBS_" + dateFormatPart.format(new java.util.Date());

        LOGGER.info("PARTITION NAME " + partitionName);
        return partitionName;
    }

}//end of class
