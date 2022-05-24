package com.serverless;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler implements RequestHandler<S3Event, Void> {

	private static final Logger LOG = LogManager.getLogger(Handler.class);
	private static final String sourceBucketName="bucketName";
	private static final String sourceBucketPath="bucketPath";
	private static final String obj="obj";
	Connection c;
	@Override
	public Void handleRequest(S3Event s3Event, Context context) {
		c=getConnection();
		String csvData=getCsvData();
		System.out.println("Successfully read S3 object to local temp file");

		return null;
	}

	private String getCsvData() {
		StringBuffer content = new StringBuffer("");
		AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		S3Object oo = s3Client.getObject(sourceBucketName, sourceBucketPath+"/"+obj+".csv");
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(oo.getObjectContent()));
			String s = null;
			while ((s = reader.readLine()) != null) {
				String[] data= s.split(",");
				insertData(data);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content.toString();
	}

	private void insertData(String[] data) {
		try {
			String qry="insert into table_name (column1,column2,column3)values(?,?,?)";
			Statement stmt=c.createStatement();
			stmt.executeUpdate(qry,data);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	Connection getConnection(){
		String dbURL = "jdbc:oracle:thin:@localhost:1521:productDB";
		String username = "tiger";
		String password = "scott";
		try {
			Connection conn = DriverManager.getConnection(dbURL, username, password);
		return conn;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

}
