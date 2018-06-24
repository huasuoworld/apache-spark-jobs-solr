package org.huasuoworld.app;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class HdfsToSolr implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7989240394871872541L;

	public static void main(String[] args) {
		System.out.println("job start!");
		if (args == null || args.length < 2) {
			System.out.println(
					"args[0] is a hello message, args[1] is a file name(like /data/spark/jobs/zhazhahui.txt).");
			return;
		}
		System.out.println(args[0]);
		System.out.println(args[1]);
		String logFile = args[1]; // Should be some file on your system
		SparkSession spark = null;
		String urlString = "http://192.168.1.5:8983/solr/zhazhahui_core";
		try {
			SolrClient solrclient  = new HttpSolrClient.Builder(urlString).build(); 
			// Create an SparkSession
			spark = SparkSession.builder().appName("Simple Application").getOrCreate();
			// Create an RDD
			//根据文件大小分区 minPartitions = 1
			JavaRDD<String> zhazhahuiRDD = spark.sparkContext().textFile(logFile, 1)
					.toJavaRDD();
			// The schema is encoded in a string
			String schemaString = "name,value,row_number";

			// Generate the schema based on the string of schema
			List<StructField> fields = new ArrayList<StructField>();
			for (String fieldName : schemaString.split(",")) {
				StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
				fields.add(field);
			}
			StructType schema = DataTypes.createStructType(fields);
			
			// Convert records of the RDD (people) to Rows
			JavaRDD<Row> rowRDD = zhazhahuiRDD.map(new Function<String, Row>() {
				private static final long serialVersionUID = 1L;
				int i = 0;
				public Row call(String record) throws Exception {
					System.out.println("record>>" + record);
					String[] attributes = record.split(",");
					Row row = RowFactory.create(attributes[0], attributes[1], i);
					i++;
					return row;
				}
			}
			// JDK8
			// (Function<String, Row>) record -> {
			// String[] attributes = record.split(",");
			// return RowFactory.create(attributes[0], attributes[1].trim());
			// }
			//
			);

			// Apply the schema to the RDD
			Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

			// Creates a temporary view using the DataFrame
			peopleDataFrame.createOrReplaceTempView("zhazhahui");

			// SQL can be run over a temporary view created using DataFrames
			Dataset<Row> results = spark.sql("SELECT name,value FROM zhazhahui");
			// The results of SQL queries are DataFrames and support all the normal RDD
			// operations
			// The columns of a row in the result can be accessed by field index or by field
			// name
			Dataset<Tuple2<String, String>> logData = results.map(new MapFunction<Row, Tuple2<String, String>>() {
				private static final long serialVersionUID = 1L;
				public Tuple2<String, String> call(Row row) throws Exception {
//					Tuple2<String, String> tuple = new Tuple2<String, String>();
					String[] str = new String[2];
					str[0] = "Name_" + row.getString(0).trim();
					str[1] = "value_" + row.getString(1).trim();
					return new Tuple2<String, String>(str[0], str[1]);
				}
			}, Encoders.tuple(Encoders.STRING(), Encoders.STRING())
			// JDK8
			// (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
			// Encoders.STRING()
			);
			logData.show();
			int batchSize = 500;
			Object numBs = logData.take(batchSize);
			
			scala.collection.Iterator<Tuple2<String, String>> logDataIt = logData.rdd().toLocalIterator();
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			try {
				while(numBs != null) {
					System.out.println(numBs.getClass().getSimpleName());
					if(numBs != null && numBs instanceof Tuple2[]) {
						
						Tuple2<String, String>[] tuple2List = (Tuple2<String, String>[]) numBs;
						for(Tuple2<String, String> tuple2: tuple2List) {
							
							SolrInputDocument doc = new SolrInputDocument();
							String id = UUID.randomUUID().toString().replaceAll("-", "");
							doc.addField("id", id);
							doc.addField("Name", tuple2._1);
							doc.addField("value", tuple2._2);
							doc.addField("description", "新增文档" + id);
							docs.add(doc);
							System.out.println(id + ">>" + tuple2._1+">>" + tuple2._2);
						}
					}
					
					System.out.println("Add doc size" + docs.size());
	            	UpdateResponse rsp = solrclient.add(docs);
	            	System.out.println("commit doc to index" + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());
	            	solrclient.commit();
	            	docs.clear();
					
					numBs = logData.take(batchSize);
					System.out.println("while>>" + numBs);
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				System.out.println("finally>>");
				if(solrclient != null) {
					solrclient.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (spark != null) {
				spark.stop();
				System.out.println("spark destroied!");
			}
		}
		System.out.println("job successful!");
	}
}
