package com.flatten.jsondata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JSONTransfrom {
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("SPARK-JSON-Transform");


		System.out.println("!! Welcome to SPARK APPLICATION !!");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		SparkSession spark = JavaSparkSessionSingleton.getInstance(sparkConf)
				.builder()
				.config(sparkConf)
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");


		List<String> jsonData = Arrays.asList("{\"name\":\"John\",\"age\":30,\"bike\":{\"name\":\"Bajaj\",\"models\":[\"Dominor\",\"Pulsar\"]},\"cars\":[{\"name\":\"Ford\",\"models\":[\"Fiesta\",\"Focus\",\"Mustang\"]},{\"name\":\"BMW\",\"models\":[\"320\",\"X3\",\"X5\"]},{\"name\":\"Fiat\",\"models\":[\"500\",\"Panda\"]}]}\n");
		Dataset<String> bikerPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> bikerDataset = spark.read().json(bikerPeopleDataset);
		bikerDataset.show();

		Dataset flat_ds = flattenJSONdf(bikerDataset);

		flat_ds.show();

		spark.stop();
		spark.close();
	}

	private static Dataset flattenJSONdf(Dataset<Row> ds) {

		StructField[] fields = ds.schema().fields();

		//Before Java8
		List<String> fieldsNames = new ArrayList<>();
		for (StructField s : fields) {
			fieldsNames.add(s.name());
		}

		for (int i = 0; i < fields.length; i++) {
			StructField field = fields[i];

			System.out.println("fieldStructField :::::::" + field);

			DataType fieldType = field.dataType();

			System.out.println("fieldType::::::" + fieldType);

			String fieldName = field.name();

			System.out.println("fieldName::::::" + fieldName);

			if (fieldType instanceof ArrayType) {
				System.out.println("Array Column");

				List<String> fieldNamesExcludingArray = new ArrayList<String>();
				for (String fieldName_index : fieldsNames) {
					if (!fieldName.equals(fieldName_index))
						fieldNamesExcludingArray.add(fieldName_index);
				}

				List<String> fieldNamesAndExplode = new ArrayList<>(fieldNamesExcludingArray);
				String s = String.format("explode_outer(%s) as %s", fieldName, fieldName);
				fieldNamesAndExplode.add(s);

				System.out.println("All Array column ready >>>>");
				System.out.println(Arrays.toString(fieldNamesAndExplode.toArray()));


				String[]  ex = new String[fieldNamesAndExplode.size()];
				Dataset explodedDf = ds.selectExpr(fieldNamesAndExplode.toArray(ex));

				System.out.println(" Please view Explode at array level");
				explodedDf.show();

				return flattenJSONdf(explodedDf);

			} else if (fieldType instanceof StructType) {

				System.out.println("Struct Column");

				String[] childFieldnames_struct = ((StructType) fieldType).fieldNames();

				List<String> childFieldnames = new ArrayList<>();
				for (String childName : childFieldnames_struct) {
					childFieldnames.add(fieldName + "." + childName);
				}

				System.out.println(Arrays.toString(childFieldnames.toArray()));

				List<String> newfieldNames = new ArrayList<>();
				for (String fieldName_index : fieldsNames) {
					if (!fieldName.equals(fieldName_index))
						newfieldNames.add(fieldName_index);
				}

				newfieldNames.addAll(childFieldnames);
				System.out.println("All struct column ready >>>>");
				System.out.println(Arrays.toString(newfieldNames.toArray()));

				List<Column> renamedStrutctCols = new ArrayList<>();

				for(String newFieldNames_index : newfieldNames){
					renamedStrutctCols.add( new Column(newFieldNames_index.toString()).as(newFieldNames_index.toString().replace(".", "_")));
				}

				Seq renamedStructCols_seq = JavaConverters.collectionAsScalaIterableConverter(renamedStrutctCols).asScala().toSeq();

				Dataset ds_struct = ds.select(renamedStructCols_seq);

				return flattenJSONdf(ds_struct);
			}
			else{

			}

		}
		return ds;
	}
}


class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession
					.builder()
					.config(sparkConf)
					.getOrCreate();
		}
		return instance;
	}
}


