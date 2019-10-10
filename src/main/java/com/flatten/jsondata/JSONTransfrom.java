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
		SparkConf sparkConf = new SparkConf().setAppName("SPARK-JSON-Transformation");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		SparkSession spark = JavaSparkSessionSingleton.getInstance(sparkConf)
				.builder()
				.config(sparkConf)
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");


		List<String> jsonData = Arrays.asList("{\"name\":\"Akash\",\"age\":26,\"watches\":{\"name\":\"Apple\",\"models\":[\"Apple Watch Series 5\",\"Apple Watch Nike\"]},\"phones\":[{\"name\":\"Apple\",\"models\":[\"iphone X\",\"iphone XR\",\"iphone XS\",\"iphone 11\",\"iphone 11 Pro\"]},{\"name\":\"Samsung\",\"models\":[\"Galaxy Note10\",\"Galaxy S10e\",\"Galaxy S10\"]},{\"name\":\"Google\",\"models\":[\"Pixel 3\",\"Pixel 3a\"]}]}");
		Dataset<String> myTechDataset = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> accessoriesDataset = spark.read().json(myTechDataset);
		accessoriesDataset.show();

		Dataset flattened_ds = flattenJSONdf(accessoriesDataset);

		flattened_ds.show();

		spark.stop();
		spark.close();
	}

	private static Dataset flattenJSONdf(Dataset<Row> ds) {

		StructField[] fields = ds.schema().fields();

		List<String> fieldsNames = new ArrayList<>();
		for (StructField s : fields) {
			fieldsNames.add(s.name());
		}

		for (int i = 0; i < fields.length; i++) {

			StructField field = fields[i];
			DataType fieldType = field.dataType();
			String fieldName = field.name();

			if (fieldType instanceof ArrayType) {
				List<String> fieldNamesExcludingArray = new ArrayList<String>();
				for (String fieldName_index : fieldsNames) {
					if (!fieldName.equals(fieldName_index))
						fieldNamesExcludingArray.add(fieldName_index);
				}

				List<String> fieldNamesAndExplode = new ArrayList<>(fieldNamesExcludingArray);
				String s = String.format("explode_outer(%s) as %s", fieldName, fieldName);
				fieldNamesAndExplode.add(s);

				String[]  exFieldsWithArray = new String[fieldNamesAndExplode.size()];
				Dataset exploded_ds = ds.selectExpr(fieldNamesAndExplode.toArray(exFieldsWithArray));

				// explodedDf.show();

				return flattenJSONdf(exploded_ds);

			}
			else if (fieldType instanceof StructType) {

				String[] childFieldnames_struct = ((StructType) fieldType).fieldNames();

				List<String> childFieldnames = new ArrayList<>();
				for (String childName : childFieldnames_struct) {
					childFieldnames.add(fieldName + "." + childName);
				}

				List<String> newfieldNames = new ArrayList<>();
				for (String fieldName_index : fieldsNames) {
					if (!fieldName.equals(fieldName_index))
						newfieldNames.add(fieldName_index);
				}

				newfieldNames.addAll(childFieldnames);

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


