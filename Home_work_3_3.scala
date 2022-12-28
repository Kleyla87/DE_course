package org.example
package com.sparkbyexamples.spark.dataframe.functions.datetime
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Home_work_3_3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .master("local[1]")
                            .appName("SparkByExamples.com")
                            .getOrCreate()
    import spark.implicits._

    //создаем фрейм данных
    val data = Seq(
      Row(12345, "1667627426", "click", 101, "Sport", false),
      Row(12671, "1667627976", "scroll", 123, "Politics", true),
      Row(18171, "1667629999", "visit", 111, "Sport", true),
      Row(18171, "1667631795", "move", 133, "Sport", true),
      Row(18171, "1667632782", "move", 114, "Games", true),
      Row(18171, "1667632783", "scroll", 114, "Games", true),
      Row(12345, "1667985906", "move", 143, "Medicine", false)
    )

    val schema = new StructType()
      .add("id", IntegerType)
      .add("timestamp", StringType)
      .add("type", StringType)
      .add("page_id", IntegerType)
      .add("tag", StringType)
      .add("sign", BooleanType)

    var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    //df.show()

    //топ-5 самых активных посетителей сайта

    df.groupBy("id")
      .agg(
        count("type").as("events")
      )
      .sort(col("events").desc)
      .limit(5)
      .show(false)

    //процент посетителей, у которых есть ЛК
    val SignUsers = df.where("sign == true" )
      .select(countDistinct("id").as("signId"))
      .first()
      .getLong(0)
    val AllUsers = df.select(countDistinct("id").as("allId"))
                      .first()
                      .getLong(0)
    val Percent = SignUsers * 100 / AllUsers
    println(s"Процент посетителей, у которых есть ЛК: $Percent")

    //топ-5 страниц сайта по показателю общего кол-ва кликов на данной странице
    df.where("type == 'click'")
      .groupBy("page_id")
      .agg(
        count("id").as("clicks")
      )
      .sort(col("clicks").desc)
      .limit(5)
      .show(false)

    //временной промежуток, в течение которого было больше всего активностей на сайте
    df = df.withColumn("time", from_unixtime(col("timestamp")))
    df = df.withColumn("interval", floor(hour(col("time")) / 4).cast(StringType))
    df = df.withColumn("window", regexp_replace(col("interval"), "0", "0-4"))
    df = df.withColumn("window", regexp_replace(col("window"), "1", "4-8"))
    df = df.withColumn("window", regexp_replace(col("window"), "2", "8-12"))
    df = df.withColumn("window", regexp_replace(col("window"), "3", "12-16"))
    df = df.withColumn("window", regexp_replace(col("window"), "4", "16-20"))
    df = df.withColumn("window", regexp_replace(col("window"), "5", "20-24"))

    df.groupBy("window")
      .agg(
        count("type").as("events")
      )
      .sort(col("events").desc)
      .limit(1)
      .show(false)

    //создаем второй фрейм данных
    val data2 = Seq(
      Row(1111, 12671, Row("Василий", "Иванович", "Петров"), "13.08.1980", "11.12.2020"),
      Row(2222, 18171, Row("Петр", "Васильевич", "Иванов"), "22.01.1960", "15.07.2022")
    )
    val schema2 = new StructType()
      .add("id_lk", IntegerType)
      .add("user_id", IntegerType)
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("birthday", StringType)
      .add("day_sign", StringType)

    var df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)
    df2.show(false)

    //фамилии посетителей, которые читали хотя бы одну новость про спорт

    val joindf = df.where("tag == 'Sport'")
      .join(df2, df("id") === df2("user_id"), "left")

    joindf.select(joindf("name.lastname"))
          .distinct()
          .filter("name is not null")
          .show(false)
  }

}
