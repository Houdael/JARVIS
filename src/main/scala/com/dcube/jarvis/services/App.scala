package com.dcube.jarvis.services

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.dcube.jarvis.models.JarvisStructTypes
import io.delta.tables.DeltaTable
import org.apache.spark
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, functions}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

object App extends SparkService{
/*
  val RefTable: StructType = StructType (List(
    StructField("Id", StringType, nullable = false)
    ,StructField("Bank", StringType, nullable = false)
    ,StructField("Iban", StringType, nullable = false)
    ,StructField("Bic", StringType, nullable = false)
    ,StructField("Prefered", BooleanType, nullable = false)
  ))
*/


  /**
   * Sets up the Data Lake Connection
   */

  var accountName = "dcubestadatalakedev"
  var tenantId = "84e25fff-8a01-49fa-9024-ac157f75279d"
  var datalakeRootDir = "abfss://datalake@" + accountName + ".dfs.core.windows.net/"

  def setUpADLSConnection(): Unit = {

    spark.conf.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type." + accountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id." + accountName + ".dfs.core.windows.net", dbutils.secrets.get(scope="jarvis-secret-scope",key="sp-dbk-appid"))
    spark.conf.set("fs.azure.account.oauth2.client.secret." + accountName + ".dfs.core.windows.net", dbutils.secrets.get(scope="jarvis-secret-scope",key="sp-dbk-secret"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint." + accountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantId + "/oauth2/token")
  }

  def getAdlsPath(mainFolder: String): String = s"abfss://datalake@dcubestadatalake.dfs.core.windows.net/$mainFolder/jarvis/gestion"

  def getFile(mainFolder : String, fileFolder: String, fileName: String): String = s"${getAdlsPath(mainFolder)}/$fileFolder/${fileName}"

  def getFolder(mainFolder : String, fileFolder: String): String = s"${getAdlsPath(mainFolder)}/$fileFolder"

  private[this] def getFolderSchema(folderName: String): (StructType, String, Array[String]) =  folderName match {

    case "SALCUST" => (JarvisStructTypes.SalesCustomer, "Sales.Customer", Array("Id"))
    case "SALINTER" => (JarvisStructTypes.SalesIntervention, "Sales.Intervention", Array("Id"))
    case "SALEAD" => (JarvisStructTypes.SalesLead, "Sales.Lead", Array("Id"))
    case "SALPER" => (JarvisStructTypes.SalesPeriod, "Sales.Period", Array("Id"))
    case "SALPHAS" => (JarvisStructTypes.SalesPhasis, "Sales.Phasis", Array("Id"))
    case "SALPROJ" => (JarvisStructTypes.SalesProject, "Sales.Project", Array("Id"))
    case "SALPROJKD" => (JarvisStructTypes.SalesProjectKind, "Sales.ProjectKind", Array("Id"))
    case "SALUSR" => (JarvisStructTypes.SalesUser, "Sales.User", Array("Id"))
    case "TIMINP" => (JarvisStructTypes.TimesheetInput, "Timesheet.Input", Array("Id"))
    case "TIMINPRW" => (JarvisStructTypes.TimesheetInputRow, "Timesheet.InputRow", Array("Id"))
  }
  def loadRawCsvData(mainFolder: String, fileFolder: String, fileName : String, schema: StructType, hasHeader: Boolean): DataFrame = {
    val inputPath = getFile(mainFolder, fileFolder, fileName)
    println(inputPath)
    val rawDataDF = spark.read.option("sep", ",").option("header", s"$hasHeader").option("inferSchema", "true").option("timestampFormat", "HH:MM:SS").schema(schema).csv(inputPath)
    rawDataDF
  }

  private[this] def getDeltaLakeTable(mainFolder: String, folderName: String, tableName: String): DeltaTable =
    DeltaTable.forPath(s"${getAdlsPath(mainFolder)}/$folderName/$tableName")

  /**
   * Initialize an empty Delta Lake Table in ADLS
   * @param deltaTableName Name of the table to init
   * @param schema Schema of the data to store
   * @return Initialized Delta Table object
   */
  private[this] def initDeltaLakeTable(mainFolder:String, folderName: String, deltaTableName: String, schema: StructType): DeltaTable = {
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df.write.format("delta").mode("append").save(s"${getAdlsPath(mainFolder)}/$folderName/$deltaTableName")
    getDeltaLakeTable(mainFolder, folderName, deltaTableName)
  }

  def getTempTableName(fileName: String): String = fileName + "_to_upsert"

  def getOrCreateDeltaTable(mainFolder:String, folderName :String, deltaTableName: String, schema: StructType): DeltaTable = {
    try {
      getDeltaLakeTable(mainFolder, folderName, deltaTableName)
    } catch {
      case e: AnalysisException => initDeltaLakeTable(mainFolder, folderName, deltaTableName, schema)
    }
  }

  private[this] val deltaLakeTableAlias = "deltaLakeTable"
  private[this] val upsertTableAlias = "upsertTable"

  def upsertDeltaLakeData(fileFolder : String, rawData: DataFrame, deltaTable: DeltaTable, keyFields : Array[String]): Unit =
  {
    val condition =
      keyFields
        .map((field: String) => {
          s"$deltaLakeTableAlias.$field = $upsertTableAlias.$field"
        })
        .mkString(" AND ")


    val fieldsHashMap = rawData.schema.fields.map(field => {
      val fieldName = field.name
      fieldName -> functions.col(s"$upsertTableAlias.$fieldName")
    }).toMap

    deltaTable.as(deltaLakeTableAlias)
      .merge(
        rawData.as(upsertTableAlias),
        condition
      )
      .whenMatched
      .update(
        fieldsHashMap
      )
      .whenNotMatched
      .insert(
        fieldsHashMap
      )
      .execute()

    deltaTable.toDF.
      coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("sep", ",").
      option("timestampFormat", "HH:MM:SS").
      mode("append")
      //save(s"${getAdlsPath("silver")}/$fileFolder/")
  }

  def main(args: Array[String]): Unit = {
    setUpADLSConnection()
    val logger =  LoggerFactory.getLogger(getClass.getSimpleName)
    println(logger)
    // make decision based on the content of the message
    // if empty file name = *
    // else filename = filename

    val mainFolderSource = "bronze"
   // val folderSource = "SALCUST"
    //val fileNameSource = "SalesCustomer_20200520_091841"
    //val (schemaSource, folderNameSource,_) = getFolderSchema(folderSource)

    val mainFolderTarget = "silver"
    //val folderTarget = "SALCUST"
    //val (schemaTarget, folderNameTarget, keyFields) = getFolderSchema(folderTarget)
    /*
    val message =
      """
        |{
        |"entities":[
        |{"entity":"BankingAccount",
        |"file":"BankingAccount_20200520_091023"}
        |]
        |}
        |""".stripMargin
        */


    val message =
      """
        |{

        |}
        |""".stripMargin



    val jsonParse = Json.parse(message)
    val entities = jsonParse \\ "entity"
    val files = jsonParse \\ "file"

    val folders = Seq("SALCUST", "SALINTER", "SALEAD", "SALPER", "SALPROJ", "SALPROJKD", "SALPHAS", "SALUSR","TIMINP", "TIMINPRW")

    if (entities.isEmpty){

      for(folder <- folders){
        val fileName = "*.csv"
        val (schema, folderName, keyFields) = getFolderSchema(folder)
        val rawData = loadRawCsvData(mainFolderSource, folderName, fileName, schema, hasHeader = true)
        //        val rawDataNoDup = rawData.orderBy(desc("DateCreation"), desc("HeureCreation")).dropDuplicates(keyFields)

        rawData.createOrReplaceTempView(getTempTableName("all_element"))

        val deltaTable = getOrCreateDeltaTable(mainFolderTarget, folderName, folder, schema)

        upsertDeltaLakeData(folder, rawData, deltaTable, keyFields)
        logger.info(s"file : $fileName is merged")

      }
    }else{
      for(indexEntity <- entities.indices){
        val folder = entities(indexEntity).as[String]
        val fileName = files(indexEntity).as[String]
        println(folder)
        println(fileName)

        val (schema, folderName, keyFields) = getFolderSchema(folder)

        val rawData = loadRawCsvData(mainFolderSource, folderName, fileName, schema, hasHeader = true)
       // val rawDataNoDup = rawData.orderBy(desc("DateCreation"), desc("HeureCreation")).dropDuplicates(keyFields)
        rawData.createOrReplaceTempView(getTempTableName(fileName.split('.')(0)))

        val deltaTable = getOrCreateDeltaTable(mainFolderTarget, folderName, folder, schema)

        upsertDeltaLakeData(folder, rawData, deltaTable, keyFields)
        logger.info(s"file : $fileName is merged")

      }

    }
/*
    val rawData = loadRawCsvData(mainFolderSource, folderNameSource, fileNameSource+".csv", schemaSource, hasHeader = true)

    rawData.createOrReplaceTempView(getTempTableName(fileNameSource))
    rawData.printSchema()

    val foldertest = s"SalesCustomer"
    val deltaTable = getOrCreateDeltaTable(mainFolderTarget, foldertest, folderTarget, schemaTarget)

    upsertDeltaLakeData(rawData, deltaTable, keyFields)

    println("condition done")
    print(deltaTable.history(1))
*/
  }

}
