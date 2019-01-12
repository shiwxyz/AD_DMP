package scala.cn.dmp.report

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import src.main.scala.cn.dmp.utils.{NBF, SchemaUtils}

object Bzip2Parquet {
  def main(args: Array[String]): Unit = {
  /*  //1. 校验参数个数
    if(args != 2){
      println(
        """
          |cn.dmp.report.Bzip2Parquet
          |logInputPath
          |ResultOutputPath
        """.stripMargin)

      sys.exit()
    }*/

    //2. 接收程序参数
    //val Array(logInputPath,ResultOutputPath) = args

    //3. 构建SparkConf -> SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Bzip2Parquet")
    sparkConf.setMaster("local")
    //RDD序列化到磁盘
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    //4. 读取日志数据
    val logdata = sc.textFile("F:\\qf\\小牛9天项目实战\\log\\input")

    //5. 根据业务需求对数据进行ETL
    val logRow = logdata
      .map(line => line.split(","))
      .filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          NBF.toInt(arr(1)),
          NBF.toInt(arr(2)),
          NBF.toInt(arr(3)),
          NBF.toInt(arr(4)),
          arr(5),
          arr(6),
          NBF.toInt(arr(7)),
          NBF.toInt(arr(8)),
          NBF.toDouble(arr(9)),
          NBF.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(15),
          arr(16),
          NBF.toInt(arr(17)),
          arr(18),
          arr(19),
          NBF.toInt(arr(20)),
          NBF.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          NBF.toInt(arr(26)),
          arr(27),
          NBF.toInt(arr(28)),
          arr(29),
          NBF.toInt(arr(30)),
          NBF.toInt(arr(31)),
          NBF.toInt(arr(32)),
          arr(33),
          NBF.toInt(arr(34)),
          NBF.toInt(arr(35)),
          NBF.toInt(arr(36)),
          arr(37),
          NBF.toInt(arr(38)),
          NBF.toInt(arr(39)),
          arr(40),
          arr(41),
          NBF.toInt(arr(42)),
          arr(43),
          arr(44),
          NBF.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          NBF.toInt(arr(57)),
          arr(58),
          NBF.toInt(arr(59)),
          NBF.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          NBF.toInt(arr(73)),
          NBF.toDouble(arr(74)),
          NBF.toDouble(arr(75)),
          NBF.toDouble(arr(76)),
          NBF.toDouble(arr(77)),
          NBF.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          NBF.toInt(arr(84))
        )
      })


    //5. 将结果存储到本地磁盘
    val dataFrame = sqlContext.createDataFrame(logRow,SchemaUtils.logStructType)
    dataFrame.write.parquet("F:\\qf\\小牛9天项目实战\\log\\output")

    //6.关闭sc
    sc.stop()


  }
}
