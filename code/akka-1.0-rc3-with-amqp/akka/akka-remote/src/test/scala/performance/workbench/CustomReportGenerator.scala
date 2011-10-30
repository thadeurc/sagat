package performance.workbench

import akka.performance.workbench.{Stats, Report, BenchResultRepository}
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import java.io.{FileInputStream, BufferedInputStream, ObjectInputStream, File}
import collection.mutable.{HashMap, Map}
import java.util.Scanner

class CustomReportGenerator extends JUnitSuite {
  def loadStatistics(location: String): Seq[Stats] = {
     val files =
        for {
          f <- new File(location).listFiles
          if f.isFile
          if f.getName.endsWith(".ser")
        } yield f
     load(files)
  }

  private def load(files: Iterable[File]): Seq[Stats] = {
    val result =
      for (f <- files) yield {
        var in: ObjectInputStream = null
        try {
          in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(f)))
          val stats = in.readObject.asInstanceOf[Stats]
          Some(stats)
        } catch {
          case e: Throwable =>
            println("Failed to load from [%s], due to [%s]".
              format(f.getAbsolutePath, e.getMessage))
            None
        } finally {
          if (in ne null) try { in.close() } catch { case ignore: Exception => }
        }
      }
    result.flatten.toSeq.sortBy(_.load)
  }

  //@Test
  def generateReports = {
    val statistics1 = loadStatistics("/Users/thadeu/Documents/IME-USP/sagat/code/akka-1.0-rc3-with-amqp/akka/backup-benchmarks/benchmark/ser/all-amqp/amqp-all/")
    val statistics2 = loadStatistics("/Users/thadeu/Documents/IME-USP/sagat/code/akka-1.0-rc3-with-amqp/akka/backup-benchmarks/benchmark/ser/all-netty/netty-all/")
    val repositories = new HashMap[Int, BenchResultRepository]
    statistics1.groupBy(_.load).foreach{
      case(key,data) => {
        val resultRepository = BenchResultRepository(true)
        val report = new Report(resultRepository)
        data.foreach(s => {
          resultRepository.add(s)
          report.html(resultRepository.get(s.name))
        })
        repositories.put(data.head.load, resultRepository)
      }
    }

    statistics2.groupBy(_.load).foreach{
      case(key,data) => {
        val resultRepository = repositories.get(data.head.load).get
        val report = new Report(resultRepository, Some("AMQPRemoteTwoWayPerformanceTest"))
        data.foreach(s => {
          resultRepository.add(s)
          report.html(resultRepository.get(s.name))
        })
        resultRepository
      }
    }

  }



 // @Test
  def generateReportsBothBothAndCompareTheBestOnes = {
    val statistics1 = loadStatistics("/Users/thadeu/Documents/IME-USP/sagat/code/akka-1.0-rc3-with-amqp/akka/backup-benchmarks/benchmark/ser/all-amqp/amqp-all/")
    val statistics2 = loadStatistics("/Users/thadeu/Documents/IME-USP/sagat/code/akka-1.0-rc3-with-amqp/akka/backup-benchmarks/benchmark/ser/all-netty/netty-all/")
    //val repositories = new HashMap[Int, BenchResultRepository]
    val resultRepository = BenchResultRepository(true)
    statistics1.groupBy(_.load).foreach{
      case(key,data) => {
        val report = new Report(resultRepository)
        data.sortBy(_.durationNanos).slice(0,1).foreach(s => {
          resultRepository.add(s)
          report.html(resultRepository.get(s.name))
        })
//        repositories.put(data.head.load, resultRepository)
      }
    }

    /*statistics2.groupBy(_.load).foreach{
      case(key,data) => {
        //val resultRepository = repositories.get(data.head.load).get
        val report = new Report(resultRepository, Some("AMQPRemoteTwoWayPerformanceTest"))
        data.sortBy(_.durationNanos).slice(0,1).foreach(s => {
          resultRepository.add(s)
          report.html(resultRepository.get(s.name))
        })
      }
    } */
  }

  @Test
  def convertData = {
    val data = "-103	407	825	293	378	423	257	2\n-6.91%	7.60%	35.64%	8.95%	10.02%	8.91%	4.17%	10.00%\n-118	539	463	391	777	432	569	2\n-7.66%	8.31%	13.68%	9.76%	17.94%	7.57%	8.08%	10.53%\n-63	552	-337	522	318	742	1415	1\n-4.03%	4.33%	-4.54%	6.36%	3.30%	7.31%	11.78%	5.26%\n-69	1169	-737	880	1342	1406	2010	1\n-4.35%	4.64%	-4.43%	5.00%	7.38%	7.23%	9.41%	5.26%\n-79	2597	367	2512	2858	3080	3455	1\n-4.97%	5.24%	1.11%	7.10%	7.82%	8.13%	8.63%	5.26%\n-87	-4730	-31611	-12231	-9129	-8518	351	1\n-5.46%	-7.55%	-74.01%	-27.37%	-19.94%	-18.01%	0.70%	5.26%"
    val reader = new Scanner(data)
    while(reader.hasNextLine){
      val line1 = reader.nextLine
      val line2 = reader.nextLine
      val split1 = line1.split("\\s+")
      val split2 = line2.split("\\s+")
      val pair = split1.zip(split2)
      val result = for(p <- pair) yield "<td>%s (%s)</td>".format(p._1, p._2)
      result.foreach(print)
      println
    }
  }
}