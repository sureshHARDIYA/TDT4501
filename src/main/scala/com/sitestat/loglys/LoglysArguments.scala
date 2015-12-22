package com.sitestat.loglys

import com.sitestat.loglys.launcher.LoglysOptionParser

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by Suresh Kumar Mukhiya <itsmeskm99@gmail.com> on 25/09/15.
 */
/**
 * Parses and encapsulates arguments from HotfootGenerate script.
 * The env argument is used for testing.
 */
private[loglys] class LoglysArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends LoglysOptionParser {
  var artReadFile: String = null
  var clickLogFile: String = null
  var verbose: Boolean = false
  val hotfootProperties: HashMap[String, String] = new HashMap[String, String]()

  // var outPutFormat: String = null
  // var outputPath: String = null
  // var jars: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

  // Set parameters from command line arguments
  try {
    parse(args.toArray)
  } catch {
    case e: IllegalArgumentException =>
      Loglys.printErrorAndExit(e.getMessage())
  }

  validateArguments()


  private def validateArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
  }

  override def toString: String = {
    s"""Parsed arguments:
        |  artReadFile              $artReadFile
        |  clickLogFile              [${childArgs.mkString(" ")}]
        |  verbose                 $verbose
    """.stripMargin
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {

      case ARTREAD_FILE =>
        artReadFile = value

      case CLICKLOG_FILE =>
        clickLogFile = value


      case CONF =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => hotfootProperties(k) = v
          case _ => Loglys.printErrorAndExit(s"LogLys config without '=': $value")
        }

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION =>
        Loglys.printVersionAndExit()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

  /**
   * Handle unrecognized command line options.
   *
   * The first unrecognized option is treated as the "primary resource". Everything else is
   * treated as application arguments.
   */
  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {
      Loglys.printErrorAndExit(s"Unrecognized option '$opt'.")
    }

    false
  }

  override protected def handleExtraArgs(extra: Array[String]): Unit = {
    childArgs ++= extra
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    val outStream = Loglys.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    outStream.println(
      """Usage: hotfoot-generate [options] <app jar | python file> [app arguments]
        |
        |Options:
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/hotfoot-defaults.conf.
        |  --schema-file SFILE         Path to a json file that specifies the schema of data to be
        |                              written.
        |  --help, -h                  Show this help message and exit
        |  --verbose, -v               Print additional debug output
        |  --version,                  Print the version of current Spark
        |
      """.stripMargin
    )
    Loglys.exitFn()
  }

}

