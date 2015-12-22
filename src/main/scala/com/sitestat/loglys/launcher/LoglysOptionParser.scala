package com.sitestat.loglys.launcher

import java.util.regex.{Matcher, Pattern}

import scala.util.control.Breaks._

/**
 * Created by Suresh Kumar Mukhiya <itsmeskm99@gmail.com> on 25/09/15.
 */
class LoglysOptionParser {
  protected val CONF: String = "--conf"
  // protected val JARS: String = "--jars"
  protected val ARTREAD_FILE: String = "--artread-file"
  protected val CLICKLOG_FILE: String = "--clicklog-file"

  protected val HELP: String = "--help"
  protected val VERBOSE: String = "--verbose"
  protected val VERSION: String = "--version"
  /**
   * This is the canonical list of spark-submit options. Each entry in the array contains the
   * different aliases for the same option; the first element of each entry is the "official"
   * name of the option, passed to {@link #handle(String, String)}.
   * <p/>
   * Options not listed here nor in the "switch" list below will result in a call to
   * {@link $#handleUnknown(String)}.
   * <p/>
   * These two arrays are visible for tests.
   */
  private[launcher] val opts: Array[Array[String]] = Array(Array(CONF, "-c"), Array(ARTREAD_FILE), Array(CLICKLOG_FILE))
  /**
   * List of switches (command line options that do not take parameters) recognized by spark-submit.
   */
  private[launcher] val switches: Array[Array[String]] = Array(Array(HELP, "-h"), Array(VERBOSE, "-v"), Array(VERSION))

  /**
   * Parse a list of spark-submit command line options.
   * <p/>
   * See SparkSubmitArguments.scala for a more formal description of available options.
   *
   * @throws IllegalArgumentException If an error is found during parsing.
   */
  protected def parse(args: Array[String]) {
    val eqSeparatedOpt: Pattern = Pattern.compile("(--[^=]+)=(.+)")
    var idx: Int = 0
    breakable {
        while (idx < args.size) {
          var arg: String = args(idx)
          var value: String = null
          var ableToHandle: Boolean = false
          val m: Matcher = eqSeparatedOpt.matcher(arg)
          if (m.matches) {
            arg = m.group(1)
            value = m.group(2)
          }

          var name: String = findCliOption(arg, opts)
          if (name != null) {
            if (value == null) {
              if (idx == args.size - 1) {
                throw new IllegalArgumentException(String.format("Missing argument for option '%s'.", arg))
              }
              idx += 1
              value = args(idx)
            }
            if (!handle(name, value)) {
              break
            }
            ableToHandle = true //todo: continue is not supported
          }

          if (!ableToHandle) {
            name = findCliOption(arg, switches)
            if (name != null) {
              if (!handle(name, null)) {
                break
              }
              // continue //todo: continue is not supported
              ableToHandle = true
            }
            if (!ableToHandle) {
              if (!handleUnknown(arg)) {
                break
              }
            }
          }
            idx += 1
        }
    }

    if (idx < args.size) {
      idx += 1
    }
    handleExtraArgs(args.slice(idx, args.size))
  }

  /**
   * Callback for when an option with an argument is parsed.
   *
   * @param opt The long name of the cli option (might differ from actual command line).
   * @param value The value. This will be <i>null</i> if the option does not take a value.
   * @return Whether to continue parsing the argument list.
   */
  protected def handle(opt: String, value: String): Boolean = {
    throw new UnsupportedOperationException
  }

  /**
   * Callback for when an unrecognized option is parsed.
   *
   * @param opt Unrecognized option from the command line.
   * @return Whether to continue parsing the argument list.
   */
  protected def handleUnknown(opt: String): Boolean = {
    throw new UnsupportedOperationException
  }

  /**
   * Callback for remaining command line arguments after either {@link #handle(String, String)} or
   * {@link #handleUnknown(String)} return "false". This will be called at the end of parsing even
   * when there are no remaining arguments.
   *
   * @param extra List of remaining arguments.
   */
  protected def handleExtraArgs(extra: Array[String]) {
    throw new UnsupportedOperationException
  }

  private def findCliOption(name: String, available: Array[Array[String]]): String = {
    for (candidates <- available) {
      for (candidate <- candidates) {
        if (candidate == name) {
          return candidates(0)
        }
      }
    }
    return null
  }
}

