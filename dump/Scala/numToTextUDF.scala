

/**
  * Created by pankajgupta on 7/15/17.
  UDF compilation
  1. Compile below code using SBT
  2. Copy the JAR file to HDFS location
  3. Run below command in Hive -
  CREATE FUNCTION g00103.convert_to_text AS 'com.example.hive.udf.numToTextUDF' USING JAR 'hdfs:///user/pankajg/load-orders_2.11-1.0.jar'

Error:
Error while compiling statement: FAILED: HiveAccessControlException Permission denied: user [pankajg] does not have [CREATE] privilege on [g00103/convert_to_text]
  */

package com.udf

import org.apache.hadoop.hive.ql.exec.UDF


class numToTextUDF extends UDF  {

  val numUnitList = List("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine")
  val numTensList = List("", "ten", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninty")
  val num11List = List("ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen")

  def evaluate(args: String): String = {

    //Validate input is a number only
    try {
      val listNumber = args.toDouble.formatted("%.2f").split('.')

      val listInt = listNumber(0).reverse.grouped(3).toList.reverse.map {_.reverse} //This Converts "12345678" to ("12","345","678")
      val fractionExist = (listNumber.length == 2) && (listNumber(1).toInt != 0)
      val listFraction = if (fractionExist) listNumber(1) :: Nil else Nil

      if (fractionExist)
        return (args(0) + ": " + numFullStr(listInt) + " and " + numFullStr(listFraction))
      else
        return (args(0) + ": " + numFullStr(listInt))
    } catch {
      case e: Throwable =>
        return (e.toString)
    }
  }

    private def numFullStr(myList: List[String]): String = {

      myList match {
        case Nil => ""
        case x :: tail =>  {
          val curNum = x.toInt
          myList.length match {
            case 1 => if (curNum != 0) num3Str(x.toList) else ""
            case 2 => if (curNum != 0) num3Str(x.toList) + " thousand " + numFullStr(tail) else numFullStr(tail)
            case 3 => if (curNum != 0) num3Str(x.toList) + " million "  + numFullStr(tail) else numFullStr(tail)
            case 4 => if (curNum != 0) num3Str(x.toList) + " billion "  + numFullStr(tail) else numFullStr(tail)
            case 5 => if (curNum != 0) num3Str(x.toList) + " trillion " + numFullStr(tail) else numFullStr(tail)
          }
        }
      }
    }

    private def num3Str(mytext: List[Char]): String = {

      mytext match {
        case Nil => ""
        case x :: tail => {
          val xNum = x.toString.toInt
          val myNum = mytext.mkString("").toInt
          val zeroHund = (mytext.length == 3) && (xNum == 0)

          //println(xNum + " " + myNum + " " +zeroHund)
          mytext.length match {
            case 1 =>  if (xNum != 0) numUnitList(xNum) else ""
            case 2 =>  if (xNum != 1) numTensList(xNum) + " " + num3Str(tail) else num11List(myNum-10)
            case 3 =>  if (!zeroHund) numUnitList(xNum) + " hundred " + num3Str(tail) else num3Str(tail)
          }
        }
      }
    }
}
