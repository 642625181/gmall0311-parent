package com.atguigu.gmall0311.app

import java.text.SimpleDateFormat
import java.util.Date

object Test {
  def main(args: Array[String]): Unit = {
    val str: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date("1565695981005".toLong))
    println(str)
  }

}
