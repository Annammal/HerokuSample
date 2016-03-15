package com.aeq.projectflake

import scala.collection.mutable._
import java.util.Arrays
/**
  * Created by suba on 15/3/16.
  */
class QueryProcess {
  val keycols=List("on","by","=")
  val keytable=List("join","from")
  var buffer=ListBuffer[String]()
  val list=querysample.split("\\s+").toList.map(_.trim)
  var n=0

  def querysample:String=
  {
    val query1="""SELECT film.film_id AS FID, film.title AS title,
                 |film.description AS description, category.name AS category, film.rental_rate AS price,
                 |film.length AS length, film.rating AS rating, GROUP_CONCAT(CONCAT(actor.first_name, _utf8' ', actor.last_name) SEPARATOR ', ') AS actors
                 |FROM category LEFT JOIN film_category ON
                 |category.category_id = film_category.category_id LEFT JOIN film ON
                 |film_category.film_id = film.film_id  JOIN film_actor ON
                 |film.film_id = film_actor.film_id JOIN actor ON film_actor.actor_id = actor.actor_id GROUP BY film.film_id""".stripMargin
    //val query1="""SELECT columna columnb FROM mytable;""".stripMargin
    val query=query1.toLowerCase()
    val samplequery=query.stripMargin.replaceAll("\n","")
    //println(samplequery)
    return samplequery
  }

  def columnsInSelectToFrom:List[String]={
    val temp1=ListBuffer[String]()
    var indexOfFrom=0
    var indexOfSelect=0
    list.zipWithIndex foreach { list1=>
      if(list1._1.equalsIgnoreCase("select"))
      {

        indexOfSelect=list1._2+1
      }
      if(list1._1.equalsIgnoreCase("from"))
      {
        indexOfFrom=list1._2-1
      }
      for(x<- indexOfSelect until indexOfFrom)
      {

        temp1.append(list(x))
      }
    }
    val colsInSelectToFrom=temp1.toList.distinct
    return colsInSelectToFrom
  }

  def columnsInKeyWords:List[String]={
    var temp=ListBuffer[String]()

    list.zipWithIndex foreach {pair=>
      for(keys<-keycols){
        if(pair._1.equalsIgnoreCase(keys))
        {
          if(list.size>pair._2+1){
            temp.append(list(pair._2+1))
          }
        }
      }
    }
    var colsInKeyWord=temp.toList
    //println("Columns In from to by:"+colsInKeyWord)
    return colsInKeyWord
  }

  def getTable:List[String]={
    var temp=ListBuffer[String]()
    list.zipWithIndex foreach {pair=>
      for(keys<-keytable)
        if(pair._1.equalsIgnoreCase(keys))
        {
          if(list.size>pair._2+1){
            temp.append(list(pair._2+1))
          }
        }
    }
    val tables=temp.toList
    return tables
  }

  def getAliase: List[String] = {
    val buf = ListBuffer[String]()
    list.zipWithIndex.foreach { pair =>
      if(pair._1.equals("as")) {
        buf.append(list(pair._2 + 1))
      }
    }
    val aliase=buf.toList
    return aliase
  }

}
