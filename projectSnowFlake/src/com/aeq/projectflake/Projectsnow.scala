package com.aeq.projectflake
import scala.collection.mutable._
import java.util.Arrays

object QueryObject extends App
{
  val sample=new QueryProcess
  val  colsInKey=sample.columnsInKeyWords
  val colsInSelFro=sample.columnsInSelectToFrom
  val tablelist=sample.getTable
  val allkeywords=List("on","group","from","on","select","=","join","left","right","by","as","seperator")
  println("List Of Tables:"+tablelist)
  val getaliase=sample.getAliase
  println("Get aliase NAme:"+getaliase)
  val columnslist=colsInKey ::: colsInSelFro
  val cols=columnslist.distinct
  //val list=lists.replaceAll("""[\p{Punct}&&[^.]&&[^,]&&[^_]]""", "")
  val colsreal=cols.diff(tablelist)
  val allcolumns=colsreal.diff(allkeywords)
  val columnlist=allcolumns.diff(getaliase)

  println("Columns Real"+columnlist)

  val maptableandcolumn=joinColsAndTable
  println(maptableandcolumn)

  def joinColsAndTable() {
    val columns = columnlist.mkString(" ")
    //println("Columns    "+columns)
    val splitColumns = columns.split(" ").toList
    var columnsList = List[(String, String)]()
    for (col <- splitColumns) {
      val index = col.indexOf(".")
      if(index > -1){
        //println("index of '.':"+index)
        val table = col.substring(0, index)
        val column = col.substring(index + 1, col.length())
        var value = (table, column)
        columnsList ++= List(value)
      }
    }
    val tableColumnsMap = scala.collection.mutable.Map[String, List[String]]()

    for (col <- columnsList) {
      if (tablelist.contains(col._1)) {
        val colList = tableColumnsMap.get(col._1)
        var finalColList: List[String] = null
        if (colList.isEmpty) {
          finalColList = List[String]()
        } else {
          finalColList = colList.get
        }
        val newList = finalColList :+ col._2
        tableColumnsMap.put(col._1, newList)
      }
    }
    println("TableColumnMap"+tableColumnsMap)
  }
}





