#!/usr/bin/env bash
#> Making table "bids" (tn3).
#>> For google, ssp_sites.
#>> in (from tn3 total.gz):
#>>  1   2     3   4  5       6        7   8   9     10      11
#>> uid second sz pz  bt exposureprice ad dom path sestart isview


scasca "$0" -main=app1 -between="#SCALA_BEGIN and #SCALA_END" -desti=./targets/14_process_google.jar |
 sort -T. -t\* -S 333M -n -k1,1 -k2,2

exit
#SCALA_BEGIN

import scala.io._
import collection.mutable.Map

object app1 extends App {
  var m = Map[String, Map[String,Int]]()
  var d = Map[String,String]()
      
 for ( ln <- Source.stdin.getLines() ){
  val fi: Array[String] = ln.split('*')
  // input ( google 3 total ) format:
  // uid second sz pz  bt exposureprice ad dom path sestart isview
  val sz = fi(2)
  val ad = fi(6)
  var l2 = m.getOrElse( sz, Map[String,Int]() )
  var cnt = l2.getOrElse(ad, 0)
  cnt = cnt+1
  l2 += ( ad -> cnt )
  m.getOrElseUpdate(sz, l2 )
  val dom = fi(7)
  d.getOrElseUpdate(sz, dom)
 }
 
 // println(m)
 //println(d)
 
 m.foreach{ 
    case (sz,m1) => m1.foreach{ 
	case (ad,cnt) => println(sz+'*'+ad+'*'+cnt+'*'+d.getOrElse(sz, "NA")) 
    } 
 }

}
    	                                                         
#SCALA_END

#>> OUT: sz, ad, bids, domain.
#>> 
#>> | field  | description |
#>> | ------ | ----------- |
#>> | sz     |
#>> | ad     |
#>> | bids   | количество бидов |
#>> | domain | домен (как внешний ключ для объединения с данными, сгруппированными по доменам ) |
#>>     Прим. 
#>>      Поле "количество бидов при exposureprice>0" (posibids) отсутствует, потому что
#>>      оно равно значению поля bids, за исключением случаев, когда ad=0. В этом случае оно равно 0.

#-------------------------------------
# old code:
#lae -lb="uid second sz pz  bt exposureprice ad dom path sestart isview" '
#my ( %bids, %positive_bids, %dom );
#_{
# $bids{ &Sz }{ &Ad }++;
# $dom{ &Sz } ||= &Dom;
#};
#
#for my $sz ( keys %bids ){
#    for my $ad ( keys %{$bids{$sz}} ){
#	p0 $sz, $ad, $bids{ $sz }{ $ad }, $dom{ $sz };
#    }
#}
#
#' | sort -T. -t\* -S 333M -n -k1,1 -k2,2

