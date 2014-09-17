#!/usr/bin/env bash
#> urls.

#>>IN:
#>> google:
#>>  1   2     3   4  5       6        7   8   9     10      11
#>> uid second sz pz  bt exposureprice ad dom path sestart isview
#>> OR
#>> net_sites:
#>> uid second sid sz pz bt exposureprice secondprice divexpsecond ad network dom path sestart isview


#>> sorted by UID,SECOND:
set -x
lb=${1:?labels!}

fields_numbers=`perl -e'
 @F=split /\s+/, shift; 
 %L = map { $F[$_],$_+1 } (0..$#F); 
 print join ",", @L{qw| uid dom path sestart isview |}' "$lb"
`
cut -d* -f$fields_numbers |
 scasca "$0" -main=Proc17 -desti=./targets/$(basename $0).jar -between="#START_SCALA and #STOP_SCALA" |
 LANG=POSIX sort -T. -S 333M -t\* -k1,1 -k2,2


exit

#START_SCALA
import scala.io
import scala.collection.mutable._

object Proc17 extends App {
 
 val (uid_i, dom_i, path_i, sestart_i, isview_i) = (0,1,2,3,4)
 
 var prevUid: Long = 0
 var prevSestart: Long = 0
 var prevDom = ""
 var prevPath = ""
 var urls = Set[String]()		 
 var sessions_count = Map[String,Int]()
 var sessions_ends = Map[String,Int]()
 
 for ( l <- io.Source.stdin.getLines ){
  val fi = l.split('*')
 
  val uid = fi(uid_i).toLong
  val dom = fi(dom_i)
  val path = fi(path_i)
  val sestart = fi(sestart_i).toLong
  val isview = fi(isview_i)
  
  
  if ( fi(isview_i).equals("1") ){
	  if ( ( uid!=prevUid  ||  sestart!=prevSestart ) && prevUid!=0 && prevSestart!=0 ) increment_counters()  
	  urls += ( dom + '*' + path )
  }
  prevUid = uid
  prevSestart = sestart
  prevDom = dom
  prevPath = path
  
 }
 increment_counters()

 for ( (ref, s_cnt) <- sessions_count ){
   val e_cnt = sessions_ends.getOrElse(ref, 0)
   println( ref + '*' + s_cnt + '*' + e_cnt) 
 }
 
 def increment_counters () : Unit = {
  for ( url <- urls ){
   val cnt = sessions_count.getOrElse(url, 0)
   sessions_count(url) =cnt+1
  }
 
  val prevUrl = prevDom+"*"+prevPath
  val ends_count = sessions_ends.getOrElse(prevUrl,0)
  sessions_ends(prevUrl) = ends_count+1
 
  urls = Set[String]()
 }
  
}
#STOP_SCALA


#>>OUT:
#>> dom, ref, sessions_count, sessions_ends
#>>  
#>> | field          | description |
#>> | -------------- | ----------- |
#>> | dom |
#>> | ref |
#>> | sessions_count | количество сессий, где dom+url участвовал |
#>> | sessions_ends  | количество сессий, где он был последним.  |





