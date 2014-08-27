#!/usr/bin/env bash
#>> Экперимент скрипта на scala ( 19_process.sh + 10_process.scala )
#> Суммы счетчиков за N дней по файлам urls.gz
set -u
set +x
set -o pipefail
#>: параметр (имя вх файла):
src_file=${1:?Src file!} # f.e...../urls.gz

#>: параметр (job):
job=${2:?Job!} # f.e. google

#>: парам:
typenum=${3:?typenum!}

#>: параметр (days):
N=${4:?N Days!} # f.e. 30. За сколько дней суммировать

day=`fn2days $src_file`

ff=$( hours -t="${day}T00" -shift=1day -n=-"$N"days -days -comm="<---$N days with $day included." | files "../RESULT/10/%F/$job/$typenum/urls.gz" )
ff=$( echo "$ff" | only -all -s | only -all -sizegt=20 )
chk "$ff" "Files for $N days back from $day. (Not found $? files)" || exit 2

echo "$ff" |
 LANG=POSIX mergef -m -k=1,1 -k=2,2 -stdout -comm="<---ursl.gz merged for $N days" |
 scasca "$0" -between="#SCALA1 AND #END1" -dest="./targets/$(basename $0).jar" -main=Proc -automkdir -cp ./sjlibs/adriver.rd.jar

exit

#SCALA1 ================================
import scala.io._
import adriver.rd.util.fastSplit

object Proc extends App {
 // println("Hello, world!")
 var prevDom = ""  //field 0
 var prevPath = "" //field 1
 var sessions = 0  //field 2
 var sess_ends = 0 //field 3

 def flush(){
    println( List( prevDom, prevPath, sessions, sess_ends).mkString("*") )
    sessions = 0
    sess_ends = 0
 }    

 for ( ln <- io.Source.stdin.getLines ){ 
    val fi = fastSplit.split(ln, '*');
    if ( !(fi.get(1).equals(prevPath)) || !(fi.get(0).equals(prevDom)) && !(prevDom.equals("")) ) flush()
    sessions+= java.lang.Integer.valueOf( fi.get(2).trim )
    sess_ends+= java.lang.Integer.valueOf( fi.get(3).trim )
    prevDom = fi.get(0)
    prevPath = fi.get(1)
 }
 
 flush()
 
}
#END1 ==================================

#-----------------
# code on awk:
#awk -F* -v"OFS=*" '
#BEGIN{
# prevDom="";
# prevPath="";
#}
#
#{
# if ( ($2!=prevPath || $1!=prevDom ) && prevDom ){ flush() };
# sessions+=$3;
# sess_ends+=$4;
# prevDom=$1;
# prevPath=$2;
#}
#
#END{ 
# flush();
#}
#
#function flush(){
# print prevDom, prevPath, sessions, sess_ends;
# sessions=0;
# sess_ends=0;
#}
#'
# ------------------------

#>> out
#>>
#>> | field     |
#>> | --------- |
#>> | dom       |
#>> | path      |
#>> | sessions  |
#>> | sess_ends |

