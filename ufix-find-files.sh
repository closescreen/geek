#!/usr/bin/env bash
#> для фикса базы
#(
set -u
set +x
set -o pipefail
cd `dirname $0`

jobs="google ssp net"

( for job in $jobs; do
 ff=`find ../RESULT -name "sz_vars_30days.gz" | grep $job | only -upto="2015-08-10"`
 
( for f in $ff;do
  dire=`dirname $f`
  #bas=`basename $f`
  #modify=`stat -c%y $f | cut -d" " -f1` # время самого gz
  sentmodify=`stat -c%y $dire/sz_vars_conv.sent | cut -d" " -f1` # время sent
  ts=`stat -c%Y $dire/sz_vars_conv.sent`
  echo "$f*$sentmodify*$ts*$job"
 done ) | sort -t\* -k2,2 
 
done ) |
cat
exit 
 lae -lb="file sent ts job" '
 my @sent;
 _{
    push @sent, {file=>File, sent=>Sent, sentts=>Ts, job=>Job}
 };
 
 my @to; 
 my $i=0;
 for my $s (@sent){
    my $to = $sent[$i+1] ? $sent[$i+1]{sent} : "now";
    my $tots = $sent[$i+1] ? $sent[$i+1]{sentts} : "now";
    push @to, {file=>$s->{file}, sent=>$s->{sent}, sentts=>$s->{sentts}, to=>$to, tots=>$tots, job=>$s->{job},};
    $i++;
 }
 #die Dumper \@to;
 
 local $\="\n";
 for my $e (@to){
    my $tofn="$e->{file}_$e->{job}_$e->{sentts}--$e->{tots}_$e->{sent}--$e->{to}.gz";
    my $only_true_sz = $e->{job} eq "net" ? "-truesz" : "";
#    warn "file $e->{file} with true sz" if $only_true_sz;
    my $cmd ="zcat $e->{file} | ./53_sz_vars_conv_FIX -conf=53_sz_vars_decl.conf $only_true_sz | viatmp -gz $tofn";
#    warn $cmd;
    my $rv = system($cmd);
    print $tofn;
 }
 
'
