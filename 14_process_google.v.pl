#!/usr/bin/env bash
#> версия чистого перла
#> Making table "bids" (tn3).
#>> For google, ssp_sites.
#>> in (from tn3 total.gz):
#>>  1   2     3   4  5       6        7   8   9     10      11
#>> uid second sz pz  bt exposureprice ad dom path sestart isview

perl -w -F'\*' -lane'
$bids{ $F[2] }{ $F[6] }++;
$dom{ $F[2] } ||= $F[7];

END{
 $,="*";
 for $sz ( keys %bids ){
     for $ad ( keys %{$bids{$sz}} ){
        print $sz, $ad, $bids{ $sz }{ $ad }, $dom{ $sz };
     }
 }
}
' | sort -T. -t\* -S 333M -n -k1,1 -k2,2


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



