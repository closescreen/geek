package Dom;
use strict;
use warnings;

sub d2l{
 #> Перевод домена (1-параметр, строка) в домен 2-го уровня
 #> Второй параметр - опции { d3l => ["regexp1","regexp2",...] }

 my ( $dom, $opt ) = @_;
 my $rv;
 $dom =~ s/^\s+|\s+$//g; # spaces
 #>  попадающие под регексп - переводятся в 3-й уровень.
 if ( $opt->{d3l} ){
    my $to3l;
    # домены 3 уровня
    for my $re ( @{ $opt->{d3l} } ){
	 if ($dom=~m/$re/){ $to3l = 1; last }
    }
    if ( $to3l ){
	($rv) = $dom=~m/([^\.]*\.?[^\.]+\.[^\.]+)$/;
    }
 }
 if ( !$rv ){
    ($rv) = $dom=~m/([^\.]+\.[^\.]+)$/;
 }    

 #> Для всех производится очистка от порта, протокола, пути, пробелов
 $rv=~s|\w+\://||; # delete protocol
 $rv=~s|\:\d+||; # delete port
 $rv=~s|\/.*$||; # deplete path
 return $rv||"";
}

sub ref2dom{
 my ($ref, $opt) = @_;
 my $rv;
 $ref||="";
 $ref=~s/^\'|\'$//g; # quotas
 $ref =~s|^.+?\://||; # protocol
 $ref =~s|^www\.||; # www
 $ref =~s|/.*$||; # path
 $ref=~ s/^\s+|\s+$//g; # spaces
 
 #($rv) = $ref=~m/([^\.]+\.[^\.]+)$/;
 return $ref||''; 
}

# $url||="";

sub split_ref{
 # Split http://ya.ru/video to domain(ya.ru) and path(/video)
 $_[0]=~m|^\'?(?:.+?\://)?(?:www\.)?([^/\']+)([^\']+?)?\'?$|;
 return ($1,$2); 
}

1;

