#!/usr/bin/perl
#> Только для FIX (2015.09.15)
#> удалить, если в базе указанный урл (поле 1) имеет категорию из списка --del-cats
#> на входе поток формата: url * ...


use strict;
use lib '/usr/local/rle/lib/perl';
use Data::Dumper;
use RLE::RetargetingClient;
use Getopt::Long;

my @del_cats;

GetOptions(
 "del-cats=s" => \@del_cats,
) or die "bad opt!";

@del_cats = grep {m/\d+/}split /\s*\,\s*/, join ",", @del_cats;
@del_cats or die "--del-cats!";
warn Dumper "Delete cats:".Dumper(\@del_cats);

my %del_cats = map {$_,1} @del_cats;

my $rc = RLE::RetargetingClient->new(1);
$rc->add("re1.adriver.x", 46000, 1);
$rc->Connect();
$rc->{ERRHAND} = sub { print Dumper(@_); };

# input format:
# url * categ * value * action
# ya.ru/* 2 * 33 * INSERT

local $|=1;
my $cnt=0;

my ( $url, $cat_id, $value, $action, $current_url, $current_url_id, $cats_to_delete );
while(<STDIN>){
    chomp;
    ( $url, $cat_id, $value, $action ) = split /\*/;
    warn("next"), next if $url eq '';
    if ( ! $url ){
	warn "bad url";
	next;
    }

    if ($current_url and $current_url eq $url){
	warn "skip dublicate $url"
    }

    if ( $current_url and $url ne $current_url ){
	flush();
    }

    $current_url = $url;
    
    for my $cat (@del_cats){
	push @$cats_to_delete, $cat;
    }	
}
flush();

sub flush{

 $cnt++;
 print "$cnt " if $cnt % 10000 == 0;
 my $url_clean = $rc->preprocessReferrer('http://'.$url);
 $url_clean or return;
 $current_url_id = $rc->GetUrlId($url_clean);
 if ( !$current_url_id ){
    die "current_url_id! $_";
 }

 # delete:
 $rc->DelUrlCategories($current_url_id, $cats_to_delete) if @$cats_to_delete;
 
 @$cats_to_delete = ();

}


