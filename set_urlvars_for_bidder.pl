#!/usr/bin/perl

use strict;

use lib '/usr/local/rle/lib/perl';

use Data::Dumper;

use RLE::RetargetingClient;

my $rc = RLE::RetargetingClient->new(1);
$rc->add("re1.adriver.x", 46000, 1);
$rc->Connect();
$rc->{ERRHAND} = sub { print Dumper(@_); };

# input format:
# url * categ * value * action
# ya.ru/* 2 * 33 * INSERT


my ( $url, $cat_id, $value, $action, $current_url, $current_url_id,  @cats_to_insert, @cats_to_delete );
while(<STDIN>){
    chomp;
    ($url, $cat_id, $value, $action ) = split /\*/;
    warn("next"), next if $url eq '';
    if ( ! $url ){
	warn "bad url";
	next;
    }
    if ( $action eq "INSERT" and $value < 1 ){
	# что делать если INSERT значение = 0 ?
	#warn "bad value=$value, line=$_";
	next;
    }
    if ( $current_url and $url ne $current_url ){

	flush();
    }
    $current_url = $url;
    
    my $cat_href = {categoryId=>$cat_id, weight=>$value};

    if ( $action eq 'INSERT' ){
	push @cats_to_insert, $cat_href;
    }elsif ($action eq 'DELETE'){
	push @cats_to_delete, $cat_id;
    }else{
	die "Unknown action $action";
    }
}
flush();

sub flush{

 my $url_clean = $rc->preprocessReferrer('http://'.$url);
 $url_clean or return;
 $current_url_id = $rc->GetUrlId($url_clean);
 if ( !$current_url_id ){
    die "current_url_id! $_";
 }

 # insert:
# warn "will: SetUrlCategories($current_url_id, [@cats_to_insert])" if @cats_to_insert;
 $rc->SetUrlCategories($current_url_id, [@cats_to_insert]) if @cats_to_insert;
 for my $cat_href ( @cats_to_insert ){
    my $cat_id = $cat_href->{categoryId} or die "CAT!";
    $rc->SetUrlCategoryPersistent($current_url_id, $cat_id, 1);  
 }
 
 # delete:
 $rc->DelUrlCategories($current_url_id, [@cats_to_delete]) if @cats_to_delete;
 
 @cats_to_insert = @cats_to_delete = ();

}


