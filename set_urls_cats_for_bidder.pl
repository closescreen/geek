#!/usr/bin/perl

use strict;

use lib '/usr/local/rle/lib/perl';

use Data::Dumper;

use RLE::RetargetingClient;

my $rc = RLE::RetargetingClient->new(1);
$rc->add("re1.adriver.x", 46000, 1);
$rc->Connect();
$rc->{ERRHAND} = sub { print Dumper(@_); };

my $input = { map { my ($k, $v) = split('=', $_); $k => $v } @ARGV };

$input->{cat_id} = 1 unless $input->{cat_id};

my %stat;
while(<STDIN>) {
    chomp;
    my ($url, $group_id, @tail) = split(/\*/, $_);

    $group_id = int($group_id);

    next if $url eq '' || $group_id < 1;

    my $url_clean = $rc->preprocessReferrer('http://'.$url);
    next if $url eq '';

    my $url_id = $rc->GetUrlId($url_clean);

    my $cat = {categoryId=>$input->{cat_id}, weight=>$group_id};

    if (! defined $tail[0] or $tail[0] eq 'insert') {
#    	print STDERR "$url_id\t",Dumper($cat),"\n";
	$rc->SetUrlCategories($url_id, [$cat]);
	$rc->SetUrlCategoryPersistent($url_id, $input->{cat_id}, 1);
#    	print STDERR Dumper($rc->GetUrlCategories($url_id));
#	$stat{$group_id}++;
    }
    elsif (defined $tail[0] and $tail[0] eq 'delete') {
	$rc->DelUrlCategories($url_id, [$input->{cat_id}]);
#    	print STDERR Dumper($rc->GetUrlCategories($url_id));
    }
}
#map {print STDERR $_.' '.$stat{$_}."\n"} sort keys %stat;
