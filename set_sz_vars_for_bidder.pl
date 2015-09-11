#!/usr/bin/perl
use strict;
use lib '/usr/local/rle/lib/perl';
use Data::Dumper;
use RLE::RetargetingClient;
use Getopt::Long;

my $del0;
GetOptions(
 "del-0-categories" => \$del0, # если указать эту опцию, то нулевые значения будут удаляться и не спамиться
) or die "Bad opt!";

my $rc = RLE::RetargetingClient->new(1);
$rc->add("re1.adriver.x", 46000, 1);
$rc->Connect();
my ($cat,$val,@cats);
$rc->{ERRHAND} = sub { warn "cat=$cat,val=$val",Dumper \@cats, Dumper(@_),$_; };

my %stat;

while(<STDIN>) {
    chomp;
    my ($id, @cat_vals) = split(/\*/, $_, -1);
    undef @cats;
    my @todel;
    for my $cat_val ( @cat_vals ){
        my ( $cv, $comment ) = split /\s+/, $cat_val, 2;
	($cat, $val) = split "\:", $cv, 2; 
	print "$cat_val = $cat, $val\n";
	next if $val eq "NA";
	if ( $del0 and !$val ){
	    push @todel, $cat;
	    next;
	}
        push @cats, { categoryId=>$cat, weight=>$val };
    }

    $rc->DelUrlCategories( $id, \@todel) if $del0 and @todel; # удаление категорий с 0 значениями, если передана опция -del-0-cat
    
    $rc->SetUrlCategories( $id, \@cats ) if @cats;
    for my $cat (@cats){
	$rc->SetUrlCategoryPersistent( $id, $cat->{categoryId}, 1 );
    }
    
    $stat{ lines }++;	
}

print "Sent $stat{ lines } lines";
