#!/usr/bin/perl
use strict;
use lib '/usr/local/rle/lib/perl';
use Data::Dumper;
use RLE::RetargetingClient;
use Getopt::Long;

# тоже само, что set только наоборот-удаление.
# Зачем? .это для ручных удалений. например удалить 781 категорию.

# на вход подать, sz_vars_conv Обычный содержащий ЧТО УДАЛИТЬ.

#GetOptions(
#) or die "Bad opt!";

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
	push @todel, $cat;
    }

    $rc->DelUrlCategories( $id, \@todel) if @todel; # удаление 
    
    $stat{ lines }++;	
}

print "Deleted $stat{ lines } lines";
