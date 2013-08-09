#! /bin/perl

use strict;

print "Hello.\n";

my $extractLog = $ARGV[0];

open my $fh, "<", $extractLog;

while (my $line = <$fh>) {
    if ($line =~ /^\d+\t(\d{3})/) {

	open my $outfh, ">>", "$1.tdf";
	print $outfh $line;
	close $outfh;
    } else {
	print $line;
    }
}
