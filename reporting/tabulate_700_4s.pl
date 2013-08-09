#! /bin/perl

use strict;
use Data::Dumper;

my $extractLog = $ARGV[0];

my %codeMaps;
my %singleCounts;
my %fullCounts;

# LOAD RELATOR CODE MAP
open my $fh, "<", "700_4_codes.txt"; # http://www.loc.gov/marc/relators/relacode.html
while (my $line = <$fh>) {
    chomp $line;
    my ($code, $value) = split /\t/, $line;
    $code =~ s/^\-?([a-z]{3})\s*/$1/;
    $codeMaps{$code} = $value;
}
close $fh;
#$codeMaps{ptr} = 'Printer';

# LOAD 700 RECORDS
open $fh, "<", $extractLog;
while (my $line = <$fh>) {
    chomp $line;
    my @parts = split /‡/, $line;
    my @codes;
    for my $part (@parts) {
	next if ($part !~ /^4 /);
	$part =~ s/[^a-z]//g;
	if (exists $codeMaps{$part}) {
	    push @codes, sprintf('%s (%s)',$part,$codeMaps{$part});
	} else {
	    push @codes, $part;
	    print $line,"\n";
	}
	$singleCounts{$part}++;
    }
    if (1 < scalar @codes) {
	$fullCounts{ join "; ", sort @codes }++;
    }
}

for my $code ( sort { $singleCounts{$b} <=> $singleCounts{$a}  } keys %singleCounts ) {
    if (exists $codeMaps{$code}) {
	print $singleCounts{$code}, "\t", $code," (",$codeMaps{$code}, ")\n"
    } else {
	print $singleCounts{$code}, "\t", $code, "\n";
    }
}
for my $code ( sort { $fullCounts{$b} <=> $fullCounts{$a}  } keys %fullCounts ) {
    last if ($fullCounts{$code} < 25);
    print $fullCounts{$code}, "\t", $code, "\n";
}
#print Dumper(\%singleCounts);
#print Dumper(\%fullCounts);
close $fh;
