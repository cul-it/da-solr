#! /bin/perl

use utf8;
use Encode;
use strict;
use Data::Dumper;
binmode(STDOUT,":utf8");

my $extractLog = $ARGV[0];

my %codeNumbers;
my %codeMaps;
my %singleCounts;
my %fullCounts;

# LOAD LOCATION CODE MAP
#open my $fh, "<:encoding(UTF-8)", "../rdf/library.nt";
open my $fh, "<:utf8", "../rdf/library.nt";
while (my $line = <$fh>) {
    chomp $line;
    if ($line =~ /0.1.code/) {
	my ($number, $code);
	if ($line =~ /loc_(\d+)>/) {
	    $number = $1;
	}
	if ($line =~ /code> "(.*)"/) {
	    $code = $1;
	}
	$codeNumbers{$number} = $code;
    }
    if ($line =~ /individual.loc.*schema#label/) {
	my ($number, $label);
	if ($line =~ /loc_(\d+)>/) {
	    $number = $1;
	}
	if ($line =~ /label> "(.*)"/) {
	    $label = $1;
	}
	$codeMaps{ $codeNumbers{$number} } = $label;
    }
}
close $fh;

#print Dumper(\%codeMaps);

# LOAD 852 RECORDS
open $fh, "<:utf8", $extractLog;
while (my $line = <$fh>) {
    chomp $line;
    my @parts = split /‡/, $line;
    my @codes;
    for my $part (@parts) {
	next if ($part !~ /^b /);
	$part =~ s/^b *//;
	$part =~ s/ *$//;

	if (exists $codeMaps{$part}) {
	    push @codes, sprintf('%s (%s)',$part,$codeMaps{$part});
	} else {
	    push @codes, $part;
	    print $line,"\n";
#	    print $part, " => ", $codeMaps{$part},"\n"; exit;
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
close $fh;
