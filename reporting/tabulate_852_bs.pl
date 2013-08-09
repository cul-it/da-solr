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
$codeMaps{'rmc,anx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'mann,doc'} = "LOCATIONOPAC = 'N'";
$codeMaps{'olin,av'} = "LOCATIONOPAC = 'N'";
$codeMaps{'uris,anx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'maps,anx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'lawr,anx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'ilr,ts'} = "LOCATIONOPAC = 'N'";
$codeMaps{'orni,cumv'} = "LOCATIONOPAC = 'N'";
$codeMaps{'lawr'} = "LOCATIONOPAC = 'N'";
$codeMaps{'olin,602'} = "LOCATIONOPAC = 'N'";
$codeMaps{'orni,mac'} = "LOCATIONOPAC = 'N'";
$codeMaps{'ech,ranx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'vet,crar'} = "LOCATIONOPAC = 'N'";
$codeMaps{'was,ranx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'jgsm,proc'} = "LOCATIONOPAC = 'N'";
$codeMaps{'sasa,ranx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'vet,path'} = "LOCATIONOPAC = 'N'";
$codeMaps{'asia'} = "LOCATIONOPAC = 'N'";
$codeMaps{'orni,anx'} = "LOCATIONOPAC = 'N'";
$codeMaps{'vet,comp'} = "LOCATIONOPAC = 'N'";
$codeMaps{'mus,ts'} = "LOCATIONOPAC = 'N'";
$codeMaps{'vet,equ'} = "LOCATIONOPAC = 'N'";
$codeMaps{'olin,res'} = "LOCATIONOPAC = 'N'";
$codeMaps{'asia,ref'} = "LOCATIONOPAC = 'N'";
$codeMaps{'vet,oph'} = "LOCATIONOPAC = 'N'";
$codeMaps{'vet,feli'} = "LOCATIONOPAC = 'N'";
$codeMaps{'rmc,ts'} = "LOCATIONOPAC = 'N'";

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
