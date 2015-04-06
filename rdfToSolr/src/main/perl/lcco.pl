#! /bin/perl
# Frances Webb (frances.webb@cornell.edu) April 2015, CUL
#
# This script populates a MySQL database table keyed for efficient hierarchical queries
# with Library of Congress call number classification data as publicly published at:
#  http://www.loc.gov/catdir/cpso/lcco/
# The script should probably be able to create the table and/or empty is as needed.
#
#CREATE TABLE `classification` (
#  `low_letters` char(3) NOT NULL,
#  `high_letters` char(3) NOT NULL,
#  `low_numbers` float(9,4) NOT NULL,
#  `high_numbers` float(9,4) NOT NULL,
#  `label` varchar(256) character set utf8 NOT NULL,
#  KEY `low_letters` (`low_letters`,`high_letters`,`low_numbers`,`high_numbers`)
#) ENGINE=MyISAM DEFAULT CHARSET=latin1


use DBI();
use strict;
use constant CLASS_MAX => 100000;
use constant OTHER_MAX =>  99999;

my $dsn = "DBI:mysql:database=classifications;mysql_read_default_file=~/.my.cnf";
my $dbh = DBI->connect($dsn);

my $insert_sth = $dbh->prepare
    ("INSERT INTO classification
                 (low_letters, high_letters, low_numbers, high_numbers, label)
      VALUES (?, ?, ?, ?, ?)");

open my $fh, "<", "../resources/lcco.txt";
while (my $line = <$fh>) {
#    print $line;
    chomp $line;
    $line =~ s/^(.*):\s*(.*)$/$1 \($2\)/;
    if ($line =~ /^CLASS/) {
	$line =~ s/CLASS //;
	process_class($line);
    } elsif ($line =~ /^Subclass/) {
	$line =~ s/Subclass(es)? //;
	process_line($line);
    } elsif (($line !~ /OUTLINE/)
	     and ($line !~ m/^\s/)) {
	process_line($line);
    }
   
}
close $fh;

sub process_line {
    my $line = shift;
    my ($field, $value) = split /\s+/, $line, 2;
    $value =~ s/^- //;
    $field =~ s/[\(\)]//g;
    my ($left, $right) = split /-/, $field, 2;
    my ($leftletters, $rightletters, $leftnumbers, $rightnumbers);
    $rightnumbers = 0;
    if ($left =~ /^[A-Z]+$/) {
	$leftletters = $left;
	$leftnumbers = 0;
    } elsif ($left =~ /^([A-Z]{1,3})([0-9\.]+)$/) {
	$leftletters = $1;
	$leftnumbers = $2;
    } else {
	print "$left todo\n";
	################
	return;
    }
    if (defined $right) {
	if ($right =~ /^[A-Z]+$/) {
	    $rightletters = $right;
	    $rightnumbers = OTHER_MAX;
	} elsif ($right =~ /^([A-Z]{1,3})([0-9\.]+)$/) {
	    $rightletters = $1;
	    $rightnumbers = $2;
	} elsif ($right =~ /^[0-9\.]+$/) {
	    $rightletters = $leftletters;
	    $rightnumbers = $right;
	} else {
	    print "$right todo\n";
	    ##############
	    return;
	}
    } else {
	$rightletters = $leftletters;
	if ($leftnumbers == 0) {
	    $rightnumbers = OTHER_MAX;
	} else {
	    $rightnumbers = $leftnumbers;
	}
    }
    if ($rightnumbers and $leftletters) {
	if ($rightnumbers !~ /\./) {
	    $rightnumbers .= ".9999";
	}
	$insert_sth->execute($leftletters,$rightletters,
			     $leftnumbers,$rightnumbers,
			     $field." - ".$value);
    }
}

sub process_class {
    my $class = shift;
    my ($field, $value) = split / - /,$class,2;
    my ($left, $right) = split /-/, $field;
    if (defined $right) {
	$right .= "ZZ";
    } else {
	$right = $left."ZZ";
    }
    $insert_sth->execute($left,$right,0,CLASS_MAX,$class);
  
}

sub z_fill {
    my $str = shift;
    if (length $str == 3) {
	return $str;
    } elsif (length $str == 2) {
	return $str.'Z';
    } elsif (length $str == 1) {
	return $str.'ZZ';
    } else {
	print "Illegal letter range $str\n";
	exit;
    }
}
