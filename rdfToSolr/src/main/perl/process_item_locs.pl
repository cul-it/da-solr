#! /usr/bin/perl

use DBI();

open my $fh, "<", "item_locations.csv"
  or die $!;

my $dsn = "DBI:mysql:database=workids;mysql_read_default_file=~/.my.cnf";
my $dbh = DBI->connect($dsn);

my $sth_online = $dbh->prepare("INSERT INTO voy2loc (bibid, format, online) VALUES (?, ?, ?)");
my $sth_library = $dbh->prepare("INSERT INTO voy2loc (bibid, format, library) VALUES (?, ?, ?)");
my $sth_both = $dbh->prepare("INSERT INTO voy2loc (bibid, format, online, library) VALUES (?, ?, ?, ?)");
my $sth_neither = $dbh->prepare("INSERT INTO voy2loc (bibid, format) VALUES (?, ?)");


my $headRow = <$fh>;
while (my $row = <$fh>) {
  chomp $row;
  my @vals = csvsplit( $row );
  my %bib;
  $bib{"id"} = $vals[0];
  $bib{"form"} = $vals[4];
  my $online = $vals[1];
  my %online;
  for my $loc ( split /,/, $online ) {
    $online{$loc}++;
  }
  if (defined $online{"At the Library"}) {
    my %libe;
    for my $libe ( split /,/, $vals[2] ) {
      $libe{$libe}++;
    }
    $bib{"library"} = sprintf( "%s: %s", "At the Library", join ", ", keys %libe );
  }
  if (defined $online{"Online"}) {
    my %vendor;
    for my $link ( split /,/, $vals[3] ) {
      if ($link =~ /U.S. Government Printing Office/) {
	$vendor{"U.S. Government Printing Office"}++;
      } elsif ($link =~ /SpringerLink/i) {
	$vendor{"SpringerLink"}++;
      } elsif ($link =~ /.safaribooksonline.com/) {
	$vendor{"Safari Books Online"}++;
      } elsif ($link =~ /NetLibrary/) {
	$vendor{"NetLibrary"}++;
      } elsif ($link =~ /naxosmusiclibrary.com/) {
	$vendor{"Naxos Music Library"}++;
      } elsif ($link =~ /set=Books24x7/) {
	$vendor{"Books24x7"}++;
      } elsif ($link =~ /www.osti.gov/) {
	$vendor{"U.S. Dept. of Energy"}++;
      } elsif ($link =~ /ebscohost.com/) {
	$vendor{"EBSCO Publishing"}++;
      } elsif ($link =~ /www.nber.org/) {
	$vendor{"Natl. Bureau of Econ. Research"}++;
      } elsif ($link =~ /\.(gpo|fldp|fdlp)\.gov/) {
        $vendor{"U.S. Government Printing Office"}++;
      } elsif ($link =~ /LexisNexis/) {
	$vendor{"LexisNexis"}++;
      } elsif ($link =~ /Brepols Miscellanea/) {
	$vendor{"Brepols Miscellanea"}++;
      } elsif ($link =~ /HighWire Press/) {
	$vendor{"HighWire Press"}++;
      } elsif ($link =~ /MIT CogNet/) {
	$vendor{"MIT CogNet"}++;
      } elsif ($link =~ /\.ebrary\.com/) {
	$vendor{"Ebrary"}++;
      } elsif ($link =~ /HathiTrust/) {
	$vendor{"HathiTrust"}++;
      } elsif ($link =~ /OECD iLibrary/) {
	$vendor{"OECD iLibrary"}++;
      } elsif ($link =~ /National Academies Press/) {
	$vendor{"National Academies Press"}++;
      } elsif ($link =~ /www\.archive\.org/) {
	$vendor{"Internet Archive (archive.org)"}++;
      } elsif ($link =~ /Early English books/i) {
	$vendor{"Early English Books Online"}++;
      } elsif ($link =~ /Business Insights/) {
	$vendor{"Business Insights"}++;
      } elsif ($link =~ /Factiva/) {
	$vendor{"Factiva"}++;
      } elsif ($link =~ /Business Source Complete/) {
	$vendor{"Business Source Complete"}++;
      } elsif ($link =~ /ABI\/INFORM/) {
	$vendor{"ABI/INFORM"}++;
      } elsif ($link =~ /spiedigitallibrary\.org/) {
	$vendor{"SPIE Digital Library"}++;
      } elsif ($link =~ /www\.rkma\.com/) {
	$vendor{"RKMA Publications"}++;
      } elsif ($link =~ /\.myilibrary\.com/) {
	$vendor{"MyiLibrary"}++;
      } elsif ($link =~ /cornell.lib.overdrive.com/) {
	$vendor{"OverDrive"}++;
      } elsif ($link =~ /www\.aspresolver\.com/) {
	$vendor{"Alexander Street Press"}++;
      } elsif ($link =~ /\.galegroup\.com/) {
	$vendor{"Gale CEngage Learning"}++;
      } elsif ($link =~ /ieeexplore.ieee.org/) {
	$vendor{"IEEE/IET"}++;
      } elsif ($link =~ /\.knovel\.com/) {
	$vendor{"Knovel"}++;
      } elsif ($link =~ /www\.heinonline\.org/) {
	$vendor{"HeinOnline"}++;
      } elsif ($link =~ /Thieme/) {
	$vendor{"Thieme"}++;
      } elsif ($link =~ /discover\.pli\.edu/) {
        $vendor{"Practising Law Institute"}++;
      } elsif ($link =~ /\.proquest\.com/) {
        $vendor{"ProQuest"}++;
      } elsif ($link =~ /ACLS Humanities/) {
        $vendor{"ACLS Humanities"}++;
      } elsif ($link =~ /www\.davidrumsey\.com/) {
        $vendor{"David Rumsey Collection"}++;
      } elsif ($link =~ /crcnetbase\.com/) {
        $vendor{"CRCnetBASE"}++;
      } elsif ($link =~ /\.jstor\.org/) {
	$vendor{"JSTOR"}++;
      }
    }

    if (scalar keys %vendor) {
      $bib{"online"} = sprintf( "%s: %s", "Online", join ",", keys %vendor );
    } else {
      $bib{"online"} = "Online";
    }
  }

  if (defined $bib{"online"}) {
    if (defined $bib{"library"}) {
      $sth_both->execute($bib{"id"},$bib{"form"},$bib{"online"},$bib{"library"});
    } else {
      $sth_online->execute($bib{"id"},$bib{"form"},$bib{"online"});
    }
  } else {
    if (defined $bib{"library"}) {
      $sth_library->execute($bib{"id"},$bib{"form"},$bib{"library"});
    } else {
      $sth_neither->execute($bib{"id"},$bib{"form"});
    }
  }
}


# http://stackoverflow.com/questions/3065095/how-do-i-efficiently-parse-a-csv-file-in-perl
sub csvsplit {
 my $line = shift;
 my $sep = (shift or ',');

 return () unless $line;

 my @cells;
 $line =~ s/\r?\n$//;

 my $re = qr/(?:^|$sep)(?:"([^"]*)"|([^$sep]*))/;

 while($line =~ /$re/g) {
   my $value = defined $1 ? $1 : $2;
   push @cells, (defined $value ? $value : '');
 }

  return @cells;
}
