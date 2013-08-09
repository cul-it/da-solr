#! /bin/perl -i

use Data::Dumper;
my %genres;
my %genres_by_field;
my @genre_sets;

foreach my $file (@ARGV) {
  open my $fh, "< $file";
#  my $first = 1;
  while (<$fh>) {
    chomp;
    my ($id, $i1, $i2, $field) = split /\t/,$_,4;
    my ($fieldtype,$value)  = split / /, $field,2;
    $value =~ s/[\s\.]*$//;
    $value =~ s/^\s*//;
    $genres{$value}++;
#    print $fieldtype," ** ",$value,"\n";
  }
  close $fh;
}
#  print Dumper(\%genres);

for (sort {$genres{$b} <=> $genres{$a} } keys %genres) {
    print $_, ": ",$genres{$_},"\n";
}
