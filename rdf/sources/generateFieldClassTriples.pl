#! perl

%classes =
 (
  'ControlFields' => [1..9],
  'DataFields' => [10..999],

  ### BIBLIOGRAPHIC
  'MainEntryAuthor' => [100,110,111],
  'SubjectTermEntry' => [600,610,611,630,648,650,651,653..658,662,690..699],
  'LinkingEntry' => [760,762,765,767,770,772..777,780,785..787],
  'AddedEntry' => [700,710,711,720,730,740,751..754],
  'SeriesAddedEntry' => [800,810,811],

  ### HOLDINGS
  'TextualHoldingsStatementField' => [866..868],
  );

$subproperty = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
$fieldPref = "http://marcrdf.library.cornell.edu/canonical/0.1/hasField";

foreach $class ( keys %classes ) {
    foreach $field ( @{$classes{$class}} ) {
	$classUri = "http://marcrdf.library.cornell.edu/canonical/0.1/".$class;

	printf("<%s%03d> <%s> <%s>.\n",$fieldPref,$field,$subproperty,$classUri);
    }
}

