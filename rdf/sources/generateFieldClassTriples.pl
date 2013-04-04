#! perl

%classes =
 (
  'MainEntry' => [100,110,111,130],
  'SubjectTermEntry' => [600,610,611,630,648,650,651,653..658,662,690..699],
  'LinkingEntry' => [760,762,765,767,770,772..777,780,785..787],
  'AddedEntry' => [700,710,711,720,730,740,751..754],
  );

$subproperty = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";

foreach $class ( keys %classes ) {
    foreach $field ( @{$classes{$class}} ) {
	$fieldUri = "http://marcrdf.library.cornell.edu/canonical/0.1/hasField".$field;
	$classUri = "http://marcrdf.library.cornell.edu/canonical/0.1/".$class;

	printf("<%s> <%s> <%s>.\n",$fieldUri,$subproperty,$classUri);
    }
}

