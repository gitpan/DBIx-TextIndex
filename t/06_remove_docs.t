use Test::More tests => 6;

use strict;

our $TESTDATA = 'testdata/encantadas.txt';

BEGIN { 
    use_ok('DBI');
    use_ok('DBIx::TextIndex');
};


$ENV{DBI_DSN} = $ENV{DBI_DSN} || "DBI:mysql:database=test";
my $dsn = $ENV{DBI_DSN};
my $dbh = DBI->connect($dsn, undef, undef, { RaiseError => 1, PrintError => 0, AutoCommit => 0, ShowErrorStatement => 1 });

ok( $dbh && $dbh->ping );

my ($max_doc_id) = $dbh->selectrow_array(qq(SELECT MAX(doc_id) FROM textindex_doc));

ok( $max_doc_id == 226 );

my $index = DBIx::TextIndex->new({
    doc_dbh => $dbh,
    index_dbh => $dbh,
    collection => 'encantadas',
});

ok( ref $index eq 'DBIx::TextIndex' );

$index->remove_doc(2,3,76,105);

my $results;

my @top_docs  = (98, 0, 0, 0, 98, 0, 0);

my @terms = ('isle',
	     'greedy',
	     'ferryman',
	     'aardvark',
	     '+isle',
	     '"captain he said"',
	     'unweeting hap fordonne isle');

my @result_docs;

foreach my $term (@terms) {
    my $top_doc;
    eval {
	$results = $index->search({ doc => $term });
    };
    if ($@) {
	if (ref $@ && $@->isa('DBIx::TextIndex::Exception::Query') ) {
	    $top_doc = 0;
	} else {
	    die $@;
	}
    } else {
	my @results;
	foreach my $doc_id (sort {$results->{$b} <=> $results->{$a}} keys %$results) {
	    push @results, $doc_id;
	}
	$top_doc = $results[0];
    }
    push @result_docs, $top_doc;
}

is_deeply(\@result_docs, \@top_docs);


$dbh->disconnect;
