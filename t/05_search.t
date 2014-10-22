use strict;

use Test::More;
use DBI;
use DBIx::TextIndex;

if (defined $ENV{DBI_DSN}) {
    plan tests => 4;
} else {
    plan skip_all => '$ENV{DBI_DSN} must be defined to run tests.';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS}, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

ok( defined $dbh && $dbh->ping );

my ($max_doc_id) = $dbh->selectrow_array(qq(SELECT MAX(doc_id) FROM textindex_doc));

ok( $max_doc_id == 226 );

my $index = DBIx::TextIndex->new({
    doc_dbh => $dbh,
    index_dbh => $dbh,
    collection => 'encantadas',
});

ok( ref $index eq 'DBIx::TextIndex' );

my $results;

my @top_docs  = (76, 3, 2, 0, 76, 105, 2, 2, 2, 13, 0, 13, 6, 6, 6);

my @terms = ('isle',
	     'greedy',
	     'ferryman',
	     'aardvark',
	     '+isle',
	     '"captain he said"',
	     'unweeting hap fordonne isle',
	     'unweet*',
             'plot?',
	     '"light winds"~3',
	     '"light winds"~2',
             '"LIGHT WINDS"~3',
	     '"Lake Erie"~1',
	     '"LAKE ERIE"~1',
	     '"lake erie"~1',
             );	

my @result_docs_okapi;

foreach my $term (@terms) {
    my $top_doc;
    eval {
	$results = $index->search({ doc => $term });
    };
    if ($@) {
	if (ref $@ && $@->isa('DBIx::TextIndex::Exception::Query') ) {
	    $top_doc = 0;
	} else {
	    die $@ . "\n\n" . $@->trace;
	}
    } else {
	my @results;
	foreach my $doc_id (sort {$results->{$b} <=> $results->{$a}} keys %$results) {
	    push @results, $doc_id;
	}
	$top_doc = $results[0];
    }
    push @result_docs_okapi, $top_doc;
}

is_deeply(\@result_docs_okapi, \@top_docs);



$dbh->disconnect;
