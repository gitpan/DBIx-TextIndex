use Test::More tests => 7;

use strict;

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

my @result_docs_tfidf;
my @result_docs_okapi;

foreach my $term (@terms) {
    my $top_doc;
    eval {
	$results = $index->search({ doc => $term },
				  { scoring_method => 'legacy_tfidf' });
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
    push @result_docs_tfidf, $top_doc;
}

is_deeply(\@result_docs_tfidf, \@top_docs);

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
