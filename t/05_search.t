use Test::More tests => 6;

use strict;

BEGIN { 
    use_ok('DBI');
    use_ok('DBIx::TextIndex');
};


$ENV{DBI_DSN} = $ENV{DBI_DSN} || "DBI:mysql:database=test";
my $dbh = DBI->connect();

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

my @top_docs  = (76, 3, 2, 0, 76, 105, 2);

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
