use Test::More tests => 11;

use strict;

our $TESTDATA = 'testdata/encantadas.txt';

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
    doc_table => 'textindex_doc',
    doc_fields => ['doc'],
    doc_id_field => 'doc_id',
    index_dbh => $dbh,
    collection => 'encantadas',
});

ok( ref $index eq 'DBIx::TextIndex' );

ok( ref($index->initialize) eq 'DBIx::TextIndex' );

ok( $index->add_doc(1) == 1 );
ok( $index->add_document(2, 3, 4) == 3 );
ok( $index->add_doc([5 .. 100]) == 96 );
ok( $index->add_doc([101 .. $max_doc_id]) == 126 );

is_deeply( [ $index->all_doc_ids ], [1 .. 226] );

$dbh->disconnect;
