use strict;

use Test::More;
use DBI;
use DBIx::TextIndex;

if (defined $ENV{DBI_DSN}) {
    plan tests => 13;
} else {
    plan skip_all => '$ENV{DBI_DSN} must be defined to run tests.';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS}, { RaiseError => 1, PrintError => 0, AutoCommit => 1 });

ok( defined $dbh && $dbh->ping );

my ($max_doc_id) = $dbh->selectrow_array(qq(SELECT MAX(doc_id) FROM textindex_doc));

ok( $max_doc_id == 226 );

my $index = DBIx::TextIndex->new({
    doc_dbh => $dbh,
    doc_table => 'textindex_doc',
    doc_fields => ['doc'],
    doc_id_field => 'doc_id',
    index_dbh => $dbh,
    collection => 'encantadas',
    update_commit_interval => 15,
    proximity_index => 1,
});

ok( ref $index eq 'DBIx::TextIndex' );

ok( ref($index->initialize) eq 'DBIx::TextIndex' );

ok( $index->add_doc(1) == 1 );
ok( $index->add_document(2, 3, 4) == 3 );
ok( $index->add_doc([5 .. 100]) == 96 );
ok( $index->add_doc([101 .. $max_doc_id]) == 126 );

ok( $index->indexed(1) );
ok( $index->indexed(100) );
ok( $index->indexed(226) );

ok( $index->last_indexed_key == 226 );

is_deeply( [ $index->all_doc_ids ], [1 .. 226] );

$dbh->disconnect;
