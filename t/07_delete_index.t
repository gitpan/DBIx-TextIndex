use strict;

use Test::More;
use DBI;
use DBIx::TextIndex;

if (defined $ENV{DBI_DSN}) {
    plan tests => 2;
} else {
    plan skip_all => '$ENV{DBI_DSN} must be defined to run tests.';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS}, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

ok( defined $dbh && $dbh->ping );

my $index = DBIx::TextIndex->new({
    doc_dbh => $dbh,
    index_dbh => $dbh,
    collection => 'encantadas',
});

ok( ref $index eq 'DBIx::TextIndex' );

$index->delete;

$dbh->disconnect;
