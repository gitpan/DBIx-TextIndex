use Test::More tests => 4;

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

my $index = DBIx::TextIndex->new({
    doc_dbh => $dbh,
    index_dbh => $dbh,
    collection => 'encantadas',
});

ok( ref $index eq 'DBIx::TextIndex' );

$index->delete;

$dbh->disconnect;
