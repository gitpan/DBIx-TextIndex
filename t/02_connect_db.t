use Test::More tests => 3;

use strict;

BEGIN { 
    use_ok('DBI');
    use_ok('DBIx::TextIndex');
};

$ENV{DBI_DSN} = $ENV{DBI_DSN} || "DBI:mysql:database=test";
my $dbh = DBI->connect();

ok($dbh && $dbh->ping);
