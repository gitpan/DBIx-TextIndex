use Test::More tests => 2;

use strict;

BEGIN { use_ok('DBIx::TextIndex') };

is(DBIx::TextIndex->VERSION, 0.13);
