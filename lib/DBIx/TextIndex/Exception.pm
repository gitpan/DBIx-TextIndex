package DBIx::TextIndex::Exception;

use strict;

our $VERSION = '0.16';

use Exception::Class (
  'DBIx::TextIndex::Exception',

  'DBIx::TextIndex::Exception::Fatal' =>
  { isa => 'DBIx::TextIndex::Exception',
    fields => [ 'detail' ] },

  'DBIx::TextIndex::Exception::Fatal::General' =>
  { isa => 'DBIx::TextIndex::Exception::Fatal',
    fields => [ 'detail' ],
    alias => 'throw_gen' },

  'DBIx::TextIndex::Exception::Query' =>
  { isa => 'DBIx::TextIndex::Exception',
    fields => [ 'detail' ],
    alias => 'throw_query' },
);

require Exporter;
*import = \&Exporter::import;

our @EXPORT_OK = qw(throw_gen throw_query);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

1;
