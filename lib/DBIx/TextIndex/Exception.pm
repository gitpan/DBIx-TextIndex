package DBIx::TextIndex::Exception;

use strict;

our $VERSION = '0.16';

use Exception::Class (
  'DBIx::TextIndex::Exception',

  'DBIx::TextIndex::Exception::DataAccess' =>
  { isa => 'DBIx::TextIndex::Exception',
    fields => [ 'syserr' ] },

  'DBIx::TextIndex::Exception::General' =>
  { isa => 'DBIx::TextIndex::Exception',
    fields => [ 'syserr' ] },

  'DBIx::TextIndex::Exception::Query' =>
  { isa => 'DBIx::TextIndex::Exception',
    fields => [ 'syserr' ] },
);


1;
