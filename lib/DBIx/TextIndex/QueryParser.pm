package DBIx::TextIndex::QueryParser;

use strict;

our $VERSION = '0.19';

use DBIx::TextIndex::Exception;
use Text::Balanced qw(extract_bracketed extract_delimited);

my $QRY = 'DBIx::TextIndex::Exception::Query';

sub new {
    my $pkg = shift;
    my $class = ref($pkg) || $pkg;
    my $self = bless {}, $class;
    return $self;
}

sub parse {
    my $self = shift;
    my $text = shift;
    my @clauses;

    $text =~ s/\s+$//;

    while ($text) {
	my $clause;

	if ($text =~ s/^\s+//) {
	    next;
	}

	if ($text =~ s/^(AND|OR)\s+//) {
	    $clause->{CONJ} = $1;
	}

	if ($text =~ s/^\+//) {
	    $clause->{MODIFIER} = 'AND';
	} elsif ($text =~ s/^\-//) {
	    $clause->{MODIFIER} = 'NOT';
	} else {
	    $clause->{MODIFIER} = 'OR';
	}

	if ($text =~ m/^\(/) {
	    my ($extract, $remain) = extract_bracketed($text, "(");
	    unless ($extract) {
		# FIXME: hard coded error message
		throw $QRY( error => 'Open and close parentheses are uneven.');
	    }
	    $text = $remain;
	    $extract =~ s/^\(//;
	    $extract =~ s/\)$//;
	    $clause->{TYPE} = 'QUERY';
	    $clause->{QUERY} = $self->parse($extract);
	} elsif ($text =~ m/^\"/) {
	    my ($extract, $remain) = extract_delimited($text, '"');
	    unless ($extract) {
		# FIXME: hard coded error message
		throw $QRY( error => 'Quotes must be used in matching pairs.')
	    }
	    $text = $remain;
	    $extract =~ s/^\"//;
	    $extract =~ s/\"$//;
	    $clause->{TYPE} = 'PHRASE';
	    $clause->{TERM} = $extract;
	    $clause->{PHRASETERMS} = $self->parse($extract);
	} elsif ($text =~ s/^(\S+[\-\&\.\@\'\*]\S+)//) {
	    $clause->{TYPE} = 'IMPLICITPHRASE';
	    $clause->{TERM} = $1;
	    $clause->{PHRASETERMS} =
	     $self->parse(join(' ', split('[\-\&\.\@\'\*]', $clause->{TERM})));
	} elsif ($text =~ s/^(\S+)\?//) {
	    $clause->{TYPE} = 'PLURAL';
	    $clause->{TERM} = $1;
	} elsif ($text =~ s/^(\S+)\*\s*//) {
	    $clause->{TYPE} = 'WILD';
	    $clause->{TERM} = $1;
	} else {
	    $text =~ s/(\S+)//;
	    $clause->{TYPE} = 'TERM';
	    $clause->{TERM} = $1;
	}
	push @clauses, $clause;
    }
    return \@clauses;
}

1;
