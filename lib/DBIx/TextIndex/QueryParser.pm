package DBIx::TextIndex::QueryParser;

use strict;

our $VERSION = '0.22';

use base qw(DBIx::TextIndex);

use DBIx::TextIndex::Exception;
use Text::Balanced qw(extract_bracketed extract_delimited);

my $QRY = 'DBIx::TextIndex::Exception::Query';

sub new {
    my $pkg = shift;
    my $class = ref($pkg) || $pkg;
    my $self = bless {}, $class;
    my $args = shift || {};
    foreach my $field (keys %$args) {
	$self->{uc($field)} = $args->{$field};
    }
    return $self;
}

sub term_fields {
    my $self = shift;
    return sort { $a cmp $b } keys %{$self->{TERM_FIELDS}};
}

sub parse {
    my $self = shift;
    delete($self->{TERM_FIELDS});
    $self->_parse(@_);
}

sub _parse {
    my $self = shift;
    my $q = shift;
    my @clauses;
    my $term;

    $q =~ s/\s+$//;

    while ($q) {
	my $clause;

	if ($q =~ s/^\s+//) {
	    next;
	}

	if ($q =~ s/^(AND|OR)\s+//) {
	    $clause->{CONJ} = $1;
	}

	if ($q =~ s/^\+//) {
	    $clause->{MODIFIER} = 'AND';
	} elsif ($q =~ s/^\-//) {
	    $clause->{MODIFIER} = 'NOT';
	} else {
	    $clause->{MODIFIER} = 'OR';
	}

	if ($q =~ s/^(\w+)://) {
	    $clause->{FIELD} = $1;
	    $self->{TERM_FIELDS}->{$clause->{FIELD}}++;
	} else {
	    $self->{TERM_FIELDS}->{__DEFAULT}++;
	}

	if ($q =~ m/^\(/) {
	    my ($extract, $remain) = extract_bracketed($q, "(");
	    unless ($extract) {
		# FIXME: hard coded error message
		throw $QRY( error => 'Open and close parentheses are uneven.');
	    }
	    $q = $remain;
	    $extract =~ s/^\(//;
	    $extract =~ s/\)$//;
	    $clause->{TYPE} = 'QUERY';
	    $clause->{QUERY} = $self->_parse($extract);
	} elsif ($q =~ m/^\"/) {
	    my ($extract, $remain) = extract_delimited($q, '"');
	    unless ($extract) {
		# FIXME: hard coded error message
		throw $QRY( error => 'Quotes must be used in matching pairs.')
	    }
	    $q = $remain;
	    $extract =~ s/^\"//;
	    $extract =~ s/\"$//;
	    $clause->{TYPE} = 'PHRASE';
	    $term = $extract;
	    $clause->{PHRASETERMS} = $self->_parse($extract);
	    if ($q =~ s/^~(\d+)//) {
		$clause->{PROXIMITY} = $1;
	    } else {
		$clause->{PROXIMITY} = 1;
	    }
	} elsif ($q =~ s/^(\S+[\-\&\.\@\'\*]\S+)//) {
	    $clause->{TYPE} = 'IMPLICITPHRASE';
	    $term = $1;
	    $clause->{PHRASETERMS} =
	     $self->_parse(join(' ', split('[\-\&\.\@\'\*]',$term)));
	} elsif ($q =~ s/^(\S+)\?//) {
	    $clause->{TYPE} = 'PLURAL';
	    $term = $1;
	} elsif ($q =~ s/^(\S+)\*\s*//) {
	    $clause->{TYPE} = 'WILD';
	    $term = $1;
	} else {
	    $q =~ s/(\S+)//;
	    $clause->{TYPE} = 'TERM';
	    $term = $1;
	}
	$clause->{TERM} = $self->_lc_and_unac($term) if $term;
	if ($clause->{TERM}) {
	    next unless $clause->{TERM} =~ m/[a-z0-9]/;
	}
	push @clauses, $clause;
    }
    my $folded = fold_nested_phrases(\@clauses);
    return $folded;
}

sub fold_nested_phrases {
    my $clauses = shift;
    my @folded;
    foreach my $clause (@$clauses) {
	if ($clause->{TYPE} eq 'PHRASE' ||
	    $clause->{TYPE} eq 'IMPLICITPHRASE') {
	    my @folded_terms;
	    foreach my $phraseterm (@{$clause->{PHRASETERMS}}) {
		if ($phraseterm->{TYPE} eq 'IMPLICITPHRASE') {
		    push @folded_terms,
		        fold_nested_phrases($phraseterm->{PHRASETERMS});
		} else {
		    push @folded_terms, $phraseterm;
		}
	    }
	    $clause->{PHRASETERMS} = \@folded_terms;
	}
	push @folded, $clause;
    }
    return wantarray ? @folded : \@folded;
}

1;
