package DBIx::TextIndex::TermDocsCache;

use strict;

our $VERSION = '0.15';

use Bit::Vector;
use DBIx::TextIndex;

sub new {
    my $pkg = shift;
    my $class = ref($pkg) || $pkg;
    my $self = bless {}, $class;
    $self->_init(shift);
    my $db = 'DBIx/TextIndex/' . $self->{DB} . '.pm';
    do $db;

    return $self;
}

sub _init {
    my $self = shift;
    my $args = shift;
    while (my ($k, $v) = each %$args) {
	$self->{uc $k} = $v;
    }
}

sub max_indexed_id {
    my $self = shift;
    if (@_) {
	$self->flush;
	$self->{MAX_INDEXED_ID} = $_[0];
    }
    return $self->{MAX_INDEXED_ID};
}

sub flush {
    my $self = shift;
    $self->{TERM_DOCS} = ();
    $self->{DOCFREQ_T} = ();
}

sub term_docs_arrayref {
    my $self = shift;
    my ($fno, $term) = @_;
    $self->_fetch_term_docs($fno, $term) unless exists $self->{TERM_DOCS}->[$fno]->{$term};
    return DBIx::TextIndex::term_docs_arrayref($self->{TERM_DOCS}->[$fno]->{$term});
}

sub term_doc_ids_arrayref {
    my $self = shift;
    my ($fno, $term) = @_;
    $self->_fetch_term_docs($fno, $term) unless exists $self->{TERM_DOCS}->[$fno]->{$term};
    return DBIx::TextIndex::term_doc_ids_arrayref($self->{TERM_DOCS}->[$fno]->{$term});
}

sub vector {
    my $self = shift;
    my $doc_ids = $self->term_doc_ids_arrayref(@_);
    my $vector = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);
    $vector->Index_List_Store(@$doc_ids);
    return $vector;
}

sub docfreq_t {
    my $self = shift;
    my ($fno, $term) = @_;
    $self->_fetch_term_docs($fno, $term) unless exists $self->{DOCFREQ_T}->[$fno]->{$term};
    return $self->{DOCFREQ_T}->[$fno]->{$term};
}

sub _fetch_term_docs {
    my $self = shift;
    my ($fno, $term) = @_;
    my $sql =
	$self->db_fetch_term_freq_and_docs($self->{INVERTED_TABLES}->[$fno]);

    ($self->{DOCFREQ_T}->[$fno]->{$term}, $self->{TERM_DOCS}->[$fno]->{$term})
	= $self->{DBH}->selectrow_array($sql, undef, $term);
}
