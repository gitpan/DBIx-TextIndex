package DBIx::TextIndex::TermDocsCache;

use strict;

our $VERSION = '0.17';

use Bit::Vector;
#use DBIx::TextIndex;

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
    delete($self->{TERM_DOCS});
    delete($self->{DOCFREQ_T});
}

sub term_docs_hashref {
    my $self = shift;
    my ($fno, $term) = @_;
    $self->_fetch_term_docs($fno, $term) unless exists $self->{TERM_DOCS}->[$fno]->{$term};
    return DBIx::TextIndex::term_docs_hashref($self->{TERM_DOCS}->[$fno]->{$term});

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

# Same as docfreq_t
sub f_t {
    my $self = shift;
    my ($fno, $term) = @_;
    $self->_fetch_term_docs($fno, $term) unless exists $self->{DOCFREQ_T}->[$fno]->{$term};
    return $self->{DOCFREQ_T}->[$fno]->{$term};
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

sub _term_docs {
    my $self = shift;
    my ($fno, $term) = @_;
 
    my $sql = $self->db_fetch_term_docs($self->{INVERTED_TABLES}->[$fno]);
    my $sth = $self->{DBH}->prepare($sql);
    $sth->execute($term);

    my $docs;
    $sth->bind_col(1, \$docs);
    $sth->fetch;
    return $docs;
}

sub _docfreq_t {
    my $self = shift;
    my ($fno, $term) = @_;
    my $sql = $self->db_docfreq_t($self->{INVERTED_TABLES}->[$fno]);
    my ($docfreq_t) =
	$self->{DBH}->selectrow_array($sql, undef, $term);
    return $docfreq_t;
}

