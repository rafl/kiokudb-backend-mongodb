package KiokuDB::Backend::MongoDB;

use Moose;
use MongoDB;

use namespace::clean -except => 'meta';

with qw/
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::MongoDB
    KiokuDB::Backend::Role::Clear
/;

has [qw/database_name collection_name/] => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has collection => (
    is => 'ro',
    isa => 'MongoDB::Collection',
    lazy => 1,
    builder => '_build_collection',
);

sub _build_collection {
    my ($self) = @_;
    my $conn = MongoDB::Connection->new;
    return $conn->get_database($self->database_name)->get_collection($self->collection_name);
}

sub BUILD {
    my ($self) = @_;
    $self->collection;
}

sub insert {
    my ($self, @entries) = @_;
    for my $entry (@entries) {
        $self->insert_entry($entry);
    }
    return;
}

sub insert_entry {
    my ($self, $entry) = @_;
    $self->collection->insert($self->serialize($entry));
    return;
}

sub get {
    my ($self, @ids) = @_;
    return map {
        $self->get_entry($_)
    } @ids;
}

sub get_entry {
    my ($self, $id) = @_;
    my $obj = $self->collection->find_one({ kiokuid => $id });
    return undef unless $obj;
    return $self->deserialize($obj);
}

sub exists {
    my ($self, @ids) = @_;
    return map {
        $self->collection->count({ kiokuid => $_ }) ? 1 : 0
    } @ids;
}

sub delete {
    my ($self, @ids_or_entries) = @_;
    for my $id (map { $_->isa('KiokuDB::Entry') ? $_->id : $_ } @ids_or_entries) {
        $self->collection->remove({ kiokuid => $id });
    }
    return;
}

sub clear {
    my ($self) = @_;
    $self->collection->drop;
}

__PACKAGE__->meta->make_immutable;

1;
