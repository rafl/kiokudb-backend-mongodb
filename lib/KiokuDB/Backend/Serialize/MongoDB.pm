package KiokuDB::Backend::Serialize::MongoDB;

use Moose::Role;
use MongoDB::OID;
use Data::Visitor::Callback;

use namespace::clean -except => 'meta';

with qw/KiokuDB::Backend::Serialize/;

has serialize_visitor => (
    is      => 'ro',
    isa     => 'Data::Visitor',
    builder => '_build_serialize_visitor',
);

has deserialize_visitor => (
    is      => 'ro',
    isa     => 'Data::Visitor',
    builder => '_build_deserialize_visitor',
);

sub _build_serialize_visitor {
    my ($self) = @_;
    return Data::Visitor::Callback->new(
        object => sub {
            my ($self, $object) = @_;
            if ($object->isa('KiokuDB::Reference')) {
                return { kiokuidref => $object->id };
            }
            elsif ($object->isa('KiokuDB::Entry')) {
                return {
                    kiokuid => $object->id,
                    class   => $object->class,
                    data    => { fake => 'data' },
                    data    => $self->visit_ref($object->data),
                };
            }
            else {
                confess 'OH NOES';
            }
        },
    );
}

sub _build_deserialize_visitor {
    my ($self) = @_;
    return Data::Visitor::Callback->new(
        hash => sub {
            my ($self, $hash) = @_;
            if (exists $hash->{kiokuidref}) {
                return KiokuDB::Reference->new(id => $hash->{kiokuidref});
            }
            elsif (exists $hash->{kiokuid}) {
                return KiokuDB::Entry->new(
                    id    => $hash->{kiokuid},
                    data  => $hash->{data},
                    class => $hash->{class},
                );
            }
            else {
                return $hash;
            }
        },
    );
}

sub serialize {
    my ($self, $entry) = @_;
    return $self->serialize_visitor->visit($entry);
}

sub deserialize {
    my ($self, $data) = @_;
    return $self->deserialize_visitor->visit($data);
}

1;
