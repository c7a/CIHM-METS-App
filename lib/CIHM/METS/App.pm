package CIHM::METS::App;

use MooseX::App;
use Data::Dumper;
use Try::Tiny;
use Module::Load::Conditional qw[can_load check_install requires];

=head1 NAME

CIHM::METS::App - Command-line tool for generating/manipulating components of METS records.

=head1 VERSION

Version 0.03

=cut

our $VERSION = '0.03';

=head1 AUTHOR

Sascha Adler, C<< <sascha.adler at canadiana.ca> >>
Russell McOrmond, C<< <russell.mcormond at canadiana.ca> >>
Julienne Pascoe, C<< <julienne.pascoe at canadiana.ca> >>

=head1 BUGS

Please report any bugs or feature requests through the web interface at
L<https://github.com/c7a/CIHM-METS-App>.  We will be notified, and then you'll
automatically be notified of progress on your bug as we make changes.

=cut

option 'dump' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Dump information to console rather than send to CouchDB],
);

has 'WIP' => (
    is => 'rw',
    lazy => 1,
    builder => '_build_WIP',
);

sub _build_WIP {
    my $self = shift;

    my $WIP;
    if (!$self->dump && can_load(modules => {
        'CIHM::WIP' => undef
                 })) {
        $WIP = CIHM::WIP->new($self->conf);
    }
    if (!$WIP) {
        warn "Not using CIHM::WIP\n";
    }
    return $WIP;
};

has 'objid' => (
    is => 'rw',
    isa => 'Str'
);

has 'myconfig' => (
    is => 'rw',
    isa => 'HashRef'
);

has 'identifier' => (
    is => 'rw',
    isa => 'Str'
);

option 'conf' => (
  is => 'rw',
  isa => 'Str',
  default => "/etc/canadiana/wip/wip.conf",
  documentation => q[An option that specifies where you can find a config file if not default],
);

parameter 'configid' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The configuration ID (Example: heritage)],
);

option 'depositor' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[The depositor (Example: oocihm)],
);

sub set_objid {
    my ($self,$identifier) = @_;

    #trim whitespace
    $identifier =~ s/^\s+|\s+$//g;

    # If we have CIHM::WIP
    if ($self->WIP) {
        my $objid=$self->WIP->i2objid($identifier,$self->configid);
        if (!$self->WIP->objid_valid($objid)) {
            die "$objid not valid OBJID\n";
        }
        $self->objid($objid);
    } else {
        # Set identifier as objectID as we can't use translation without WIP
        $self->objid($identifier);
    }
}


sub setup {
    my $self = shift;

    # If we have CIHM::WIP
    if ($self->WIP) {
        if (!$self->depositor) {
            my $configdocs=$self->WIP->configdocs ||
                die "Can't retrieve configuration documents\n";

            my $myconfig=$configdocs->{$self->configid} ||
                die $self->configid." is not a valid configuration id\n";

            $self->myconfig($myconfig);

            my $depositor=$myconfig->{depositor} ||
                die "Depositor not set for ".$self->configid."\n";

            $self->depositor($depositor);
        }
    }
    if ($self->identifier) {
        $self->set_objid($self->identifier);
    }
    if (!$self->depositor) {
        die "--depositor not set (and not available from configid)\n";
    }
    
}

sub itemlabel {
    my $self = shift;

    if ($self->myconfig) {
        return $self->myconfig->{itemlabel};
    }
}


sub dataDump {
    my ($self,$uid,$params) = @_;

    print Data::Dumper->Dump([$params], [$uid]);
}

sub couchSend {
    my ($self,$params) = @_;

    my $uid=join('.',$self->depositor,$self->objid);


    if (!$self->WIP) {
        return $self->dataDump($uid,$params);
    }

    my ($res, $revision);

    my $wipmeta=$self->WIP->wipmeta;
    my $updatedoc={};
    if ($params->{label}) {
        $updatedoc->{label}=$params->{label};
    }

#    print "couchSend: " . Dumper($params)."\n";

    # This encoding makes variables available as form data
    $wipmeta->type("application/x-www-form-urlencoded");
    $res = $wipmeta->post("/".$wipmeta->{database}."/_design/tdr/_update/basic/".$uid, $updatedoc, {deserializer => 'application/json'});

    if ($res->code != 201 && $res->code != 200) {
        die "_update/basic/$uid POST return code: " . $res->code . "\n";
    }
    $revision=$res->response->header('X-Couch-Update-Newrev');


    if ($params->{clabels} || $params->{dmdsec} || $params->{metadata}) {
        if (!$revision) {
            $res = $wipmeta->head("/".$wipmeta->database."/$uid",{}, {deserializer => 'application/json'});
            if ($res->code == 200) {
                $revision=$res->response->header("etag");
                $revision =~ s/^\"|\"$//g
            } else {
                die "couchSend($uid) HEAD return code: ".$res->code."\n"; 
            }
        }
        if ($params->{clabels}) {
            $revision=$self->put_attachment($uid,$revision,'labels.json','application/json',$params->{clabels});
        }
        if ($params->{dmdsec}) {
            $revision=$self->put_attachment($uid,$revision,'dmd.xml','application/xml',$params->{dmdsec});
        }
        if ($params->{metadata}) {
            $revision=$self->put_attachment($uid,$revision,'metadata.xml','application/xml',$params->{metadata});
        }
    }
}

sub put_attachment {
    my ($self,$uid,$revision,$name,$type,$data) = @_;
    my $wipmeta=$self->WIP->wipmeta;

    $wipmeta->clear_headers;
    $wipmeta->set_header('If-Match' => $revision) if $revision;
    $wipmeta->type($type);
    my $res=$wipmeta->put("/".$wipmeta->database."/$uid/$name",$data, {deserializer => 'application/json'});
    if ($res->code != 201) {
        if ($res->failed) {
            print STDERR "Content: " . $res->response->content;
        }
        die "put_attachment($uid) PUT of $name return code: ".$res->code."\n";
    }
    return $res->data->{rev};
}

1;
