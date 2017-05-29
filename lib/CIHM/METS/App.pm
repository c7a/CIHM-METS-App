package CIHM::METS::App;

use MooseX::App;
use Log::Log4perl;
with 'MooseX::Log::Log4perl';
use Data::Dumper;

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
}

=head1 NAME

CIHM::METS::App - Command-line tool for generating/manipulating components of METS records.

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 AUTHOR

Sascha Adler, C<< <sascha.adler at canadiana.ca> >>
Russell McOrmond, C<< <russell.mcormond at canadiana.ca> >>
Julienne Pascoe, C<< <julienne.pascoe at canadiana.ca> >>

=head1 BUGS

Please report any bugs or feature requests through the web interface at
L<https://github.com/c7a/CIHM-METS-App>.  We will be notified, and then you'll
automatically be notified of progress on your bug as we make changes.

=cut



has 'WIP' => (
    is => 'rw',
    isa => 'CIHM::WIP',
    lazy => 1,
    builder => '_build_WIP',
);

sub _build_WIP {
    my $self = shift;
    my $WIP = CIHM::WIP->new($self->conf);
    if (!($WIP->wipmeta)) {
        die "<wipmeta> access to wipmeta database not configured in ".$self->conf."\n";
    }
    return $WIP;
};

option 'conf' => (
  is => 'rw',
  isa => 'Str',
  default => "/etc/canadiana/wip/wip.conf",
  documentation => q[An option that specifies where you can find a config file if not default],
);

sub couchSend {
    my ($self,$params) = @_;
    my ($res, $revision);

    my $wipmeta=$self->WIP->wipmeta;
    my $uid=$params->{uid};
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
