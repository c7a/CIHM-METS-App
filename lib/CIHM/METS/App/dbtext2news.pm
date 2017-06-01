package CIHM::METS::App::dbtext2news;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use XML::LibXML;
use File::Basename;
use feature qw(say);
use Try::Tiny;

extends qw(CIHM::METS::App);

parameter 'dmp' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[An Inmagic DB/TextWorks file containing metadata records for newspapers (issues and series)],
);

option 'type' => (
  is => 'rw',
  isa => 'Str',
  default => "issues",
  documentation => q[Indicates if dump file has 'issues' or 'series' data],
);




command_short_description 'Sets fields \'wipmeta\' database based on DB/Text xml dumps';


sub run {
    my ($self) = @_;

    $self->setup();

    # Where to find my XSD
    $self->{simpledcxsd} = "/opt/xml/current/unpublished/xsd/simpledc.xsd";
    $self->{issueinfoxsd} = "/opt/xml/current/published/xsd/issueinfo.xsd";

    my $doc = XML::LibXML->load_xml(location => $self->dmp);
    $self->{xpc} = XML::LibXML::XPathContext->new($doc);
    $self->{xpc}->registerNs('inm', 'http://www.inmagic.com/webpublisher/query');

    # Used to store all parents, to be checked against wipmeta
    $self->{series}={};

    my @records=$self->xpc->findnodes('inm:Results/inm:Recordset/inm:Record');

    say STDERR "Processing ".scalar(@records)." records: \n"; 
    foreach my $record (@records) {
        if ($self->type eq 'issues') {
            $self->processIssueRecord($record);
        } else {
            $self->processSeriesRecord($record);
        }
    }

    my @series=keys $self->series;
    my $serieserror=0;
    if ($self->WIP && @series) {
        my $wipmeta=$self->WIP->wipmeta;
        $wipmeta->type("application/json");
        my $res = $wipmeta->post("/".$wipmeta->{database}."/_all_docs?include_docs=true",{keys => \@series}, {deserializer => 'application/json'});
        if ($res->code == 200) {
            if (defined $res->data->{rows}) {
                foreach my $row (@{$res->data->{rows}}) {
                    if (exists $row->{error}) {
                        if ($serieserror++ == 0) {
                            say STDERR "\n\nSeries Warnings:";
                        }
                        warn "Series=".$row->{key}." (".join(",",@{$self->series->{$row->{key}}}).") : ".$row->{error}."\n";
                    } elsif (!exists $row->{doc}->{reposManifestDate}) {
                        if ($serieserror++ == 0) {
                            say STDERR "\n\nSeries Warnings:";
                        }
                        warn "Series=".$row->{key}." (".join(",",@{$self->series->{$row->{key}}}).") : exists, but no reposManifestDate field (not yet ingested)\n";
                    }
                }
            }
        }
        else {
            die "\nGET of series records returned: ".$res->response->status_line."\n"; 
        }
    }
}

sub xpc {
    my $self = shift;
    return $self->{xpc};
}
sub series {
    my $self = shift;
    return $self->{series};
}


sub processIssueRecord {
    my($self,$record) = @_;

    my $doc = XML::LibXML::Document->new('1.0', 'UTF-8');
    my $type = $self->xpc->findvalue('inm:Type', $record);


    my $root = $doc->createElement('issueinfo');
    $root->setAttribute('xmlns', 'http://canadiana.ca/schema/2012/xsd/issueinfo');
    $doc->setDocumentElement($root);

    my $ord = $self->xpc->findvalue('inm:ORD', $record);
    $self->set_objid($ord);
    say STDERR $self->objid;

    my $label = $self->xpc->findvalue('inm:Iss', $record);
    my $updatedoc = {
        label => $label
    };

    my $series=$self->xpc->findvalue('inm:CIHM', $record);
    my $seriesid=$self->depositor.".".$series;
    if (!exists $self->series->{$seriesid}) {
        $self->series->{$seriesid}=[];
    };
    push  $self->series->{$seriesid},$label;
    add_value($doc, 'series', $series);

    add_value($doc, 'title', $self->xpc->findvalue('inm:TI', $record));
    
    my @ordparts=split('_',$ord);
    if (! @ordparts) {
        die "ORD=$ord can't be parsed into sequence\n";
    }
    # Take the 3'rd component
    my $seq=$ordparts[2];
    if (! $seq =~ /^\d+$/) {
        die "Sequence composed from $ord must have seq=$seq of only digits\n";
    }
    if (length($seq) == 6) {
        $seq .= '01';
    } elsif (length($seq) != 8) {
        die "Sequence composed from $ord must have seq=$seq of 6 or 8 digits\n";
    }
    add_value($doc, 'sequence', $seq);

    add_field($doc, 'language', $self->xpc->findnodes('inm:Lang', $record));
    add_field($doc, 'published', $self->xpc->findnodes('inm:IssueDate', $record));
    add_value($doc, 'source', $self->xpc->findvalue('inm:533', $record));


    $updatedoc->{'dmdsec'} = $doc->toString(1);

    # TODO: This should be a NOP, but is required -- why?
    $doc= XML::LibXML->load_xml(string => $doc->toString(0));

    my $schema = XML::LibXML::Schema->new(location => $self->{issueinfoxsd});
    # Will die() if failed.
    try {
        $schema->validate($doc);
    } catch {
        die "Caught error: $_\nXML=".$updatedoc->{'dmdsec'}."\n";
    };

    #send to couch
    $self->couchSend($updatedoc);
}


sub processSeriesRecord {
    my($self,$record) = @_;

    my $doc = XML::LibXML::Document->new('1.0', 'UTF-8');
    my $root = $doc->createElement('simpledc');

    my $cihm = $self->xpc->findvalue('inm:CIHM', $record);
    $self->set_objid($cihm);
    say STDERR $self->objid;


    $root->setAttribute('xmlns:dc', 'http://purl.org/dc/elements/1.1/');
    $doc->setDocumentElement($root);
    add_value($doc, 'dc:identifier', $self->objid);

    add_field($doc, 'dc:title', $self->xpc->findnodes('inm:TI', $record));
    add_field($doc, 'dc:title', $self->xpc->findnodes('inm:UNTI', $record));
    add_field($doc, 'dc:title', $self->xpc->findnodes('inm:AT', $record));


    add_field($doc, 'dc:contributor', $self->xpc->findnodes('inm:AN', $record));

    add_field($doc, 'dc:language', $self->xpc->findnodes('inm:Lang', $record));

    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:GN', $record));
    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:FRE', $record));
    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:ED', $record));


# PL,PUB,DT
    my $pl=$self->xpc->findvalue('inm:PL', $record);
    if (!$pl) {
        $pl='';
    }
    my $publisher=$pl;
    my $pub=$self->xpc->findvalue('inm:PUB', $record);
    if ($pub) {
        $publisher .= ": $pub";
    }
    my $dt=$self->xpc->findvalue('inm:DT', $record);
    if ($dt) {
        $publisher .= " $dt";
    }
    add_value($doc, 'dc:publisher', $publisher);

# DT
    foreach my $date (split('-',$dt)) {
        next if ($date =~ /^\s*$/);
        if ($date =~ /(\d{4}?)/) {
            add_value($doc, 'dc:date', $1);
        } else {
            warn "$date not understood as year\n";
        }
    }


    foreach my $lnk ($self->xpc->findnodes('inm:LNK', $record)) {
        my $value = $lnk->findvalue('.');
        next unless ($value);
        $value =~ s/\(\s*(id|an)[^)]*\)//gi;
        add_value($doc, 'dc:relation', $value);
    }

    add_field($doc, 'dc:subject', $self->xpc->findnodes('inm:SH1', $record));


    add_field($doc, 'dc:coverage', $self->xpc->findnodes('inm:GEOG', $record));
    add_field($doc, 'dc:coverage', $self->xpc->findnodes('inm:City', $record));


    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:362', $record));
    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:515', $record));

    add_value($doc, 'dc:format', 'Text');

    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:525', $record));
    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:580', $record));
    add_field($doc, 'dc:description', $self->xpc->findnodes('inm:588', $record));

    # TODO: This should be a NOP, but is required -- why?
    $doc= XML::LibXML->load_xml(string => $doc->toString(0));

    # Label is the first title
    my $updatedoc = {
        label => $doc->findnodes('//dc:title')->[0]->findvalue('.'),
        dmdsec => $doc->toString(1)
    };


    my $schema = XML::LibXML::Schema->new(location => $self->{simpledcxsd});
    # Will die() if failed.
    try {
        $schema->validate($doc);
    } catch {
        die "Caught error: $_\nXML=".$updatedoc->{'dmdsec'}."\n";
    };

    #send to couch
    $self->couchSend($updatedoc);
}

sub add_value {
    my($doc, $field, $value) = @_;
    return unless ($value);
    my $node = $doc->createElement($field);
    $node->appendChild($doc->createTextNode($value));
    $doc->documentElement->appendChild($node);
}

sub add_field {
    my($doc, $field, @list) = @_;
    foreach my $element (@list) {
        my $value = $element->findvalue('.');
        next unless ($value);
        my $text = $doc->createTextNode($value);
        my $node = $doc->createElement($field);
        $node->appendChild($text);
        $doc->documentElement->appendChild($node);
    }
}




1;
