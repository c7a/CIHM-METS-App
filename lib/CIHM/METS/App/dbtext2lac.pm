package CIHM::METS::App::dbtext2lac;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use XML::LibXML;
use File::Basename;
use feature qw(say);

extends qw(CIHM::METS::App);

parameter 'dmp' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[An Inmagic DB/TextWorks file containing metadata records for LAC (reels and mikan)],
);


command_short_description 'Sets fields \'wipmeta\' database based on DB/Text xml dumps';


sub run {
    my ($self) = @_;

    $self->setup();

    my $file = $self->dmp ||
        die "Can't retrieve csv file\n";

    # Where to find my XSD
    $self->{simpledcxsd} = "/opt/xml/current/unpublished/xsd/simpledc.xsd";
    $self->{issueinfoxsd} = "/opt/xml/current/published/xsd/issueinfo.xsd";

    my $doc = XML::LibXML->load_xml(location => $file);
    $self->{xpc} = XML::LibXML::XPathContext->new($doc);
    $self->{xpc}->registerNs('inm', 'http://www.inmagic.com/webpublisher/query');

    # Used to store all parents, to be checked against wipmeta
    $self->{series}={};

    my @records=$self->xpc->findnodes('inm:Results/inm:Recordset/inm:Record');

    print "Processing ".scalar(@records)." records: \n\n"; 
    foreach my $record (@records) {
        $self->process_record($record);
    }

    my @series=keys %{$self->series};
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
                            print STDERR "\n\nSeries Warnings:\n";
                        }
                        warn "Series=".$row->{key}." (".join(",",@{$self->series->{$row->{key}}}).") : ".$row->{error}."\n";
                    } elsif (!exists $row->{doc}->{reposManifestDate}) {
                        if ($serieserror++ == 0) {
                            print STDERR "\n\nSeries Warnings:\n";
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


sub process_record {
    my($self,$record) = @_;

    my $doc = XML::LibXML::Document->new('1.0', 'UTF-8');
    my $type = $self->xpc->findvalue('inm:Type', $record);

    my $objid;
    my $label;

    if ($type eq 'Issue') {
        my $root = $doc->createElement('issueinfo');
        my $seq = $self->xpc->findvalue('inm:Reel_Number', $record);
        $objid = ($seq);
        $label = ($seq);
        $seq =~ s/[^0-9]//g;
        $objid =~ s/\W//g;
        
        #create lac reel objids
        $objid =~ s/(^[Cc])(.*)$/lac_reel_c$2/g;
        $objid =~ s/(^[Tt])(.*)$/lac_reel_t$2/g;
        $objid =~ s/(^[Hh])(.*)$/lac_reel_h$2/g;        

        say STDERR "$objid";
        
        $root->setAttribute('xmlns', 'http://canadiana.ca/schema/2012/xsd/issueinfo');
        $doc->setDocumentElement($root);

        my $series=join("_", "lac", "mikan", $self->xpc->findvalue('inm:Mikan', $record));
        my $seriesid=$self->depositor.".".$series;
        if (!exists $self->series->{$seriesid}) {
            $self->series->{$seriesid}=[];
        };
        push  @{$self->series->{$seriesid}},$label;
        add_value($doc, 'series', $series);

        add_value($doc, 'title', join(' - ', $self->xpc->findvalue('inm:Title', $record), $seq));
        add_value($doc, 'sequence', $seq);
        add_field($doc, 'language', $self->xpc->findnodes('inm:Lang', $record));
        my $coverage_from = $self->xpc->findvalue('inm:Begin_Date', $record);
        my $coverage_to = $self->xpc->findvalue('inm:End_Date', $record);
        if ($coverage_from && $coverage_to) {
            my $coverage = $doc->createElement('coverage');
            $coverage->setAttribute('start', $coverage_from);
            $coverage->setAttribute('end', $coverage_to);
            $root->appendChild($coverage);
        }
        add_value($doc, 'source', "Library and Archives Canada / Bibliothèque et Archives Canada");
        foreach my $identifier ($self->xpc->findnodes('inm:Reel_Number', $record)) {
            my $value = $identifier->findvalue('.');
            next unless($value);
            my $node = $doc->createElement('identifier');
            $node->setAttribute('type', 'mikan_reel');
            $node->appendChild($doc->createTextNode($value));
            $root->appendChild($node);
        }
        foreach my $identifier ($self->xpc->findnodes('inm:Mikan', $record), $self->xpc->findnodes('inm:Mikan_F', $record), $self->xpc->findnodes('inm:Mikan_Other', $record)) {
            my $value = $identifier->findvalue('.');
            next unless($value);
            my $node = $doc->createElement('identifier');
            $node->setAttribute('type', 'mikan_number');
            $node->appendChild($doc->createTextNode($value));
            $root->appendChild($node);
        }
        foreach my $identifier ($self->xpc->findnodes('inm:Reference', $record)) {
            my $value = $identifier->findvalue('.');
            next unless($value);
            my $node = $doc->createElement('identifier');
            $node->setAttribute('type', 'mikan_reference');
            $node->appendChild($doc->createTextNode($value));
            $root->appendChild($node);
        }


        # TODO: This should be a NOP, but is required -- why?
        $doc= XML::LibXML->load_xml(string => $doc->toString(0));

        my $schema = XML::LibXML::Schema->new(location => $self->{issueinfoxsd});

        # Will die() if failed.
        $schema->validate($doc);

        
    }
    elsif ($type eq 'Series') {
        my $root = $doc->createElement('simpledc');
        my $mikan = $self->xpc->findvalue('inm:Mikan', $record);

        $objid = "lac_mikan_$mikan";
        say STDERR "$objid";

        # Label is the first title
        $label = $self->xpc->findnodes('inm:Title', $record)->[0]->findvalue('.');
        $root->setAttribute('xmlns:dc', 'http://purl.org/dc/elements/1.1/');
        $doc->setDocumentElement($root);
        add_value($doc, 'dc:identifier', join("_", "lac", "mikan", $mikan));
        add_field($doc, 'dc:title', $self->xpc->findnodes('inm:Title', $record));
        add_field($doc, 'dc:title', $self->xpc->findnodes('inm:AT', $record));
        add_field($doc, 'dc:date', $self->xpc->findnodes('inm:Begin_Date', $record));
        add_field($doc, 'dc:date', $self->xpc->findnodes('inm:End_Date', $record));
        add_field($doc, 'dc:language', $self->xpc->findnodes('inm:Lang', $record));
        add_field($doc, 'dc:identifier', $self->xpc->findnodes('inm:Mikan_F', $record));
        add_field($doc, 'dc:identifier', $self->xpc->findnodes('inm:Mikan_Other', $record));
        add_field($doc, 'dc:identifier', $self->xpc->findnodes('inm:Reference', $record));
        add_field($doc, 'dc:identifier', $self->xpc->findnodes('inm:Archival_Reference', $record));
        add_field($doc, 'dc:identifier', $self->xpc->findnodes('inm:Collections', $record));
        add_field($doc, 'dc:subject', $self->xpc->findnodes('inm:Subject', $record));
        add_field($doc, 'dc:creator', $self->xpc->findnodes('inm:AU', $record));
        add_field($doc, 'dc:creator', $self->xpc->findnodes('inm:AN', $record));
        add_field($doc, 'dc:description', $self->xpc->findnodes('inm:Description', $record));
        add_field($doc, 'dc:description', $self->xpc->findnodes('inm:Contents', $record));    
        add_field($doc, 'dc:description', $self->xpc->findnodes('inm:Contents_F', $record));
        add_value($doc, 'dc:source', "Library and Archives Canada / Bibliothèque et Archives Canada");


        # TODO: This should be a NOP, but is required -- why?
        $doc= XML::LibXML->load_xml(string => $doc->toString(0));

        my $schema = XML::LibXML::Schema->new(location => $self->{simpledcxsd});

        # Will die() if failed.
        $schema->validate($doc);
        
    }
    else {
        die("Cannot process record with unknown type \"$type\"\n");
    }

    $self->set_objid($objid);

    my $updatedoc = {
        label => $label,
        dmdsec => $doc->toString(1)
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
