package CIHM::METS::App::csv2issue;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use Try::Tiny;
use Cwd qw(realpath);
use File::Slurp;
use JSON;
use Text::CSV;
use XML::LibXML;
use List::Util qw(first);
use List::MoreUtils qw(first_index);
use File::Basename;
use feature qw(say);

extends qw(CIHM::METS::App);

parameter 'csv' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[A csv file containing metadata records],
);


command_short_description 'Sets metadata related fields and attachments in \'wipmeta\' database';


sub run {
    my ($self) = @_;

    $self->setup();

    my $label=$self->itemlabel;
    if (!$label) {
        warn "'itemlabel' not set for configid=".$self->configid." , will use default value 'column'\n";
        $label='column';
    }

    my $file = $self->csv ||
        die "Can't retrieve csv file\n";

    # Where to find my XSD
    my $issueinfoxsd = "/opt/xml/current/published/xsd/issueinfo.xsd";


    my @sequence = ('series','title','sequence','language','coverage','published','pubstatement','source','identifier','note');

    #process csv file
    my $csv = Text::CSV->new({binary=>1});
    open my $fh, "<:encoding(utf8)", $file or die "Cannot read $file: $!";
    my $headerline = $csv->getline($fh);

    # Create hash from headerline
    my %headers;
    my @unknownheader;
    for (my $i=0; $i < @$headerline; $i++) {
        my $header=$headerline->[$i];
        my $value={
            index => $i
        };
        if (index($header,'=') != -1) {
            my $type;
            ($header,$type) = split('=',$header);
            $value->{type}=$type;
        }

        if ($header eq 'objid' || $header eq 'label' ||
            first {$_ eq $header} @sequence) {
            if (!exists $headers{$header}) {
                $headers{$header}=[];
            }
            push @{$headers{$header}}, $value;
        } else {
            push @unknownheader, $header;
        }
    }
    if (@unknownheader) {
        warn "The following headers are unknown: " . join(',',@unknownheader)."\n";
    }
    die "'title' header missing\n" if (!defined $headers{'title'});
    die "'label' header missing\n" if (!defined $headers{'label'});


    my %series;
    while(my $row = $csv->getline($fh)) {
	
        #get object id
        my $objid_column = first {@$headerline[$_] eq 'objid'}0..@$headerline;
	  
        #process each metadata record based on the object ID	
        foreach my $id ($row->[$objid_column]){
            if (!$id || $id =~ /^\s*$/) {
                warn "Line missing ID\n";
                next;
            }

            $self->set_objid($id);
            say STDERR "processing: ".$self->objid;

            my $doc = XML::LibXML::Document->new("1.0", "UTF-8");
            my $root = $doc->createElement('issueinfo');
            $root->setAttribute('xmlns' => 'http://canadiana.ca/schema/2012/xsd/issueinfo');	


            foreach my $element (@sequence) {
                if (exists $headers{$element}) {
                    foreach my $elementtype (@{$headers{$element}}) {
                        my $value = $row->[$elementtype->{'index'}];
                        if ($element eq "coverage") {
                            my $child= XML::LibXML::Element->new($element);
                            my $attribute=0;
                            if ($value =~ /start=([0-9-]+)/i) {
                                $child->setAttribute('start',$1);
                                $attribute++;
                            }
                            if ($value =~ /end=([0-9-]+)/i) {
                                $child->setAttribute('end',$1);
                                $attribute++;
                            }
                            $root->appendChild($child);

                            if (!$attribute) {
                                warn "Unable to parse coverage value: $value\n";
                            }
                        }
                        else {
                            my $type = $elementtype->{'type'};
                            
                            #split on delimiters
                            my @values = split (/\s*\|\|\s*/, $value);
                            foreach (@values) {
                                my $child= XML::LibXML::Element->new($element);
                                $child->appendTextNode($_);
                                if ($type) {
                                    $child->setAttribute('type',$type);
                                }
                                $root->appendChild($child);
                                if ($element eq 'series') {
                                    $series{$self->depositor.'.'.$_}=1;
                                }
                            }
                        }
                    }
                }
            }

            #create xml file
            $doc->setDocumentElement($root);

            # TODO: This should be a NOP, but is required -- why?
            $doc= XML::LibXML->load_xml(string => $doc->toString(0));

            my $schema = XML::LibXML::Schema->new(location => $issueinfoxsd);

            # Will die() if failed.
            $schema->validate($doc);
            		
            # Determine label
            my $label_col;
            my $mets_label;
            if ($label eq "column") {
                $mets_label = $row->[$headers{'label'}[0]->{'index'}];
            }
            else {
                die "not recognized label value: $label";
            }

            #send to couch
            $self->couchSend({
                label => $mets_label,
                dmdsec => $doc->toString (1),
                             });
        }
    }
    my @series=keys %series;
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
                        warn "Series=".$row->{key}." : ".$row->{error}."\n";
                    } elsif (!exists $row->{doc}->{reposManifestDate}) {
                        if ($serieserror++ == 0) {
                            print STDERR "\n\nSeries Warnings:\n";
                        }
                        warn "Series=".$row->{key}." : exists, but no reposManifestDate field (not yet ingested)\n";
                    }
                }
            }
        }
        else {
            die "\nGET of series records returned: ".$res->response->status_line."\n"; 
        }
    }
}

1;
