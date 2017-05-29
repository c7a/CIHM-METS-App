package CIHM::METS::App::csv2dc;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use Try::Tiny;
use CIHM::WIP;
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

parameter 'configid' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The configuration ID (Example: heritage)],
);

parameter 'csv' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[A csv file containing metadata records],
);


command_short_description 'Sets metadata related fields and attachments in \'wipmeta\' database';


sub run {
    my ($self) = @_;

    my $configdocs=$self->WIP->configdocs ||
        die "Can't retrieve configuration documents\n";

    my $myconfig=$configdocs->{$self->configid} ||
        die $self->configid." is not a valid configuration id\n";

    my $depositor=$myconfig->{depositor} ||
        die "Depositor not set for ".$self->configid."\n";

    my $file = $self->csv ||
        die "Can't retrieve csv file\n";

    # Where to find my XSD
    my $simpledcxsd = "/opt/xml/current/unpublished/xsd/simpledc.xsd";

    #process csv file
    my $csv = Text::CSV->new({binary=>1});
    open my $fh, "<:encoding(utf8)", $file or die "Cannot read $file: $!\n";
    my $header = $csv->getline($fh);

    while(my $row = $csv->getline($fh)) {
	
        #get object id
        my $objid_column = first {@$header[$_] eq 'objid'}0..@$header;
	  
        #process each metadata record based on the object ID	
        foreach my $id ($row->[$objid_column]){
            if (!$id || $id =~ /^\s*$/) {
                warn "Line missing ID\n";
                next;
            }
		
            my $objid=$self->WIP->i2objid($id, $self->configid);
            if (!$self->WIP->objid_valid($objid)) {
                die "$objid not valid OBJID\n";
            }

            say STDERR "processing: $objid";
            my $doc = XML::LibXML::Document->new("1.0", "UTF-8");
            my $root = $doc->createElement('simpledc');
#            $root->setAttribute('xmlns:dc' => 'http://purl.org/dc/elements/1.1/');
            $root->setNamespace('http://purl.org/dc/elements/1.1/','dc',0);
            

            my %skipheaders = (
                'objid' => 1
            );

            # map header to dc element and process values	
            foreach my $thisheader (@$header) {
                if (substr($thisheader,0,3) eq 'dc:') {
                    get_element($thisheader, $header, $row, $root);
                }
            }
	  	
            #create xml file
            $doc->setDocumentElement($root);

            # TODO: This should be a NOP, but is required -- why?
            $doc= XML::LibXML->load_xml(string => $doc->toString(0));

            my $schema = XML::LibXML::Schema->new(location => $simpledcxsd);

            # Will die if validation failed.
            $schema->validate($doc);
            
            my $updatedoc = {
                uid => "$depositor.$objid",
                dmdsec => $doc->toString(1)
            };

            my $label_col;
            my $mets_label;
            $label_col = first {@$header[$_] eq 'dc:title'}0..@$header;
            if ($label_col) {
                my $label_value = $row->[$label_col];

                #split on delimiters
                my @titles = split (/\s*\|\|\s*/, $label_value);
                $mets_label = $titles[0];
            }

            if ($mets_label) {
                $updatedoc->{label}= $mets_label,
            } else {
                warn "No METS label set!\n";
            }
            
            #send to couch
            $self->couchSend($updatedoc);
        }
    }
}

sub get_element{
	my ($header, $header_array, $row, $root) = @_;

        my $header_index = first {@$header_array[$_] eq $header}0..@$header_array;
        my $value = $row->[$header_index];
		
        #split on delimiters
        my @values = split (/\s*\|\|\s*/, $value);
        foreach (@values){
            $root->appendTextChild($header,$_);
        }	
}

1;
