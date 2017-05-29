package CIHM::METS::App::Marc;

use common::sense;
use Encode;
use MooseX::App::Command;
use Try::Tiny;
use CIHM::WIP;
use Cwd qw(realpath);
use File::Slurp;
use Data::Dumper;
use XML::LibXML;
use feature qw(say);
use Encode;
use MARC::Batch;
use MARC::File::XML (BinaryEncoding => 'utf8', RecordFormat => 'USMARC');
extends qw(CIHM::METS::App);

parameter 'configid' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The configuration ID (Example: heritage)],
);

parameter 'marc' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[A .mrc file containing a metadata record],
);

option 'idschema' => (
  is => 'rw',
  isa => 'Str',
  default => '490',
  documentation => q[Identifier extraction schema (ooe|oocihm|490)],
);


option 'dump' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Dump information to screen rather than to CouchDB],
);

option 'objid' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Force this OBJID, and don't use idschema.  Only useful for single record marc files],
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

  my $file = $self->marc ||
  die "Can't retrieve marc file\n";

  #process .mrc files
  my $marc = MARC::Batch->new("USMARC", $file);


  # Loop through each record
  while (my $record = $marc->next)  {

      my $label = $record->subfield(245, "a");

      if (!$label || $label =~/^\s*$/) {
          warn "Label can't be extracted from 245a\n";
          next;  # Don't try to store based on this record
      }

      my $objid;

      if ($self->objid) {
          $objid=$self->objid;
      }
      # Get the OBJID - based on legacy production/marcsplit
      elsif ($self->idschema eq 'ooe') {
          $objid=substr($record->subfield('035', 'a'),1);
      }
      elsif ($self->idschema eq '490' || $self->idschema eq 'oocihm') {
          my $sf3=$record->subfield(490, "3");
          my $sfv=$record->subfield(490, "v");
          if ($sfv && $sfv ne '') {
              if ($sf3 && $sf3 ne '') {
                  $objid = join('_',$sf3,$sfv);
              } else {
                  $objid = $sfv;
                  if ($self->idschema eq 'oocihm') {
                      $objid =~ s/-/_/g;
                      $objid =~ s/[^0-9_]//g;
                  }
              }
          } else {
              warn "490v is missing\n";
          }
      }
      else {
          die("Unknown identifier extraction scheme: ". $self->idschema . "\n");
      }

      if (!$objid) {
          warn "OBJID wasn't able to be determined\n";
          next;  # Don't try to store based on this record
      }
      if (!($self->WIP->objid_valid($objid))) {
          warn "$objid not valid OBJID\n";
          next;  # Don't try to store based on this record
      }

      say STDERR "processing: $objid";
      #create xml structure -- using the "encode_utf8" function to encode to Perl's internal format 
      my $xml = encode_utf8(join( "",
          MARC::File::XML::header(),
          MARC::File::XML::record($record),
          MARC::File::XML::footer()
      ));

      if ($self->dump) {
          print Data::Dumper->Dump(["$depositor.$objid",$label,$xml] , [qw(AIP label xml)]);
      } else {
          #send to couch
          $self->couchSend({
              uid => "$depositor.$objid",
              label => $label,
              dmdsec =>   $xml
                           });
      }
  }
}

1;
