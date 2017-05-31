package CIHM::METS::App::Marc;

use common::sense;
use Encode;
use MooseX::App::Command;
use Try::Tiny;
use Cwd qw(realpath);
use File::Slurp;
use XML::LibXML;
use feature qw(say);
use Encode;
use MARC::Batch;
use MARC::File::XML (BinaryEncoding => 'utf8', RecordFormat => 'USMARC');
extends qw(CIHM::METS::App);

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

option 'forceobjid' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Force this OBJID, and don't use idschema.  Only useful for single record marc files],
);


command_short_description 'Sets metadata related fields and attachments in \'wipmeta\' database';


sub run {
  my ($self) = @_;

  $self->setup();

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

      if ($self->forceobjid) {
          $self->set_objid($self->forceobjid);
      }
      # Get the OBJID - based on legacy production/marcsplit
      elsif ($self->idschema eq 'ooe') {
          $self->set_objid(substr($record->subfield('035', 'a'),1));
      }
      elsif ($self->idschema eq '490' || $self->idschema eq 'oocihm') {
          my $objid;
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
              $self->set_objid($objid);
          } else {
              warn "490v is missing\n";
          }
      }
      else {
          die("Unknown identifier extraction scheme: ". $self->idschema . "\n");
      }

      if (!$self->objid) {
          warn "OBJID wasn't able to be determined\n";
          next;  # Don't try to store based on this record
      }

      say STDERR "processing: ".$self->objid;
      #create xml structure -- using the "encode_utf8" function to encode to Perl's internal format 
      my $xml = encode_utf8(join( "",
          MARC::File::XML::header(),
          MARC::File::XML::record($record),
          MARC::File::XML::footer()
      ));

      #send to couch
      $self->couchSend({
          label => $label,
          dmdsec =>   $xml
                       });
  }
}

1;
