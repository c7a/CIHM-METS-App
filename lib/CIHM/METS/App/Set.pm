package CIHM::METS::App::Set;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use Try::Tiny;
use Cwd qw(realpath);
use File::Slurp;
use JSON;
use Text::CSV_XS;

extends qw(CIHM::METS::App);

parameter 'identifier' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The identifier (Example: C-11 , debates_CDC2702_20)],
);

option 'setlabel' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[String representing the label for this item],
);
option 'loadlabel' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[A filename that will be loaded to set label for this item],
);

option 'componentlabels' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[A filename read as component labels (.csv or .json)],
);
option 'dmdsec' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[A filename containing the dmdSec for this item],
);
option 'metadata' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[A filename containing a full METS record for this item],
);


command_short_description 'Sets metadata related fields and attachments in \'wipmeta\' database';


sub run {
  my ($self) = @_;

  $self->setup();

  # Load the provided label file
  if ($self->loadlabel) {
      my $label;
      open(my $fh,$self->loadlabel) or die "Can't open ".$self->loadlabel.": $!\n";
      defined($label = readline $fh) or die "Readline failed: $!";
      $label =~ s/\s+$//;
      if ($label ne '') {
          $self->setlabel($label);
      }
  }

  my $labels;
  if ($self->componentlabels) {
      my $file=$self->componentlabels;
      my ($ext) = $file =~ /(\.[^.]+)$/;
      if ($ext && $ext eq 'json') {
          my $clabels;
          try {
              $clabels = read_file($file);
          } catch {
              die "Error reading $file: $_";
          };
          try {
              $labels = from_json($clabels);
          } catch {
              die "Error parsing json file $file: $_\n";
          }
      } else {
          try {
              my @labels;
              my $csv = Text::CSV_XS->new ({ binary => 1});
              open my $fh, "<:encoding(utf8)", $file or die "Error opening $file: $!";
              while (my $row = $csv->getline ($fh)) {
                  push @labels,$row->[0];
              }
              close $fh;
              if (!($csv->eof)) {
                  my ($cde,$str,$pos, $rec, $fld) =  $csv->error_diag();
                  die "$str line $pos\n";
              }
              $labels = \@labels;
          } catch {
              die "Error parsing csv file $file: $_\n";
          }
      }
  }


  my $dmdsec;
  if ($self->dmdsec) {
      my $file=$self->dmdsec;
      try {
          $dmdsec = read_file($file);
      } catch {
          die "Error reading --dmdsec=$file: $_";
      };
  }

  my $metadata;
  if ($self->metadata) {
      my $file=$self->metadata;
      try {
          $metadata = read_file($file);
      } catch {
          die "Error reading --metadata=$file: $_";
      };
  }


  # Common function used to send them to CouchDB, no matter how they were
  # generated.
  $self->couchSend({
      label => $self->setlabel,
      clabels => $labels,
      dmdsec => $dmdsec,
      metadata => $metadata
                   });
  
}

1;
