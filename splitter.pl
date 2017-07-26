#!/usr/bin/env perl

use strict;
use warnings;

package Logger;

use constant {
  INFO => "INFO",
  WARN => "WARN",
};

sub logger {
  my $level = shift;
  my $format_string = shift;
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
  my $line = sprintf($format_string, @_);
  printf "%4d-%02d-%02d %02d:%02d:%02d (%d) %s: %s\n",$year+1900,$mon+1,$mday,$hour,$min,$sec,$$,$level,$line;
}

package main;

my $max_objsize = $ENV{MAX_FILESIZE};
my $max_blocksize = $ENV{MAX_BLOCKSIZE};
my $bucketname = $ENV{BUCKETNAME};
my $template = $ENV{TEMPLATE};
my $skip = 0; # We start uploading at this chunk number

use Net::Amazon::S3;
use LWP::Protocol::https;

$SIG{__DIE__} = \&Carp::confess;
$SIG{__WARN__} = \&Carp::cluck;

my $s3 = Net::Amazon::S3->new({
  aws_access_key_id     => $ENV{'AWS_ACCESS_KEY_ID'},
  aws_secret_access_key => $ENV{'AWS_ACCESS_KEY_SECRET'},
  retry                 => 1,
  secure                => 1,
  host                  => 's3-eu-west-1.amazonaws.com',
});

my $s3client = Net::Amazon::S3::Client->new( s3 => $s3 );
my $bucket = $s3client->bucket( name => $bucketname );

my $block;
my $this_blocksize = 0;
binmode(STDIN);

my $c = 0;

my $objref = S3ObjWrapper->new($bucket,($skip > $c),$template,$c++);

while ($this_blocksize = read(STDIN,$block,$max_blocksize)) {
  Logger::logger(Logger::INFO, "Read %d",$this_blocksize);
  die $! if $!;
  
  if ($objref->size() + $this_blocksize <= $max_objsize) {
    #
    # Append to obj
    #
    $objref->write_chunk($block,$this_blocksize);
  } else {
    #
    # Flush the obj
    #
    $objref->complete_obj;
    $objref = S3ObjWrapper->new($bucket,($skip > $c),$template,$c++);
  }
}
$objref->complete_obj;
exit;

package S3ObjWrapper;

sub new {
  my $class = shift;
  my $bucket = shift;
  my $skip = shift;
  my $template = shift;
  my @template_args = @_;
  my $self = {};
  bless $self, $class;

  $self->{'name'} = sprintf $template, @template_args;
  $self->{'skip'} = $skip;
  if ($skip) {
    Logger::logger(Logger::INFO,"Skipping S3 object ref %s",$self->{'name'});
  } else {
    Logger::logger(Logger::INFO,"Getting S3 object ref %s",$self->{'name'});
    $self->{'object'} = $bucket->object( key => $self->{'name'} );
    $self->{'upload_id'} = $self->{'object'}->initiate_multipart_upload;
  }
  $self->{'etags'} = [];
  $self->{'parts'} = [];
  $self->{'part'} = 1;
  $self->{'size'} = 0;
  return $self;
}

sub size {
  my $self = shift;
  return $self->{'size'};
}

sub write_chunk {
  my $self = shift;
  my $data = shift;
  my $size = shift;

  unless ($self->{'skip'}) {
    Logger::logger(Logger::INFO,"Uploading part %d of size %s to S3 [uploaded so far %d]",$self->{'part'},$size,$self->{'size'});
    my $put_part_response = $self->{'object'}->put_part(
      upload_id   => $self->{'upload_id'},
      part_number => $self->{'part'},
      value       => $data
    );
    push @{$self->{'etags'}}, $put_part_response->header('ETag');
    push @{$self->{'parts'}}, $self->{'part'};
    $self->{'part'}++;
  }
  $self->{'size'} += $size;
}

sub complete_obj {
  my $self = shift;

  unless ($self->{'skip'}) {
    Logger::logger(Logger::INFO,"Completing S3 object of size %d",$self->{'size'});
    $self->{'object'}->complete_multipart_upload(
      upload_id    => $self->{'upload_id'},
      etags        => $self->{'etags'},
      part_numbers => $self->{'parts'},
    );
  }
}
