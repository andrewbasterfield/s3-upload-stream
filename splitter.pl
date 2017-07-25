#!/usr/bin/env perl

my $max_objsize = $ENV{MAX_FILESIZE};
my $max_blocksize = $ENV{MAX_BLOCKSIZE};
my $bucketname = $ENV{BUCKETNAME};
my $template = $ENV{TEMPLATE};
my $skip = 0;

use Net::Amazon::S3;
use strict;
use warnings;
use File::Temp;
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

use constant {
  INFO => "INFO",
  WARN => "WARN",
};

my $block;
my $obj;
my $this_objsize = 0;
my $this_blocksize = 0;
binmode(STDIN);

my $c = 0;
my $p;
my @etags;
my @pns;

my ($objref,$upload_id) = getobjref();

while ($this_blocksize = read(STDIN,$block,$max_blocksize)) {
  logger(INFO, "Read %d",$this_blocksize);
  die $! if $!;
  
  if ($this_objsize + $this_blocksize <= $max_objsize) {
    #
    # Append to obj
    #
    write_chunk($objref,$block,$upload_id);
    $this_objsize += $this_blocksize;
  } else {
    #
    # Flush the obj
    #
    complete_obj($objref,$upload_id);
    ($objref,$upload_id) = getobjref();
    $this_objsize = 0;
  }
}
complete_obj($objref,$upload_id);
exit;

sub getobjref {
  my $objname = sprintf $template, $c++;
  logger(INFO,"Getting S3 object ref %s",$objname);
  my $object = $bucket->object( key => $objname );
  my $upload_id = $object->initiate_multipart_upload;
  @etags = ();
  @pns = ();
  $p = 1;
  return ($object,$upload_id);
}

sub write_chunk {
  my $obj = shift;
  my $data = shift;
  my $upload_id = shift;

  logger(INFO,"Uploading a chunk to S3");

  my $put_part_response = $obj->put_part(
    upload_id   => $upload_id,
    part_number => $p,
    value       => $data
  );

  push @etags, $put_part_response->header('ETag');
  push @pns, $p;
  $p++;
}

sub complete_obj {
  my $obj = shift;
  my $upload_id = shift;

  logger(INFO,"Completing S3 object");

  $obj->complete_multipart_upload(
    upload_id    => $upload_id,
    etags        => \@etags,
    part_numbers => \@pns,
  );
}

sub logger {
  my $level = shift;
  my $format_string = shift;
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
  my $line = sprintf($format_string, @_);
  printf "%4d-%02d-%02d %02d:%02d:%02d (%d) %s: %s\n",$year+1900,$mon+1,$mday,$hour,$min,$sec,$$,$level,$line;
}
