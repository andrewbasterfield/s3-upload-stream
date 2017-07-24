#!/usr/bin/env perl

my $max_filesize = $ENV{MAX_FILESIZE};
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
my $file;
my $this_filesize = 0;
my $this_blocksize = 0;
binmode(STDIN);

my ($tmpfh,$filename) = getfh();

while ($this_blocksize = read(STDIN,$block,$max_blocksize)) {
  logger(INFO, "Read %d",$this_blocksize);
  logger(WARN, "Got error: %s", $!) if $!;
  
  if ($this_filesize + $this_blocksize <= $max_filesize) {
    #
    # Append to file
    #
    $tmpfh->write($block);
    $this_filesize += $this_blocksize;
  } else {
    #
    # Flush the file
    #
    process_file($tmpfh,$filename,$this_filesize);
    ($tmpfh,$filename) = getfh();
    $this_filesize = 0;
  }
}
process_file($tmpfh,$filename,$this_filesize) if $this_filesize;

sub getfh {
  my ($fh,$filename) = File::Temp::tempfile();
  binmode($fh);
  return ($fh,$filename);
}

my $c = 0;
sub process_file {
  my $fh = shift;
  my $filename = shift;
  my $filesize = shift;
  close($fh);
  if ($c++ > $skip) {
    my $s3filename = sprintf $template, $c;
    logger(INFO,"Flushing file %s of %d to S3 file %s",$filename,$filesize,$s3filename);
    my $object = $bucket->object( key => $s3filename );
    $object->put_filename( $filename );
  } else {
    logger(INFO,"Skipping file %s of %d",$filename,$filesize);
  }
  unlink $filename;
}

sub logger {
  my $level = shift;
  my $format_string = shift;
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
  my $line = sprintf($format_string, @_);
  printf "%4d-%02d-%02d %02d:%02d:%02d (%d) %s: %s\n",$year+1900,$mon+1,$mday,$hour,$min,$sec,$$,$level,$line;
}
