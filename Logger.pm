#------------------------------------------------
# Author  : zameer ahmed
# E-Mail  : zameer298@gmail.com
# Desc    : Module Created for Logger.
#------------------------------------------------

package Logger;

$|=1;

use Exporter;
use POSIX;
use IO::Socket;

# Constructor
sub new
{
    my ($class,$level) = @_;
    $level = uc($level);

    # Setting for Blank Parameter
    $level = "DEBUG" if ( $level eq '' );

    # Setting for Invalid Input
    $level = "DEBUG" if ( $level ne 'SQL' && $level ne 'FATAL' && $level ne 'ERROR' && $level ne 'WARN' && $level ne 'INFO' && $level ne 'DEBUG' );

    my $Log = {};
    # Setting Log Level
    if ( $level eq 'DEBUG' )
    {
        $Log = { _fatal => 1, _error => 1, _warng => 1, _info  => 1, _debug => 1, _writefile => 0, _console => 1, _sql => 1 };
    }
    elsif ( $level eq 'INFO' )
    {
        $Log = { _fatal => 1, _error => 1, _warng => 1, _info  => 1, _debug => 0, _writefile => 0, _console => 1, _sql => 1 };
    }
    elsif ( $level eq 'WARN' )
    {
        $Log = { _fatal => 1, _error => 1, _warng => 1, _info  => 0, _debug => 0, _writefile => 0, _console => 1, _sql => 0 };
    }
    elsif ( $level eq 'ERROR' )
    {
        $Log = { _fatal => 1, _error => 1, _warng => 0, _info  => 0, _debug => 0, _writefile => 0, _console => 1, _sql => 0 };
    }
    elsif ( $level eq 'FATAL' )
    {
        $Log = { _fatal => 1, _error => 0, _warng => 0, _info  => 0, _debug => 0, _writefile => 0, _console => 1, _sql => 0 };
    }
    elsif ( $level eq 'SQL' )
    {
        $Log = { _fatal => 1, _error => 1, _warng => 1, _info  => 1, _debug => 0, _writefile => 0, _console => 1, _sql => 1 };
    }

    $Log->{ _remoteflag }   = 0;
    $Log->{ _remotehost }   = '127.0.0.1';
    $Log->{ _remoteport }   = 1514;
    $Log->{ _logstr }       = 'scripts: ';
    $Log->{ _filehandle }   = '';

    return bless($Log,$class);
}

sub fatal
{
    my ($obj,$msg) = @_;

        if ( $obj->{_fatal} == 1 )
        {
        print strftime("%m-%d-%Y %H:%M:%S",localtime()), " [FATAL] => $msg \n" if ( $obj->{_console} == 1 );
        if($obj->{_remoteflag} == 1)
        {
            $obj->writeremotelog($msg,"FATAL");
        }
        else
        {
            $obj->writelog($msg,"FATAL") if ( $obj->{_writefile} == 1 );
        }
        }
}

sub error
{
    my ($obj,$msg) = @_;

        if ( $obj->{_error} == 1 )
        {
            print strftime("%m-%d-%Y %H:%M:%S",localtime()), " [ERROR] => $msg \n" if ( $obj->{_console} == 1 );
        if($obj->{_remoteflag} == 1)
        {
            $obj->writeremotelog($msg,"ERROR");
        }
        else
        {
            $obj->writelog($msg,"ERROR") if ( $obj->{_writefile} == 1 );
        }
    }
}

sub warng
{
    my ($obj,$msg) = @_;

        if ( $obj->{_warng} == 1 )
        {
            print strftime("%m-%d-%Y %H:%M:%S",localtime()), " [WARN ] => $msg \n" if ( $obj->{_console} == 1 );
        if($obj->{_remoteflag} == 1)
        {
            $obj->writeremotelog($msg,"WARN ");
        }
        else
        {
            $obj->writelog($msg,"WARN ") if ( $obj->{_writefile} == 1 );
        }
    }
}

sub info
{
    my ($obj,$msg) = @_;
        if ( $obj->{_info} == 1 )
        {
            print strftime("%m-%d-%Y %H:%M:%S",localtime()), " [INFO ] => $msg \n" if ( $obj->{_console} == 1 );
        if($obj->{_remoteflag} == 1)
        {
            $obj->writeremotelog($msg,"INFO ");
        }
        else
        {
                    $obj->writelog($msg,"INFO ") if ( $obj->{_writefile} == 1 );
        }
        }
}

sub debug
{
    my ($obj,$msg) = @_;

        if ( $obj->{_debug} == 1 )
        {
            print strftime("%m-%d-%Y %H:%M:%S",localtime()), " [DEBUG] => $msg \n" if ( $obj->{_console} == 1 );
        if($obj->{_remoteflag} == 1)
        {
            $obj->writeremotelog($msg,"DEBUG");
        }
        else
        {
            $obj->writelog($msg,"DEBUG") if ( $obj->{_writefile} == 1 );
        }
    }
}

sub sql
{
    my ($obj,$msg) = @_;

    if ( $obj->{_sql} == 1 )
    {
        print strftime("%m-%d-%Y %H:%M:%S",localtime()), " [SQL  ] => $msg \n" if ( $obj->{_console} == 1 );
        if($obj->{_remoteflag} == 1)
        {
            $obj->writeremotelog($msg,"SQL  ");
        }
        else
        {
            $obj->writelog($msg,"SQL  ") if ( $obj->{_writefile} == 1 );
        }
    }
}


sub setlogfile
{
    my ($obj,$file,$mode,$RemoteFlag) = @_;

    if($RemoteFlag == 1)
    {
        my $logstr = $1 if($file =~ m/.*\/(.*?).log$/);
        my $sock = IO::Socket::INET->new(
                                PeerAddr => $obj->{_remotehost},
                                PeerPort => $obj->{_remoteport},
                                Proto    => 'udp',
                            ) || die "Couldn't connect to $host:$port: $!";
        $obj->{_remoteflag} = $RemoteFlag;
        $obj->{_remotehandle} = $sock;
        $obj->{_logstr} .= $logstr;
    }
    else
    {
        # Default Mode is Write
            if ( $mode eq 'a' ){
                    $mode = ">>";
        }else{
                $mode = ">";
            }

        my $fh;
            open($fh,"$mode$file") || die "Error: $! \n";
        $obj->{_writefile} = 1;
            $obj->{_filehandle} = $fh;
    }
}

sub writeremotelog
{
    my ($obj,$msg,$mode) = @_;
    $msg = $obj->{_logstr}." |[$mode] => $msg \n";
    $obj->{_remotehandle}->send("$msg");
}

sub writelog
{
    my ($obj,$msg,$mode) = @_;
        my $fh = $obj->{_filehandle};
    print $fh strftime("%m-%d-%Y %H:%M:%S",localtime()), " [$mode] => $msg \n";
}

sub closelog
{
    my ($obj) = @_;
        my $fh = $obj->{_filehandle};
        close $fh;
}

sub enable_console
{
    my ($obj) = @_;
        $obj->{_console} = 1;
}

sub disable_console
{
    my ($obj) = @_;
    $obj->{_console} = 0;
}

1;
