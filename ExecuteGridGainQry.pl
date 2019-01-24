#!/usr/bin/perl

use strict;
use warnings;

BEGIN
{
   if(!-d "/DG/activeRelease/lib/perl_lib/")
   {
      print "Dependent directory /DG/activeRelease/lib/perl_lib/ not found in current path!!!\n\n";
      exit();
   }
   unshift(@INC,"/DG/activeRelease/lib/perl_lib/");
}

use Kodiak::Logger;
use Kodiak::GGainDBMgr;

my $LogFile = "/DGlogs/ExecuteGridGainQry.log";
my $Logger = Logger->new("DEBUG");
$Logger->setlogfile($LogFile,"a");
$Logger->disable_console();

my ($GG_FQDN, $GG_SQL_PORT, $GG_SERVICEPLANE_IP, $IN_OPT, $IN_QRY);

my $CommonConfFile   = "/DG/activeRelease/dat/CommonConfig.properties";
my $ContainerINIFile = "/DG/activeRelease/dat/containerinit.ini";

sub Usage
{
    $Logger->info("--------------- Usage() ------------------");

    print "\tThis script will excute the given Query on GridGain server and display Query result\n";
    print "\tUSAGE : perl $0 -opt=<DBOperation> -q=<query>\n";
    print "\t\t<DBOperation> like INSERT|UPDATE|DELETE|DROP|CREATE|SELECT \n";
    print "\t\t<query> is SQL Query which need to be executed\n";
    print "\tEX: perl $0 -opt=Insert -q=\"Insert into TestTable values('test')\"\n";
    print "\tEX: perl $0 -opt=Select -q=\"Select * From TestTable\"\n";
}

sub CleanUp
{
    $Logger->info("--------------- CleanUp() ------------------");

    my $Flag = shift;

    $Logger->closelog();
    exit $Flag;
}

sub PrintInfo
{
    my $msg = shift;
    print "$msg\n";
    $Logger->info($msg);
}

sub PrintError
{
    my $msg = shift;
    print "ERROR: $msg\n";
    $Logger->error($msg);
}

sub Validate
{
    $Logger->info("--------------- Validate() ------------------");

    if ( scalar @ARGV < 2 )
    {
        PrintError("Arguments is missing.... Please follow below Usage");
        Usage();
        CleanUp(1);
    }

    if($ARGV[0] =~ m/-opt=(\w+)/i && ($1 =~ m/^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|SELECT)\s*$/i) )
    {
        $IN_OPT = $1;
    }

    if($ARGV[1] =~ m/-q=(.*)/i  )
    {
        $IN_QRY = $1;
    }

    if ( !$IN_OPT || !$IN_QRY )
    {
        PrintError("Arguments is Empty or Invalid.... Please follow below Usage");
        Usage();
        CleanUp(1);
    }

    $GG_FQDN = GetValueFromFile('GG_FQDN',$CommonConfFile);
    $GG_SQL_PORT = GetValueFromFile('GG_SQL_PORT',$CommonConfFile);
    $GG_SERVICEPLANE_IP = GetValueFromFile('SERVICEPLANE_IP_ADDRESSES',$ContainerINIFile);
    if (($GG_FQDN eq '') || ($GG_SQL_PORT eq '') || ($GG_SERVICEPLANE_IP eq ''))
    {
        PrintError("GG_SERVICEPLANE_IP, GG_FQDN or GG_SQL_PORT is null.. Please check $CommonConfFile and $ContainerINIFile file....");
        CleanUp(1);
    }
}

sub GetValueFromFile
{
    $Logger->info("--------------- GetValueFromFile() ------------------");
    my ($Key, $File ) = @_;

    my $Value = `egrep '^$Key=' $File | awk -F'=' '{print \$2}'`;
    chomp($Value) if ( $Value );
    $Value ? return $Value : return;
}

sub GetGGConection
{
    $Logger->info("--------------- GetGGConection() ------------------");

    foreach my $IPorFQDN ($GG_FQDN, $GG_SERVICEPLANE_IP )
    {
        my $GGConnHandlr = new GGainDBMgr($Logger,$IPorFQDN,$GG_SQL_PORT);
        my ($Status,$msgstr)= $GGConnHandlr->GGConnect();
        if ( $Status != 0 )
        {
            $Logger->error("DB connection Failed for $IPorFQDN.. $msgstr");
            next;
        }
        else
        {
            $Logger->info("DB connection Success with '$IPorFQDN'...");
            return $GGConnHandlr;
        }
    }
    PrintError("DB connection Failed for both GG_FQDN=$GG_FQDN and GG_SERVICEPLANE_IP=$GG_SERVICEPLANE_IP");
    CleanUp(1);
}

sub ExecuteQry
{
    $Logger->info("--------------- ExecuteQry() ------------------");

    my $GGConnHandlr = GetGGConection();

    $Logger->info("Executing Query [$IN_QRY]...");

    if( $IN_OPT =~ /SELECT/i )
    {
        my @Out = $GGConnHandlr->GGExecuteQry($IN_QRY);

        if( ref ($Out[0]) ne 'ARRAY' || !(scalar(@{$Out[0]})))
        {
            PrintInfo("No records found for Query[$IN_QRY]") unless( $Out[1] );
            PrintError("$Out[1]") if ($Out[1]);
            CleanUp(0);
        }

        PrintInfo("Query [$IN_QRY] Execution success..");

        $Logger->info("Query Out= @{$Out[0]}");

        foreach my $ROW( @{$Out[0]} )
        {
            print"$ROW\n";
        }
    }
    else
    {
        my ($Status,$msgstr) = $GGConnHandlr->GGUpdate($IN_QRY);
        $Logger->info("Query Status=$Status and msgstr=$msgstr");

        if($Status == -1)
        {
            PrintError("$msgstr");
            CleanUp(1);
        }
        PrintInfo("Query [$IN_QRY] Execution success..");
    }

    $GGConnHandlr->GGDisconnect();
}


$Logger->info("----Main Start---");

Validate();

ExecuteQry();
CleanUp(0);

