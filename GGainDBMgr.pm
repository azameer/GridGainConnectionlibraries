#! /usr/bin/perl

package GGainDBMgr;

use strict;
use warnings;

use Exporter;
use POSIX;
#use Kodiak::GGDbMgr;
use GGDbMgr;
my @ISA = qw(Exporter);

sub new
{
    my $class   = shift;
    my $logger  = shift;
    my $ggsvrip = shift;
    my $ggport  = shift;

    my $self = {};
    $self->{'logger'} = $logger;
    $self->{'DBMgrObj'} = new GGDbMgr::GGDbMgr();
    $self->{'GGSvrIP'} = $ggsvrip;
    $self->{'GGPort'} = $ggport;
    $self->{'GGRetryCntrObj'} = 0; ## This cnt is required for Qry execution retries. Default is 0

    bless $self, $class;
    return $self;
}

sub GGConnect
{
    ##### GGConnect() return's 0 for Success and 1 for Failure ############################################
    my ($self)=@_;
    my $dsn = "DRIVER=/DG/activeRelease/GridGain/lib/libignite-odbc.so;SERVER=$self->{'GGSvrIP'};PORT=$self->{'GGPort'}";
    $self->{'logger'}->info("Connecting to GG Server $self->{'GGSvrIP'}");
    my $ConnStatus = $self->{'DBMgrObj'}->Connect($dsn);
    if ($ConnStatus eq 0)
    {
        $self->{'logger'}->info("Successfully Connecting to GG Server: $self->{'GGSvrIP'} Grid Gain Port: $self->{'GGPort'}");
    }
    else
    {
        $self->{'logger'}->error("Failed to connect to GG Server: $self->{'GGSvrIP'} Grid Gain Port: $self->{'GGPort'}");
        $ConnStatus=-1;
    }
    my $msgstr=dmsg($self);
    return ($ConnStatus,$msgstr);
}

sub GGReconnect
{
    my ($self)=@_;
    foreach my $retrycnt (0 .. $self->{'GGRetryCntrObj'})
    {
        GGDisconnect($self);
        $self->{'logger'}->info("...... Reconnecting to GG Svr. Retry Count: $retrycnt");
        my $ConnStatus=GGConnect($self);
        return 0 if ($ConnStatus eq 0);
    }
    return 1;
}    

sub RetryCount
{
    my ($self,$RetryCnt)=@_;
    $self->{'GGRetryCntrObj'} = $RetryCnt; 
}

sub GGExecuteQry
{
    #### This returns an Array Ref which will have the select O/P array. 
    #### Query Failure returns -1 Success returns Array Ref
    my ($self,$Qry)=@_;
    my $msgstr="";
    $self->{'logger'}->info("Excuting Query: $Qry");
    my $QryOutRef=$self->{'DBMgrObj'}->executeQuery("$Qry");
    if ($QryOutRef eq -1)
    {
        $msgstr=dmsg($self);
        return ($QryOutRef,$msgstr);
    }
    $QryOutRef=GetArrayRef($QryOutRef);
    $self->{'logger'}->info("Execute Query O/P is: @$QryOutRef");
    $msgstr=dmsg($self);
    return ($QryOutRef,$msgstr);   
}

sub GGUpdate
{
    my ($self,$Qry)=@_;
    $self->{'logger'}->info("Excuting Query: $Qry");
    my $QryStatus=$self->{'DBMgrObj'}->executeUpdate("$Qry");
    my $msgstr=dmsg($self);
    return ($QryStatus,$msgstr);
}

sub GGDisconnect
{
    my ($self)=@_;
    my $DisconnStatus=$self->{'DBMgrObj'}->Disconnect();
    my $msgstr=dmsg($self);
    return ($DisconnStatus,$msgstr);
}

sub GetArrayRef($)
{
    my ($QryOut)=shift;
    my @QryOutArr = split(/\n/,$QryOut);
    map($_ =~ s/(\s*<\s*|\s*>\s*)//g,@QryOutArr);
    return \@QryOutArr;
}

sub dmsg($)
{
    my $myobj=shift;
    my $msgstr = $myobj->{'DBMgrObj'}->GetDebugMsg();
    $myobj->{'logger'}->info("$msgstr");
    return $msgstr;
}

sub GGExecuteQryFromFile
{
    ##### return's 0 for Success and 1 for Failure ############################################
    ##### Takes parameter as Hashref
    my ($self, $Paramref) = @_;

    my @ConfFileLines =  @{$Paramref->{'SQLFileArray'}};
    my $ContFlag     =  (exists $Paramref->{'Continue'}) ? $Paramref->{'Continue'} : 0;

    foreach my $Line ( @ConfFileLines )
    {
        next if (($Line =~ m/^\s*$/) || ($Line =~ m/^\s*#/));        ### Skip Empty or commented lines

        next if ( $Line =~ m/^\s*commit\s*$/i ); #### Skipping commit line, as GG won't support commit

        if ( $Line !~ m/^\s*DBQUERY::/i )
        {
            $self->{'logger'}->error("QueryLine [$Line] is in invalid format");
            return 1;            
        }

        my $Status = $self->QueryHandle($Line);
        if ( $Status == 1 )
        {
            $self->{'logger'}->error("QueryLine [$Line] Execution Failed");
            next if ( $ContFlag );    #### Continue other Query Line 
            return 1;                 ### Return error
        }
    }
    return 0;
}

sub CheckQuery
{
    my ($self, $ChkQry) = @_;
    
    my @DelimiterArray = ($ChkQry =~ m/([|&])/g);
    my @QueryList = split(/[|&]/, $ChkQry); 
    my @CheckStatusResult;
    my $CheckQryReturnVal;
  
    foreach my $Qry ( @QueryList )
    {
        if ($Qry !~ m/\~/)
        {
            $self->{'logger'}->error("Check Operator '~' in missing in CheckQry[$Qry]");
            return 1;
        }

        my ($CheckQuery,$CheckStatus) = split(/\~/,$Qry);


        my @Out = $self->GGExecuteQry($CheckQuery); 
        
        my $OutResult;
        if( ref ($Out[0]) eq 'ARRAY' )
        {
            $OutResult = $Out[0][0];
        }
        else
        {
            $self->{'logger'}->info("No records found for $CheckQuery.. $Out[1]..");
            return 1;
        }
       
        if ($CheckStatus =~ m/(.*)\s+(.*)/)
        {
            my $result = eval "$OutResult $1 $2";
            $result = 0 if ($result eq "");
            $self->{'logger'}->info("eval Result of CheckQuery is $result");
            push(@CheckStatusResult,$result);
        }
    }

    if ( scalar @DelimiterArray )
    {
        my $result = shift (@CheckStatusResult);
        for( my $len=0; $len<=scalar(@DelimiterArray); $len++ )
        {
            $result = eval "$result $DelimiterArray[$len] $CheckStatusResult[$len]";
        }
        $CheckQryReturnVal = $result; 
    }
    else
    {
        $CheckQryReturnVal = $CheckStatusResult[0];
    } 

    if ( $CheckQryReturnVal eq 1 )
    {
        $self->{'logger'}->info("CheckQuery result is true. Returning 0 as success");
        return 0;
    }
    else
    {
        $self->{'logger'}->info("CheckQuery result is false. Returning 2 as already applied");
        return 2;
    }
}

sub QueryHandle
{
    my ($self, $QryLine) = @_;
    
    my ($StartString,$ValidateQuery,$ExecuteQuery) = split(/::/,$QryLine);
   
    if ($ValidateQuery ne '')
    {
        my $Status = $self->CheckQuery($ValidateQuery);
        if ( $Status == 1 )
        {
             $self->{'logger'}->error("CheckQuery [$ValidateQuery] Failed");
             return 1;
        }
        elsif( $Status == 2 )
        {
            $self->{'logger'}->info("CheckQuery [$ValidateQuery] already applied");
            return 0;
        }
         
    }

    my $Splitvalue = '$#$$#';    
    my @QueryList = split(/\Q$Splitvalue\E/, $ExecuteQuery);
    
    foreach my $Qry ( @QueryList )
    {
        my ($Status,$msgstr) = $self->GGUpdate($Qry);
         
        if ($Status != 1)
        {
             $self->{'logger'}->error("ExecuteQuery [$Qry] Failed"); 
             $self->{'logger'}->error("$msgstr");
             return 1;
        }
        $self->{'logger'}->info("ExecuteQuery [$Qry] Success..");
    }

    return 0;
}

1;
