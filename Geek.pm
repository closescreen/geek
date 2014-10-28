package Geek;
use strict;
use warnings;
use Data::Dumper;
use Date::Calc;
use Exporter 'import';
use Netflag;
use File::Path qw(make_path);
use File::Basename qw(dirname);

our @EXPORT_OK = qw( cmd need any target netflag qb bash perl );
our %EXPORT_TAGS = ( all=>[qw( cmd need any target netflag qb bash perl )] );

my $geek_object = Geek->new();

sub new{
 my $pkg = shift or die "Package! Usage: Geek->new()";
 my %se = ();
 return bless \%se;
};

sub uniq{
 # give LIST  
 # Return uniqued LIST
 if ( ref $_[0] eq "Geek" ){
    shift;
 }
 my %rv = map {$_,$_} @_;
 return wantarray ? (values %rv) : [values %rv];
}


sub routes{
 my $se = shift;
 ref $se or die "First param must be a reference to Geek object";
 $se->{routes} = \@_;
 $se->check( $se->{routes} );
 return $se;
}

sub check{
 # предварительная проверка структуры routes на валидность
 my $se = shift;
 my $rs = shift;
 ref $rs eq "ARRAY" or die "routes must be a ARRAY! ".Dumper($rs);
 if ( my @missing_cmd = grep {! $_->{cmd} } @$rs ){
    die "Missing 'cmd' for routes: ".Dumper(@missing_cmd);
 }
 if ( my @missing_target = grep {! $_->{target} } @$rs ){
    die "Missing 'target' for routes: ".Dumper(@missing_target);
 } 
 
 my %possible = (
    target=>"Route template for matching with wanted file name(s)",
    cmd =>"Command to run for matched route",
    need =>"List of need files for wanted file",
    any =>"One of this list is need for wanted file",
    desc =>"Description for cmd",
 );
 
 for my $r ( @$rs ){
    if ( my @bad_keys = grep {!$possible{$_}} keys %$r ){
	die "Bad keys: @bad_keys in route ".Dumper($r)." Allowed only: ".Dumper(\%possible)
    }
 }
 
}

sub match{
 # gives "wanted", returns "jobs"
 my $se = shift;
 ref $se eq "Geek" or die "First param must be a reference to Geek object"; 
 my %pa = @_;
 my %allowed = ( wanted=>1, #ARRAYREF of wanted filenames to calculate
		 #recursive=>1, # 0|1|2. By default = 0. 0-norecursive, 1-recursive for files with filed tests, 2-recursive for all files
		 need=>1, # "", "all", "todo",sub{ ..work with @_=@tested  }
		 any=>1, # "", "max","min",sub{ ..work with @_=@tested }
		 deep=>1, # ""||0, 1..* - stop when recursion level equal 'deep'. 1=top, 2=top and its need|any
		 testers=>1, # HASHREF {"name"=>sub{ testing $_ }, ...} - way to define/override test subs. See sourse of ->test() for details.
		 return=>1, # 'subs'|'dump'. By default - 'subs'. 'dump' - return HASH or HASHREF with result structure.
		 _recurse_level =>1 , # internal parameter for count recursion level. 
		 _parent_wanted=>1, # internal for reference to parent wanted element 
		 _rv=>1, # internal return value hashref
		 );
 if ( my @bad_params = grep { !$allowed{$_} } keys %pa ){
    die "Bad params: @bad_params. Allowed: ".(join(", ", keys %allowed));
 }
 my $wanted;
 defined $pa{ wanted } or die "param wanted must be defined! (STRING or ARRAYREF or CODEREF)";
 if ( !ref $pa{ wanted } ){ $wanted = [ split /\s+/, $pa{ wanted } ] }
 elsif( ref $pa{ wanted } eq "ARRAY" ){ $wanted = $pa{ wanted } }
 elsif( ref $pa{ wanted } eq "CODE" ){
  $wanted = $pa{ wanted }->();
  if ( !ref $wanted ){
    $wanted = [$wanted]; # may be a single filename returned.
  }
  ref $wanted eq "ARRAY" or die "wanted=>sub{...} must return ARRAYREF!"; 
 }else{
    die "Unknown type of 'wanted' param. ".Dumper($pa{ wanted });
 }

 my %possible_need = (
    "" => sub{()}, # nothing to add
    "todo" => sub { map { $_->{name} } grep { ! $_->{test_ok} } @_ },
    "all" => sub{ map { $_->{name} } @_ }, # all "need" files to recursive tree
 ); 

 my %possible_any = (
    "" => sub{()}, # nothing to add
    "max" => sub{ # max name from 'any' list
	my $any_exists = grep { $_->{test_ok} } @_;
	[ sort map { $_->{name} } @_ ]->[-1] if not $any_exists
    }, 
    "min" => sub{ # min name from 'any' list
	my $any_exists = grep { $_->{test_ok} } @_;
	[ sort map { $_->{name} } @_ ]->[0] 
    }, 
    "all" => sub{ map { $_->{name} } @_ }, # all "need" files to recursive tree
 );  
 
 my %filter_code;
 if (!ref $pa{need}){
    $filter_code{need} = $possible_need{$pa{need}||""} or die "not possible need=>'$pa{need}'. Possible:".Dumper(\%possible_need);
    $filter_code{wanted} = $pa{need} ? $filter_code{need} : $possible_need{todo};
 }else{
    $filter_code{need} = $pa{need};
    $filter_code{wanted} = $filter_code{need};
 }    
 
 if (!ref $pa{any}){
    $filter_code{any} = $possible_any{ $pa{any}||"" } or die "not possible any=>'$pa{any}'";
 }else{
    $filter_code{any} = $pa{any};
 }
 

 
 $pa{deep}+=0;
 
 
 my $testers = $pa{testers}||{}; # allow to set: testers=>{ proto1=>sub{ -s $_ > 20 }, } # and need_file like 'proto1://aa.gz' - will checked that sub
 my @recursive_wanted;
 my @matched_routes;
 if ( !$se->{routes} ){ die "routes is empty." }
 $pa{ _recurse_level }++; # current recurse level
 #warn $pa{ _recurse_level };
 $pa{ _parent_wanted }||="";
 $pa{return}||='subs';

 my $rv = $pa{ _rv }||{};
 
 my %allowed_route_keys = (target=>1, need=>1, any=>1, cmd=>1,);
 
 for my $wanted_elem ( @$wanted ){ # level I
    
    my @tested_elem = map { { name=>$_, test_ok=>$se->test( $testers, $_) } } $wanted_elem;
    next if not $filter_code{wanted}->(@tested_elem);
  
    for my $r ( @{ $se->{routes}||[] } ){
	ref $r eq "HASH" or die "route must be a hashref:".Dumper($r);
	if ( my @bad_keys = grep { !$allowed_route_keys{$_} } keys %$r ){
	    die "bad keys:@bad_keys. Allowed only:".join(", ",keys %allowed_route_keys);
	}
	if ( my $re = $r->{target} ){

	    if ( $wanted_elem=~$re ){
	    	
	    	
	    	if ( $rv->{$wanted_elem} ){ # если есть такой в rv, то только записываем _parent_wanted и пропускаем:
	    	    if ( $pa{ _parent_wanted } ){
	    		$rv->{ $wanted_elem }{ parents }{ $pa{ _parent_wanted } }++;
	    	    }
	    	    next;
	    	}
		
		for my $node_name (qw| need any |){
		    if ( my $node = $r->{$node_name} ){
			if ( ref $node eq "CODE" ){
			    # need,any могут быть процедурой возвращающей хеш. (наверное, это будет полезно когда-нибудь). 
			    $node = $node->(); #передавать параметры и какие?
			}
			if ( ref $node eq "HASH"){
			    # need,any могут быть hashref-ом
			    for my $type ( keys %$node ){
				my $need_smf = $node->{$type};
				if ( ref $need_smf eq "CODE" ){
				    my @need_smf = $need_smf->();
				    my @tested = map { { name=>$_, test_ok=>$se->test( $testers, $_) } } @need_smf;
				    $rv->{ $wanted_elem }{ $node_name }{ $type } = [ @tested ];
				    #if ( $recursive==1 ){
				    
				    ref $filter_code{$node_name} or die "filter_code for node name '$node_name' is not a CODEREF !"; 
				    if ( !$pa{deep} or $pa{deep}>$pa{ _recurse_level } ){
					push @recursive_wanted, $filter_code{$node_name}->(@tested);
				    }	
				    
				}
			    }
			}else{
			    die "$node_name is unknown type. Forgot wrap your code by '{}' or 'sub{...}' ?'".Dumper($node);
			}
		    }
		}
		
		if ( my $cmd = $r->{cmd} ){
		    my $re2; my $str2;
		    my %named_groups = ( TARGET=>[$wanted_elem], %{ $rv->{ $wanted_elem }{ need }||{} }, %{ $rv->{ $wanted_elem }{ any }||{} } );
		    #die Dumper \%named_groups;
		    for my $name ( keys %named_groups ){
			my $value;
			if ( ref $named_groups{$name} eq "ARRAY" ){
			    $value = join(" ", map {ref $_ eq "HASH" ? $_->{name} : $_ } @{ $named_groups{$name} } );
			}elsif(!ref $named_groups{$name}){
			    $value = $named_groups{$name};
			}else{
			    die "Unknown type!";
			}
			
			my $escaped = quotemeta $value;
			
			$re2.="-$name-(?<$name>$escaped)";
			$str2.="-$name-$value";
		    }
		    my $str1 = "$wanted_elem$str2";
		    my $re1 = qr/$re$re2/;
	    	    die "---NOT MATCHED $str1 to $re1---" if not "$str1"=~$re1;
		    #warn "$re1 match with $re1".Dumper(\%+);
		    %_ = %+;
		    for my $vname ( grep {!m/TARGET/} keys %_ ){
			my @vals = split /\s+/, $_{$vname};
			$_{"${vname}_ARRAY"} = [ @vals ];
			$_{"${vname}_HASH"} = { map {$_,1} @vals };
		    } 
		    my $job = &$cmd;
		    if ( !$job ){
			die "empty job returned.".Dumper( \$rv->{ $wanted_elem } );
		    }else{
			$rv->{$wanted_elem}{ parents }{ $pa{ _parent_wanted } }++;
			$rv->{$wanted_elem}{ job } = $job;
			$rv->{$wanted_elem}{ vars } = {%_};
		    }
		}else{
		    die "cmd must be defined for route! ".Dumper($r);
		}
	    }
	}
    }#<-------( end of $r )----------

    for my $name ( @recursive_wanted ){
	    if ( $rv->{ $name } ){
		$rv->{ $name }{ parents }{ $wanted_elem }++;
		$name = undef;
	    }
    }
    @recursive_wanted = grep {$_} @recursive_wanted;
    
    if ( @recursive_wanted ){
	    #warn "call match..";
	    $se->match( %pa, wanted=>[ @recursive_wanted ], _parent_wanted=>$wanted_elem, _rv=>$rv );
	    #warn "OK";
    }    
    

     #<--------( end or wanted_elem )------
 
 }
 
 if ( $pa{ _recurse_level }==1 ){

    if ( $pa{return} eq 'subs' ){
	my $counter;
	my %names;
	#$names{$_}=++$counter for select_names("", $rv, \%names);
	select_names("", $rv, \%names, \$counter);
	my @names = grep {$_} sort { $names{$a} <=> $names{$b} } keys %names;
	#die Dumper \@names;
	#die Dumper $rv;
	my @targets = map { {target=>$_, %{$rv->{$_}} } } @names;
	#die Dumper \@targets;
	#warn "return targets";
	return wantarray ? @targets : \@targets;
    }
 }    
 #3warn "return 1"; 
 return 1; # nothing need to return
  
}

sub select_names{ 
 my $given_name = $_[0];
# warn "got '$given_name...'";
 my $tree = $_[1];
 #die Dumper $tree;
 my $names = $_[2];
 my $cntref = $_[3];
 # принимает имя, возвр список имен у кот в parents есть это имя. Рекурсивно.
 if ( my @names = grep { $tree->{$_}{ parents }{ $given_name } } keys %{$tree} ){
    #warn "names:". scalar(%$names)." names: ".scalar( @names );
    #warn "\t ---> @names";
    my @rv = ( map { $names->{$_}=++$$cntref; $_ } grep {!$names->{$_}} $given_name, map { select_names($_,$tree,$names,$cntref) } @names  );
    #warn "active return: @rv" if @rv;
    return @rv;
 }else{
    my @rv = ( map { $names->{$_}=++$$cntref; $_ } grep {!$names->{$_}} $given_name );
    #warn "passive return: @rv" if @rv;
    return @rv;
 }
}

sub test{
 # testing files
 my $se = shift or die "SE!";
 my %proto_testers = (
    default => sub {
	# check any file for non-zero size, or *.gz files for 'size -gt 20'
	my $size=-s||0; m/\.gz$/ ? $size > 20 : $size 
	# Return true or false. 
    },
    dfs => sub{ system("bash","-c","hadoop fs -ls $_ | grep $_") },
 ); 
 if ( ref $_[0] eq "HASH" ){
    %proto_testers = ( %proto_testers, %{ shift() } );
 }

 my @fnames = @_;
 my @rv;

 for my $given_name ( @fnames ){
    my ( $proto, $name );
    if ( $given_name=~m|(\w+)(\:\/\/)(.+)| ){
	( $proto, $name )=($1,$3);
    }
    $proto||="default";
    $name||=$given_name;
    my $tester_code = $proto_testers{ $proto } or warn "Not found tester code for proto '$proto'. File:'$given_name'";
    ref $tester_code eq "CODE" or die "Tester code must be a CODE ref ".Dumper(\%proto_testers);
    for ( $name ){
	push @rv, $name ? join( " ", &$tester_code) : undef;
    }
 }

 return wantarray ? @rv : join " ",@rv;
}

sub days{
 my $se = shift or die "First param must be a reference to Geek object";
 #ref $se eq "Geek" or die "First param must be a reference to Geek object";
 my %pa = @_;
 my %allow = (from=>1, to=>1, n=>1, shift=>1);
 if ( my @bad_pars = grep { !$allow{$_} } keys %pa ){
    die "Bad parameters: @bad_pars. Allowed only: ".(join ", ", keys %allow);
 }
 my @rv;
 my $shift = $pa{"shift"}||0;
 if ($pa{from} and $pa{n}){
    my $n = $pa{n};
    my ($y,$m,$d)=$pa{from}=~/(\d\d\d\d)-(\d\d)-(\d\d)/;
    $y and $m and $d or die "bad parameter from=$pa{from}";
    my @ii = $n>0 ? ((0+$shift)..($n-1+$shift)) : (($n+$shift)..(-1+$shift));
    for my $i ( @ii ){
	push @rv, [ Date::Calc::Add_Delta_Days($y,$m,$d, $i) ];
    }
 }
 @rv = map {sprintf("%d-%02d-%02d",@$_)} @rv;
 my $days = Geek::Days->new('days');
 $days->days(\@rv);
 return $days;
}

sub hours{
 my $se = shift or die "First param must be a reference to Geek object";
 #ref $se eq "Geek" or die "First param must be a reference to Geek object";
 my %pa = @_;
 my %allow = (from=>1, to=>1, n=>1, shift=>1);
 if ( my @bad_pars = grep { !$allow{$_} } keys %pa ){
    die "Bad parameters: @bad_pars. Allowed only: ".(join ", ", keys %allow);
 }
 my @rv;
 my $shift = $pa{"shift"}||0;
 if ( $shift=~/(\-?\d+)\s*day/ ){
    $shift = $1*24
 }
 if ( $pa{from} and $pa{n} ){
    my $n = $pa{n};
    if ( $n=~/(\-?\d+)\s*day/ ){
	# param n may be set like: ... n=>"2days", ...
	$n = $1*24
    }
    my ($y,$m,$d,$h)=$pa{from}=~/(\d\d\d\d)-(\d\d)-(\d\d)\D?(\d+)?/;
    $h||="00";
    $y and $m and $d or die "bad parameter from=$pa{from}. y=$y, m=$m, d=$d, h=$h";
    my @ii = $n>0 ? ((0+$shift)..($n-1+$shift)) : (($n+$shift)..(-1+$shift));
    for my $i ( @ii ){
	#push @rv, [ Date::Calc::Add_Delta_Days($y,$m,$d, $i) ];
	push @rv, [ Date::Calc::Add_Delta_DHMS($y,$m,$d,$h,0,0, 0,$i,0,0) ];
    }
 }
 @rv = map {sprintf("%d-%02d-%02dT%02d",@$_)} @rv;
 my $hours = Geek::Hours->new('hours');
 $hours->hours(\@rv);
 return $hours;
}


sub named_groups{
 ref shift or die "First param must be a reference to Geek object";
 my %pa = @_;
 my %rv;
 for my $name (keys %pa){
    my $v = $pa{$name};
    #$rv{ $name } = qr/(?<$name>$pa{$name})/; 
    $rv{ $name } = "(?<$name>$pa{$name})"; 
 }
 return wantarray ? %rv : \%rv;
}

sub qb{
 my $se;
 if ( ref $_[0] eq "Geek" ){
    $se = shift;
 }
 my ($str, @pars) = @_;
 $str=~s/^\s+//mg; # remove tabs
 $str=~s/\s*$//mg; # remove empty strings
 $str=~s/\n/\\\n/mg; # backslash ends of lines
 $str=~s/\\$//; # ... exept last line
 if ( @pars ){
    $str = sprintf($str,@pars);
 }
 return $str;
}

sub desc(&){
 # Usage: desc {"my text"}
 # shortcut for desc => sub{"my text"}
 # описание смысла выполняемой команды ( cmd=>sub{} )
 # переданный код выполняется в момент сопоставления маршрута и должен возвращать строку.
 if (ref $_[0] ne "CODE"){ die "desc() - First param must be a CODEREF.".caller() }
 my $code = shift;
 return ("desc",$code);
}

sub bash(&){
 # usage bash { code }, где code - возвращает строку (команду для bash)
 # shortcut for: 'cmd=>sub{ code }'
 # принимfет блок кода
 # возвращает 2 эл-та: "cmd",sub (для выполнения в момент сопоставления маршрута)
 if (ref $_[0] ne "CODE"){ die "cmd() - First param must be a CODEREF.".caller() }
 my $cmd = shift;
 return ("cmd",$cmd);
}


sub perl(&){
 # usage perl { code }
 # shortcut for: 'cmd=>sub{ sub{ code } }'
 # принимfет блок кода
 # возвращает два эл-та: строку "cmd" и sub (для выполнения в момент сопоставления маршрута), внутри которого sub (для отложенного вызова $job->() )
 if (ref $_[0] ne "CODE"){ die "cmd() - First param must be a CODEREF.".caller() }
 my $cmd = shift;
 return ("cmd",sub { $cmd });
}

sub need(&){
 # usage need {code}
 # принимает блок которые вернет попарный список для хеш-массива
 # возвр: список из двух ел-тов: "need",{hash content}
 if (ref $_[0] ne "CODE"){ die "need() - First param must be a CODEREF.".caller() } 
 return ( need=>{ $_[0]->() } )
}

sub any(&){
 # usage any {code}
 # принимает блок который вернет попарный список для хеш-массива
 # возвр: список из двух ел-тов: "any",{hash content}
 if (ref $_[0] ne "CODE"){ die "any() - First param must be a CODEREF.".caller() }
 return ( any=>{ $_[0]->() } )
}

sub target($){
 # usage: target "string"
 # принимает строку
 # возвр: список из двух эл-тов: "target","string"
 if (ref $_[0]){ die "target() - is not for object-oriented calling.".caller() }
 return ("target",$_[0])
}

sub netflag{
 # usage netflag sub{code}, flag=>$flagname, viatmp=>$file, deb=>1, force=>1,
 #       netflag "bash code", ....
 # выполняет code, проверяя и устанавливая flag
 my %allow_pars = (
 deb=>"some warnings for debug",
 debug=>"same as 'deb'",
 set=>"STRING like 'set -x' or ARRAYREF like ['set -x','set -v'] or HASHREF like {'-o'=>'pipefail', } or CODEREF like sub{'set -x'}",
 viatmp=>"target filename like ./.../myresult.gz",
 flag=>"OPTIONAL. Flag file name like: ../result/myresult.gz.FLAG (BY DEFAULT = viatmp)",
 fl=>"like as flag",f=>"like as flag",
 force=>"force to rewrite 'viatmp' file",
# stderr=>"file name to redirect shell STDERR to it",
# stdout=>"like stderr but STDOUT",
 stdall=>"STDERR and STDOUT",
 );
 
 require Netflag;
 my $perlcode;
 my $cmd = shift;
 $cmd or die "netflag() : first param must be a CODEREF or bash code STRING";
 my %pa = @_;
 
 for my $p (keys %pa){
    if ( !$allow_pars{$p} ){
	die "Bad param '$p'. Allowed only:  ".Dumper(\%allow_pars);
    }
 }
 
 my @default_set = ('set -o pipefail');
 my $deb = $pa{debug}||$pa{deb};
# warn "debug=$deb" if $deb;
 
 my $f_ext = "FLAG";
 my $t_ext = "TMP";
 
 my $viatmp = $pa{viatmp};
 my $flag = $pa{flag}||$pa{fl}||$pa{f};
 
 $flag ||= $viatmp if $viatmp;
 $flag or die "param 'flag'|'viatmp' must be defined!";
 s/(\.$f_ext|\.$t_ext)$//i for grep {$_} ($flag, $viatmp); 
 
 my $force_rewrite;
 if ( !ref $pa{force} ){
    $force_rewrite = $pa{force}; # like: force=>1
 }elsif( ref $pa{force} eq "CODE" ){
    $force_rewrite = $pa{force}->( $viatmp ); # # like: force=>sub{ -s $_[0] < 20 }
    if ($deb){
	warn "force=$force_rewrite";
    }
 }
 
 { 
    for my $file ( grep {$_} $pa{stdall} ){
	my $dir = dirname( $file );
	if (!-s $dir){
	    make_path( $dir ) or die "Can't make_dir: $! ($dir)"
	}    
    }	
    
    #open my $err_fh, ">>", $pa{stderr} or die "Can't dup STDERR: $!" if $pa{stderr};
    #local *STDERR = $err_fh if $pa{stderr};
    
    
    if ( !ref $cmd ){ # as bash string: ------------------
    
	# может поступить команда с переносом строки:
	$cmd=~s/\s*$//mg; # remove empty string

	my $set = defined $pa{set} ? $pa{set}||"" : \@default_set;
	if (ref $set eq "ARRAY"){ $set = join("; ",@$set) }
	elsif( ref $set eq "CODE" ){ $set = join "; ", &$set }
	elsif( ref $set eq "HASH" ){ $set = join "; ", map {"set $_ $set->{$_}"} keys %$set }
	elsif(!ref $set){ 1 }
	else{ die "Param 'set' - is wrong type!".Dumper($set) }
    
	my $full_cmd = "$set; $cmd";
	
	if ( $viatmp ){
	    if ( $viatmp=~m/\.gz/ ){ $full_cmd = "set -o pipefail; ( $full_cmd ) | gzip > $viatmp.$t_ext && mv $viatmp.$t_ext $viatmp;" }
	    else{ $full_cmd = "( $full_cmd )> $viatmp;" }
	}
	
	if ( $pa{stdall} ){
	    $full_cmd = "( $full_cmd )>>$pa{stdall} 2>&1";
	}
	
	warn "CMD: $full_cmd" if $deb;
	$perlcode = sub{ 
	    system( 'bash', '-c', $full_cmd );
	    warn "exit code: $?, \$?>>8=".($?>>8) if $deb;
	    my $shell_ex_code = $?>>8;
	    my $rv = !$shell_ex_code + 0;
	    return wantarray ? ( $rv, "Shell exit code: $shell_ex_code" ) : $rv; 	
	}; # now 'cmd' is a CODEREF

    }elsif( ref $cmd eq "CODE" ){ # as perl code ---------------
	$perlcode = $cmd 
    }else{ # wrong
	die "Bad type of first param. ".Dumper($cmd).caller;
    }
 
    my $flag_full = "$flag.$f_ext";
    if ( -s $flag_full ){
	my ($info, $info_err) = Netflag::info( $flag_full );
	die "Can't do cmd '$cmd' becouse was errors while reading $flag_full: $info_err" if $info_err;
	my ($works, $works_err) = Netflag::works( $info );
	die "Can't check process working ($flag_full): $works_err" if $works_err;
	if ( $works ){
	    my $msg = "$flag_full exists and process is works.";
    	    warn $msg if $deb;
    	    return wantarray ? (-1, $msg) : -1;
	}
    }
    {
	my $flag_dir = dirname( $flag_full );
	make_path( $flag_dir ) or die "$! ($flag_dir)" if ! -d $flag_dir;
    }	
    
    my $will_do_it;
    $will_do_it = 1 if !-s $viatmp or $force_rewrite;
    if ( $will_do_it ){
	Netflag::set( $flag_full );
        my ($rv, $msg) = $perlcode->();
        Netflag::unset( $flag_full );

	return wantarray ? ($rv, $msg) : $rv;
    }else{
	# file exists. Not to do job.
	return wantarray ? (-2, "File $viatmp already exists.") : -2;
    }	
 }
}

#-----------------------------------------------------------------
package Geek::Days;
use base qw|Object::Accessor|;

sub files{
 my $se = shift or die "se!";
 my $tmpl = shift or die "tmpl!";
 my @rv;
 for my $d ( @{ $se->days() } ){
    my $file = $tmpl;
    $file=~s/\%F/$d/g;
    push @rv, $file;
 }
 if (wantarray){
    return @rv;
 }else{
    #my $files = Geek::Files->new('files');
    #$files->files(\@rv);
    #return $files;
    return \@rv;
 }
}

1;
#---------------------------------------------------
package Geek::Hours;
use base qw|Object::Accessor|;

sub files{
 my $se = shift or die "se!";
 my $tmpl = shift or die "tmpl!";
 my @rv;
 
 for my $hour ( @{ $se->hours() } ){
    my ($d,$h) = $hour=~/(\d\d\d\d-\d\d-\d\d)\D(\d\d)/;
    $d and $h or die "Bad hour!";
    if (!ref $tmpl){	
	my $file = $tmpl;
	$file=~s/\%F/$d/g;
	$file=~s/\%H/$h/;
	push @rv, $file;
    }elsif( ref $tmpl eq "CODE"){
	for ( $hour ){
	    push @rv, $tmpl->($hour, $d, $h); # можно вызывать files(sub{...}), $_=$hour @ARG=($hour,$d,$h)
	}    
    }else{
	die "Bad param for files():".Dumper($tmpl);
    }
 }
         
 if (wantarray){
    return @rv;
 }else{
    return \@rv;
 }
}



1;
