%%% @author Gianfranco <zenon@zen.local>
%%% @copyright (C) 2010, Gianfranco
%%% Created : 26 Dec 2010 by Gianfranco <zenon@zen.local>
-module(wn_job_layer_tests).
-include_lib("eunit/include/eunit.hrl").
-include("include/worker_net.hrl").
-define(NODE_ROOT,
	"/Users/zenon/ErlangBlog/worker_net-0.1/node_root/").

local_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Can register locally", fun register_locally/0},
      {"Executed locally", fun executed_locally/0},
      {"Executed queue", fun executed_queue/0},
      {"Queueus on resource type amount", fun queues_on_resource_types_amount/0},
      {"Canceled in queue", fun cancel_before_running/0},
      {"Done Job Stored in file layer", fun stored_in_file_layer/0},
      {"Done Job canceled", fun done_job_deleted/0}
      ]}.

done_job_deleted() ->
    wn_resource_layer:register(#wn_resource{name = "Laptop",
					    type = [{perl,1}],
					    resides = node()
					   }),
    Path = create_file_at(?NODE_ROOT),
    File1 = #wn_file{id = "File1",file = Path,resides = node()},
    Job1 = #wn_job{id = "JobId",
		   files = [File1],
		   resources = [perl],
		   commands = ["perl -e 'print(\"HelloWorld\n\")'"],
		   timeout = 1000
		  },
    ok = wn_job_layer:register(Job1),
    ok = wn_job_layer:stream(user,"JobId"),    
    timer:sleep(500),
    ok = wn_job_layer:delete("JobId"),
    ?assertMatch([#wn_file{id="File1"}],wn_file_layer:list_files()),
    ?assertEqual([],wn_job_layer:list_all_jobs()),
    ok.

stored_in_file_layer() ->
    %% (1)
    wn_resource_layer:register(#wn_resource{name = "Laptop",
					    type = [{laptop,1}],
					    resides = node()
					   }),
    %% (2)
    Path = create_file_at(?NODE_ROOT),
    %% (3)
    File1 = #wn_file{id = "File1",file = Path,resides = node()},
    Job1 = #wn_job{id = "JobId",
		   files = [File1],
		   resources = [laptop],
		   commands = ["cat EunitFile","touch MadeFile"],
		   timeout = 100
		  },
    %% (4)
    ok = wn_job_layer:register(Job1),
    %% (5)
    timer:sleep(500),
    
    %% (6)
    {ok,T} = wn_job_layer:finished_at("JobId"),
    
    %% (7)
    TimeSuffix = wn_util:time_marker_to_string(T),
    ExpectedFileName = "JobId_result_"++TimeSuffix++".tgz",
    ExpectedFileId = "JobId_result",
    ExpectedFiles = [F1,F2,F3] = ["EUnitFile","MadeFile","Log.txt"],

    %% (8)
    {ok,ResultId} = wn_job_layer:stored_result("JobId"),
    ?assertEqual(ExpectedFileId,ResultId),

    %% (9)
    [_,Res] = wn_file_layer:list_files(),
    ?assertEqual(ExpectedFileName,filename:basename(Res#wn_file.file)),
    ?assertEqual(ExpectedFileId,Res#wn_file.id),

    %% (10)
    {ok,Tar} = wn_file_layer:retrieve_file(node(),ResultId),
    ?assertMatch({ok,ExpectedFiles},erl_tar:table(Tar,[compressed])),

    %% (11)
    erl_tar:extract(Tar,[{files,[F3]},compressed]),
    {ok,IoDev} = file:open("local_stream.txt",[write]),
    ok  = wn_job_layer:stream(IoDev,"JobId"),
    timer:sleep(100),
    file:close(IoDev),
    {ok,LocalStreamBin} = file:read_file("local_stream.txt"),
    {ok,JobLogBin} = file:read_file(F3),
    ?assertEqual(LocalStreamBin,JobLogBin),    

    %% (12)
    erl_tar:extract(Tar,[{files,[F1]},compressed]),
    {ok,LocalEunitBin} = file:read_file(Path),
    {ok,JobEunitBin} = file:read_file(F1),
    ?assertEqual(LocalEunitBin,JobEunitBin),

    %% (13)
    erl_tar:extract(Tar,[{files,[F2]},compressed]),
    ?assertEqual({ok,<<>>},file:read_file(F2)),

    %% (14)
    ok = file:delete("local_stream.txt"),
    ok = file:delete(F1),
    ok = file:delete(F2),
    ok = file:delete(F3),    
    ok = file:delete(Tar).


queues_on_resource_types_amount() ->
    wn_resource_layer:register(#wn_resource{name = "Laptop",
					    type = [{a,0},{b,0}],
					    resides = node()
					   }),
    Queued = fun() -> wn_resource_layer:queued(node(),"Laptop") end,
    Job1 = #wn_job{id = "JobId",files = [],resources = [a]},
    Job2 = Job1#wn_job{id = "JobId2", resources = [a]},
    Job3 = Job1#wn_job{id = "JobId3", resources = [a,b]},
    
    wn_job_layer:register(Job1),
    ?assertMatch({ok,[{a,[Job1]},{b,[]}]},Queued()),
    
    wn_job_layer:register(Job2),
    ?assertMatch({ok,[{a,[Job1, Job2]},{b,[]}]},Queued()),
    
    wn_job_layer:register(Job3),
    ?assertMatch({ok,[{b,[Job3]},
                      {a,[Job1,Job2,Job3]}
                     ]},Queued()).

cancel_before_running() ->
    wn_resource_layer:register(#wn_resource{name = "Laptop",
					    type = [{'os-x',0}],
					    resides = node()
					   }),
    Job1 = #wn_job{id = "JobId",
		   files = [],
		   resources = ['os-x'],
		   commands = ["echo hello"]},
    ?assertEqual(ok,wn_job_layer:register(Job1)),
    ?assertEqual({ok,[{'os-x',[Job1]}]},wn_resource_layer:queued(node(),"Laptop")),
    [Res] = wn_job_layer:list_all_jobs(),
    ?assertEqual("JobId",Res#wn_job.id),
    ?assertEqual(['os-x'],Res#wn_job.resources),
    ?assertEqual([],Res#wn_job.files),
    ?assertEqual(["echo hello"],Res#wn_job.commands),
    ?assertEqual(ok,wn_job_layer:cancel("JobId")),
    [] = wn_job_layer:list_all_jobs(),
    ?assertEqual({ok,[{'os-x',[]}]},wn_resource_layer:queued(node(),"Laptop")).

executed_queue() ->
    wn_resource_layer:register(#wn_resource{name = "Laptop",
					    type = [{'os-x',1}],
					    resides = node()
					   }),
    Path = create_file_at(?NODE_ROOT),
    File1 = #wn_file{id = "File1",file = Path,resides = node()},
    Job1 = #wn_job{id = "JobId",
		   files = [File1],
		   resources = ['os-x'],
		   commands = ["file EunitFile"],
		   timeout = 100
		  },
    Job2 = Job1#wn_job{id = "JobId2",
		       files = [File1#wn_file{id="File"}],
		       commands = ["cat EUnitFile"]},
    ?assertEqual(ok,wn_job_layer:register(Job1)),
    ?assertEqual(ok,wn_job_layer:register(Job2)),
    ok  = wn_job_layer:stream(user,"JobId"),
    ok  = wn_job_layer:stream(user,"JobId2"),
    timer:sleep(100),
    ?assertEqual({ok,["EunitFile: ASCII text"]},wn_job_layer:result("JobId")),
    timer:sleep(1000),
    ?assertEqual({ok,["1,2,3"]},wn_job_layer:result("JobId2")),
    {ok,T1} = wn_job_layer:finished_at("JobId"),
    {ok,T2} = wn_job_layer:finished_at("JobId2"),
    ?assertEqual(true,T1 < T2).

executed_locally() ->
    wn_resource_layer:register(#wn_resource{name = "Laptop",
					    type = [{'os-x',infinity}],
					    resides = node()
					   }),
    Path = create_file_at(?NODE_ROOT),
    File1 = #wn_file{id = "File1",file = Path,resides = node()},
    Job1 = #wn_job{id = "JobId",
		   files = [File1],
		   resources = ['os-x'],
		   commands = ["more EUnitFile"],
		   timeout = 100
		  },
    ?assertEqual(ok,wn_job_layer:register(Job1)),
    ok  = wn_job_layer:stream(user,"JobId"),
    timer:sleep(1000),
    ?assertEqual({ok,["1,2,3"]},wn_job_layer:result("JobId")).

register_locally() ->
    Path = create_file_at(?NODE_ROOT),
    File1 = #wn_file{id = "File1",file = Path,resides = node()},
    File2 = #wn_file{id = "File2",file = Path,resides = node()},
    Job = #wn_job{id = "JobId",
		  files = [File1,File2],
		  resources = ['non-existent'],
		  commands = ["ls -l"]
		 },
    ?assertEqual(ok,wn_job_layer:register(Job)),
    [Res] = wn_job_layer:list_all_jobs(),
    ?assertEqual("JobId",Res#wn_job.id),
    ?assertEqual(['non-existent'],Res#wn_job.resources),
    ?assertEqual([File1,File2],Res#wn_job.files),
    ?assertEqual(["ls -l"],Res#wn_job.commands).

%% -----------------------------------------------------------------
setup() ->
    {ok,_} = net_kernel:start([eunit_resource,shortnames]),
    erlang:set_cookie(node(),eunit),
    {ok,_} = wn_file_layer:start_link(?NODE_ROOT),
    {ok,_} = wn_resource_layer:start_link(?NODE_ROOT),
    {ok,_} = wn_job_layer:start_link(),
    ok.

cleanup(_) ->
    clean_up(?NODE_ROOT),    
    ok = net_kernel:stop(),
    ok = wn_file_layer:stop(),
    ok = wn_resource_layer:stop(),
    ok = wn_job_layer:stop().

create_file_at(X) ->
    Path = X++"EUnitFile",
    ok = filelib:ensure_dir(X),
    ok = file:write_file(Path,"1,2,3\n"),
    Path.

clean_up(X) ->
    case filelib:is_dir(X) of
	true ->
	    {ok,Files} = file:list_dir(X),
	    lists:foreach(
	      fun(File) ->
		      clean_up(X++"/"++File)
	      end,Files),
	    file:del_dir(X);
	false ->
	    ok = file:delete(X)
    end.
