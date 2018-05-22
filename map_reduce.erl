%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% %
%% This is a very simple implementation of map-reduce, in both
%% sequential and parallel versions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% %
-module(map_reduce).
-compile(export_all).

map_reduce_seq(Map,Reduce,Input) ->
  Mapped = [{K2,V2} || {K,V} <- Input,
    {K2,V2} <- Map(K,V)],
  reduce_seq(Reduce,Mapped).

reduce_seq(Reduce,KVs) ->
  [KV || {K,Vs} <- group(lists:sort(KVs)),KV <- Reduce(K,Vs)].

group([]) -> [];
group([{K,V}|Rest]) -> group(K,[V],Rest).

group(K,Vs,[{K,V}|Rest]) -> group(K,[V|Vs],Rest);
group(K,Vs,Rest) -> [{K,lists:reverse(Vs)}|group(Rest)].

map_reduce_par(Map,M,Reduce,R,Input) ->
  Parent = self(),
  Splits = split_into(M,Input),
  Mappers = [spawn_mapper(Parent,Map,R,Split) || Split <- Splits],
  Mappeds = [receive {Pid,L} -> L end || Pid <- Mappers],
  Reducers = [spawn_reducer(Parent,Reduce,I,Mappeds) || I <- lists:seq(0,R-1)],
  Reduceds = [receive {Pid,L} -> L end || Pid <- Reducers],
  Web_reduced=lists:sort(lists:flatten(Reduceds)),
  file:write_file("output_par.txt", io_lib:fwrite("~p.\n", [Web_reduced])).

spawn_mapper(Parent,Map,R,Split) ->
  spawn_link(fun() ->
    Mapped = [{erlang:phash2(K2,R),{K2,V2}} || {K,V} <- Split,{K2,V2} <- Map(K,V) ],
    Parent ! {self(),group(lists:sort(Mapped))}
             end).

split_into(N,L) -> split_into(N,L,length(L)).

split_into(1,L,_) -> [L];
split_into(N,L,Len) ->
  {Pre,Suf} = lists:split(Len div N,L),[Pre|split_into(N-1,Suf,Len-(Len div N))].

spawn_reducer(Parent,Reduce,I,Mappeds) ->
  Inputs = [KV || Mapped <- Mappeds,{J,KVs} <- Mapped,I==J,KV <- KVs],
  spawn_link(fun() ->
    Parent ! {self(), reduce_seq(Reduce,Inputs)}
             end).

%% Distributed Map-Reduce
map_reduce_dist(Map,M,Reduce,R,Input) ->
  Parent = self(),
  Splits = split_into(M,Input),
  io:fwrite("Splitting done. Starting mapping...\n"),
  Mappers = [dist_mapper(Parent,Map,R,Split) || Split <- Splits],
  Mappeds = workers(Mappers),
  io:fwrite("Mapping done. Starting reduce...\n"),
  Reducers = [dist_reducer(Parent,Reduce,I,Mappeds) || I <- lists:seq(0,R-1)],
  Reduceds = workers(Reducers),

  Web_reduced=lists:sort(lists:flatten(Reduceds)),
  file:write_file("output_dist.txt", io_lib:fwrite("~p.\n", [Web_reduced])).

dist_reducer(Parent,Reduce,I,Mappeds) ->
  Inputs = [KV || Mapped <- Mappeds, {J,KVs} <- Mapped, I==J, KV <- KVs],
  fun() ->
    Parent ! {self(),reduce_seq(Reduce,Inputs)}
  end.

dist_mapper(Parent,Map,R,Split) ->
  Mapped = [{erlang:phash2(K2,R),{K2,V2}} || {K,V} <- Split, {K2,V2} <- Map (K,V) ],
  fun() ->
    Parent ! {self(),group(lists:sort(Mapped))}
  end.

%Initialize worker pool
workers(Functions) -> workers(Functions,nodes(),#{},[]).

workers([Function|Functions],[Node| Free_nodes], Busy, Return) ->
  % Assign jobs to free nodes
  spawn(Node,Function),
  Temp=maps:put(Node, Function, Busy),
  workers(Functions, Free_nodes, Temp, Return);

workers(Functions, [], Busy, Return) ->
  % When a node is done, reassign a job to it.
  receive {Pid,L} ->
    %workers(Function, [Node], [{N,Funct}||{N,Funct}<-Busy, N =/= Node]),
    io:fwrite("Node ~w finished its work, there are ~w chunks remaining\n",[node(Pid), length(Functions)]),
    Busy2 =maps:remove(node(Pid),Busy),
    Return2=lists:append(Return,[L]),
    workers(Functions,[node(Pid)], Busy2,Return2)
  end;

workers([],_,#{},Return) -> Return;

workers([], Nodes, Busy, Return)  ->
  io:format("Waiting for processes to finish\n"),
  receive {Pid,L} ->
    Nodes2=lists:append(Nodes,node(Pid)),
    Return2=lists:append(Return,[L]),
    Busy2 =maps:remove(node(Pid),Busy),
    workers([], Nodes2, Busy2, Return2)
  %after 5000 ->
  %  workers()
  end.
