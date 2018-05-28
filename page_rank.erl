
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%% This implements a page rank algorithm using map-reduce
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%

-module(page_rank).
-compile(export_all).

%% Use map_reduce to count word occurrences
map(Url,ok) ->
  [{Url,Body}] = dets:lookup(web,Url),
  Urls = crawl:find_urls(Url,Body),[{U,1} || U <- Urls].

reduce(Url,Ns) -> [{Url,lists:sum(Ns)}].

page_rank() ->
  dets:open_file(web,[{file,"web.dat"}]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  map_reduce:map_reduce_seq(fun map/2, fun reduce/2, [{Url,ok} || Url <- Urls]).

page_rank_par() -> %Parallel Page rank
  dets:open_file(web,[{file,"web.dat"}]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  statistics(runtime),
  map_reduce:map_reduce_par(fun map/2, 16, fun reduce/2, 16,[{Url,ok} || Url <- Urls]),
  {_, T1} = statistics(runtime),
  io:fwrite("Runtime=~.3f  seconds~n",[T1/1000]).

page_rank_dist() -> %Distributed Page Rank
  dets:open_file(web,[{file,"web.dat"}]),
  %rpc:multicall(nodes(), dets, open_file, [web,[{file,"web.dat"}]]),
  %If you don't run the line below on the worker nodes, dets will throw an exception. Check the readme file.
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  statistics(runtime),
  Web_reduced=map_reduce:map_reduce_dist(fun map/2, 16, fun reduce/2, 16,[{Url,ok} || Url <- Urls]),
  {_, T1} = statistics(runtime),
  io:fwrite("Runtime=~.3f  seconds~n",[T1/1000]),
  {_,_,Timestamp}=os:timestamp(),
  Filename="output_dist_"++integer_to_list(Timestamp)++".txt",
  file:write_file(Filename, io_lib:fwrite("~p.\n", [Web_reduced])),
  io:fwrite("All done! Check output file: ~s.\n",[Filename]).