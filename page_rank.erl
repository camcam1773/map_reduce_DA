
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

reduce(Url,Ns) ->
  [{Url,lists:sum(Ns)}].

page_rank() ->
  dets:open_file(web,[{file,"web.dat"}]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  map_reduce:map_reduce_seq(fun map/2, fun reduce/2,
    [{Url,ok} || Url <- Urls]).

page_rank_par() ->
  dets:open_file(web,[{file,"web.dat"}]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  map_reduce:map_reduce_par(fun map/2, 8, fun reduce/2, 8,[{Url,ok} || Url <- Urls]).

page_rank_dist() ->
  erlang:set_cookie(node(),'yim'),
  net_adm:ping('n1@127.0.0.1'),
  net_adm:ping('n2@127.0.0.2'),
  %net_adm:ping('n4@127.0.0.4'),
  dets:open_file(web,[{file,"web.dat"}]),
  %rpc:multicall(nodes(), dets, open_file, [web,[{file,"web.dat"}]]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  Web_reduced=map_reduce:map_reduce_dist(fun map/2, 16, fun reduce/2, 16,[{Url,ok} || Url <- Urls]),
  {_,_,Timestamp}=os:timestamp(),
  file:write_file("output_dist_"++integer_to_list(Timestamp)++".txt", io_lib:fwrite("~p.\n", [Web_reduced])),
  io:fwrite("All done! Check output file.\n").