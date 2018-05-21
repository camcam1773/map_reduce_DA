
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
  map_reduce:map_reduce_par(fun map/2, 32, fun reduce/2, 32,[{Url,ok} || Url <- Urls]).

page_rank_dist() ->
  %erlang:set_cookie(node(),'yim'),
  %pool:start(mel,[]),
  %pool:attach('n1@127.0.0.1'),
  %pool:attach('n2@127.0.0.2'),
  %net_adm:ping('n1@127.0.0.1'),
  %net_kernel:connect_node('n2@127.0.0.2'),
  dets:open_file(web,[{file,"web.dat"}]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  map_reduce:map_reduce_dist(fun map/2, 32, fun reduce/2, 32,[{Url,ok} || Url <- Urls]).