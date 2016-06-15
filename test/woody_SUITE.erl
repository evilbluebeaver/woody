-module(woody_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([test_set/1,
         test_unknown_update/1,
         test_unknown_query/1,
         test_invalid_update/1,
         test_unset/1,
         test_inc/1,
         test_sets/1,
         test_zsets/1,
         test_where/1,
         test_trie/1
        ]).

all() -> [test_set,
          test_unknown_update,
          test_unknown_query,
          test_invalid_update,
          test_unset,
          test_inc,
          test_sets,
          test_zsets,
          test_where,
          test_trie
         ].

test_set(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'SET', <<"1">>}},
    Tree1 = woody:update(Update, Tree),

    Query = #{1 => 'GET'},
    ExpectedData = #{1 => <<"1">>},
    ExpectedData = woody:encode(woody:query(Query, Tree1)),

    Tree2 = woody:update(Update, Tree1),

    Query2 = #{1 => 'GET', 2 => 'GET'},
    ExpectedData2 = #{1 => <<"1">>},
    ExpectedData2 = woody:encode(
                      woody:query(Query2, Tree2)),
    ok.

test_unknown_update(_Config) ->
    Tree = woody:new(),
    Update1 = #{1 => <<"1">>, 2 => <<"2">>},
    {error, unknown_update} = woody:update(Update1, Tree),
    Update2 = #{1 => {'Z_SETSCORE', 10, 'OLOLO'}},
    {error, unknown_update} = woody:update(Update2, Tree),
    ok.

test_unknown_query(_Config) ->
    Tree = woody:new(),
    Query = #{1 => <<"1">>, 2 => <<"2">>},
    {error, unknown_query} = woody:query(Query, Tree),
    ok.

test_invalid_update(_Config) ->
    Tree = woody:new(),
    Update = {'SET', <<"1">>},
    Tree1 = woody:update(Update, Tree),
    Update1 = #{1 => {'Z_SETSCORE', 10, 'UNSET'}},
    {error, invalid_update} = woody:update(Update1, Tree1),
    ok.

test_unset(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'SET', <<"1">>}},
    Tree1 = woody:update(Update, Tree),
    Update1 = #{1 => 'UNSET'},
    Tree = woody:update(Update1, Tree1),


    Update2 = #{1 => #{2 => {'SET', <<"2">>}}},
    Tree2 = woody:update(Update2, Tree),
    Update3 = #{1 => #{2 => 'UNSET'}},
    Tree = woody:update(Update3, Tree2),
    ok.

test_inc(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'INC', 1}},
    Tree1 = woody:update(Update, Tree),
    Tree2 = woody:update(Update, Tree1),

    Query = #{1 => 'GET'},
    ExpectedData = #{1 => 2},
    ExpectedData = woody:encode(
                     woody:query(Query, Tree2)),
    ok.

test_sets(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'S_ADD', <<"1">>}},
    Tree1 = woody:update(Update, Tree),
    Tree1 = woody:update(Update, Tree1),

    Query1 = 'GET',
    ExpectedData1 = #{1 => [<<"1">>]},
    ExpectedData1 = woody:encode(
                      woody:query(Query1, Tree1)),

    Update2 = #{1 => {'S_UNION', [<<"1">>]}},
    Tree3 = woody:update(Update2, Tree),
    Tree4 = woody:update(Update2, Tree3),
    ExpectedData1 = woody:encode(
                      woody:query(Query1, Tree4)),

    Query3 = #{1 => {'S_INTERSECTION', [<<"1">>, <<"2">>]}},
    ExpectedData3 = #{1 => [<<"1">>]},
    ExpectedData3 = woody:encode(
                      woody:query(Query3, Tree4)),

    Update4 = #{1 => {'S_REMOVE', <<"1">>}},
    Update5 = #{1 => {'S_SUBTRACT', [<<"1">>, <<"2">>]}},
    Update6 = #{1 => {'S_SUBTRACT', [<<"2">>]}},
    Tree = woody:update(Update4, Tree4),
    Tree = woody:update(Update5, Tree4),
    Tree = woody:update(Update5, Tree),
    Tree = woody:update(Update4, Tree),
    Tree1 = woody:update(Update6, Tree4),

    Update7 = #{1 => {'S_UPDATE', [<<"2">>], [<<"1">>]}},
    Tree7 = woody:update(Update7, Tree1),

    ExpectedData7 = #{1 => [<<"2">>]},
    ExpectedData7 = woody:encode(
                      woody:query(Query1, Tree7)),
    ok.

test_zsets(_Config) ->
    Tree = woody:new(),
    Update1 = #{1 => {'Z_SETSCORE', 10, {'SET', <<"1">>}}},
    Tree1 = woody:update(Update1, Tree),
    Tree1 = woody:update(Update1, Tree1),

    Query1 = #{1 => 'GET', 2 => 'GET'},
    ExpectedData1 = #{1 => <<"1">>},
    ExpectedData1 = woody:encode(
                      woody:query(Query1, Tree1)),

    Query2 = {'Z_TOP', 5, 'WITHSCORES'},
    ExpectedData2 = #{1 => {<<"1">>, 'WITHSCORE', 10}},
    ExpectedData2 = woody:encode(
                      woody:query(Query2, Tree1)),

    Query3 = {'Z_TOP', 5, 'FROMSCORE', 10, 'WITHSCORES'},
    ExpectedData2 = woody:encode(
                      woody:query(Query3, Tree1)),

    Query4 = {'Z_RANGE', 5, 10, 'WITHSCORES'},
    ExpectedData2 = woody:encode(
                      woody:query(Query4, Tree1)),

    Query5 = {'Z_PAGE', 5, 'FROMKEY', 1, 'WITHSCORES'},
    ExpectedData2 = woody:encode(
                      woody:query(Query5, Tree1)),

    Query6 = 'GET',
    ExpectedData1 = woody:encode(
                      woody:query(Query6, Tree1)),

    Update7 = #{1 => {'Z_SETSCORE', 10, 'UNSET'}},
    Tree = woody:update(Update7, Tree),

    Query8 = {'Z_TOP', 5},
    ExpectedData1 = woody:encode(
                      woody:query(Query8, Tree1)),

    Query9 = {'Z_TOP', 5, 'FROMSCORE', 10},
    ExpectedData1 = woody:encode(
                      woody:query(Query9, Tree1)),

    Query10 = {'Z_RANGE', 5, 10},
    ExpectedData1 = woody:encode(
                      woody:query(Query10, Tree1)),

    Query11 = {'Z_PAGE', 5, 'FROMKEY', 1},
    ExpectedData1 = woody:encode(
                      woody:query(Query11, Tree1)),
    Update12 = #{2 => {'Z_SETSCORE', 10, 'UNSET'}},
    Tree1 = woody:update(Update12, Tree1),

    Query13 =  {'GET', 'WITHSCORES'},
    ExpectedData2 = woody:encode(
                      woody:query(Query13, Tree1)),
    ok.

test_where(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'SET', 10}},
    Tree1 = woody:update(Update, Tree),

    Update1 = #{1 => {'WHERE', {'EQ', 10}}},
    Tree1 = woody:update(Update1, Tree1),

    Update2 = #{1 => {'WHERE', {'NE', 10}}},
    {error, predicate_failed} = woody:update(Update2, Tree1),

    Update3 = #{1 => {'WHERE', {'EQ', 10}, {'SET', 20}}},
    Tree2 = woody:update(Update3, Tree1),
    Query3 = 'GET',
    ExpectedData3 = #{1 => 20},
    ExpectedData3 = woody:encode(
                      woody:query(Query3, Tree2)),

    Update4 = #{1 => {'WHERE', {'NE', 10}, {'SET', 20}}},
    {error, predicate_failed} = woody:update(Update4, Tree1),

    Update5 = #{2 => {'WHERE', 'UNDEF'}},
    Tree1 = woody:update(Update5, Tree1),

    Update6 = #{1 => {'WHERE', {'GT', 5}}},
    Tree1 = woody:update(Update6, Tree1),

    Update7 = #{1 => {'WHERE', {'GTE', 10}}},
    Tree1 = woody:update(Update7, Tree1),

    Update8 = #{1 => {'WHERE', {'LT', 15}}},
    Tree1 = woody:update(Update8, Tree1),

    Update9 = #{1 => {'WHERE', {'LTE', 10}}},
    Tree1 = woody:update(Update9, Tree1),

    Update10 = #{1 => {'WHERE', {'AND',
                                 [{'GTE', 10},
                                  {'LTE', 10}]}}},
    Tree1 = woody:update(Update10, Tree1),

    Update11 = #{1 => {'WHERE', {'OR',
                                 [{'GT', 100},
                                  {'LT', 5}]}}},
    {error, predicate_failed} = woody:update(Update11, Tree1),

    Update12 = #{1 => {'WHERE', {'NOT', 'UNDEF'}}},
    Tree1 = woody:update(Update12, Tree1),

    SetsUpdate = #{1 => {'S_UNION', [1,2,3]}},
    SetsTree = woody:update(SetsUpdate, Tree),

    SetsUpdate1 = #{1 => {'WHERE', {'S_IN', 1}}},
    SetsTree = woody:update(SetsUpdate1, SetsTree),

    SetsUpdate2 = #{1 => {'WHERE', {'S_SUBSET', [1, 2]}}},
    SetsTree = woody:update(SetsUpdate2, SetsTree),

    SetsUpdate3 = #{1 => {'WHERE', {'NOT', 'S_EMPTY'}}},
    SetsTree = woody:update(SetsUpdate3, SetsTree),

    SetsUpdate4 = #{2 => {'WHERE', 'S_EMPTY'}},
    SetsTree = woody:update(SetsUpdate4, SetsTree),

    SetsUpdate5 = #{1 => {'WHERE', 'UNKNOWN'}},
    {error, predicate_failed} = woody:update(SetsUpdate5, SetsTree),

    SetsUpdate6 = #{1 => {'WHERE', {'S_LEN', {'EQ', 3}}}},
    SetsTree = woody:update(SetsUpdate6, SetsTree),

    SetsUpdate7 = #{2 => {'WHERE', {'S_LEN', {'EQ', 0}}}},
    SetsTree = woody:update(SetsUpdate7, SetsTree),
    ok.

test_trie(_Config) ->
    Tree = woody:new(),
    GetQuery = 'GET',
    Update1 = #{"prefix" => {'T_PREFIX', {'SET', <<"1">>}}},
    Tree1 = woody:update(Update1, Tree),
    Tree1 = woody:update(Update1, Tree1),
    Expected1 = #{"prefix" => <<"1">>},
    Expected1 = woody:encode(woody:query(GetQuery, Tree1)),

    Update2 = #{"prefix" => {'T_PREFIX', 'UNKNOWN'}},
    {error, unknown_update} = woody:update(Update2, Tree),

    Update3 = #{"prefix2" => {'T_PREFIX', {'SET', <<"2">>}}},
    Tree3 = woody:update(Update3, Tree1),
    Expected3 = #{"prefix" => <<"1">>,
                  "prefix2" => <<"2">>},
    Expected3 = woody:encode(woody:query(GetQuery, Tree3)),

    Update4 = #{"prefix2" => {'T_PREFIX', 'UNSET'}},
    Tree4 = woody:update(Update4, Tree3),
    Expected1 = woody:encode(woody:query(GetQuery, Tree4)),

    Update5 = #{"prefix" => {'T_PREFIX', 'UNSET'}},
    Tree5 = woody:update(Update5, Tree4),
    undefined = woody:encode(woody:query(GetQuery, Tree5)),

    Query6 = #{"prefix" => 'GET'},
    Expected6 = #{"prefix" => <<"1">>},
    Expected6 = woody:encode(woody:query(Query6, Tree3)),

    Query7 = #{"unknown_prefix" => 'GET'},
    Expected7 = #{},
    Expected7 = woody:encode(woody:query(Query7, Tree3)),

    Update8 = #{"prefix" => {'T_PREFIX', #{1 => {'SET', <<"11">>},
                                            2 => {'SET', <<"21">>}}},
                "prefix2" => {'T_PREFIX', #{1 => {'SET', <<"12">>},
                                            2 => {'SET', <<"22">>}}}},
    Tree8 = woody:update(Update8, Tree),

    Query9 = {'T_SIMILAR', "prefix", #{1 => 'GET'}},
    Expected9 = #{"prefix" => #{1 => <<"11">>},
                  "prefix2" => #{1 => <<"12">>}},
    Expected9 = woody:encode(woody:query(Query9, Tree8)),

    Query10 = {'T_SIMILAR', "prefix2", #{1 => 'GET'}},
    Expected10 = #{"prefix2" => #{1 => <<"12">>}},
    Expected10 = woody:encode(woody:query(Query10, Tree8)),

    ok.
