{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Test.Tasty
import Test.Tasty.HUnit
import S3Parallel (PageResultNew,S3Object(..),PageRequest(..), ProcessResult)

import Data.List
import qualified Data.Text as T

main :: IO ()
main = defaultMain tests

getResults :: [S3Object] -> PageRequest rs -> PageResultNew rs
getResults items PageRequest{..} =
  let
    maxPageSize = 1000
    results = take maxPageSize . dropBeforeStart . sort $ items
    next = if length results < maxPageSize then
             Nothing
           else
             Just $ s3ObjectKey (last results)
  in (results, next, requestState)
  where
    dropBeforeStart =
      case startAfter of Nothing      ->
                           id
                         (Just start) ->
                           dropWhile (\S3Object{..} -> s3ObjectKey <= start)

simulate :: Int -> [S3Object] -> [PageRequest rs] -> s -> ProcessResult s rs -> (s, Int)
simulate requestTime items initialRequests initialState onResult =
  loop initialState (appendRequestTime requestTime <$> initialRequests) 0
  where
    appendRequestTime t x = (t,x)
    loop state [] time = (state, time)
    loop state requests _ =
      let
        sortedRequests = sortOn fst requests
        (time, nextReq) = head sortedRequests
        remainingRequests = tail sortedRequests
        nextResult = getResults items nextReq
        (_results, state', newRequests) = onResult nextResult state
        newRequestsWithTime = appendRequestTime (time + requestTime) <$> newRequests
        requests' = remainingRequests ++ newRequestsWithTime
      in loop state' requests' time

tests :: TestTree
tests = testGroup "Tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [ testCase "Serial requests takes pages * time to complete" $
      let
        items = S3Object . T.pack . show <$> [(1::Integer)..1999]
        onResult (x, Nothing, ())    () = (x, (), [])
        onResult (x, startAfter, ()) () = (x, (), [PageRequest startAfter ()])
      in simulate 123 items [PageRequest Nothing ()] () onResult @?= ((), 246)
  , testCase "Initial requests are done in parallel" $
      let
        items = S3Object . T.pack . show <$> [(1::Integer)..1999]
        onResult (x, _, _)    () = (x, (), [])
        requests = [PageRequest Nothing (), PageRequest (Just "1000") ()]
      in simulate 123 items requests () onResult @?= ((), 123)
  ]
