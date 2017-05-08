{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Test.Tasty
import Test.Tasty.HUnit
import S3Parallel (PageResultNew,S3Object(..))

import Data.List
import Data.Text (Text)
import qualified Data.Text as T

main :: IO ()
main = defaultMain tests

data PageRequest = PageRequest { startAfter :: Maybe Text }
  deriving (Show)

type ProcessResult s = PageResultNew -> s -> ([S3Object], s, [PageRequest])

getResults :: [S3Object] -> PageRequest -> PageResultNew
getResults items PageRequest{..} =
  let
    maxPageSize = 1000
    results = take maxPageSize . dropBeforeStart . sort $ items
    next = if length results < maxPageSize then
             Nothing
           else
             Just $ s3ObjectKey (last results)
  in (results, next)
  where
    dropBeforeStart =
      case startAfter of Nothing      ->
                           id
                         (Just start) ->
                           dropWhile (\S3Object{..} -> s3ObjectKey <= start)

simulate :: Int -> [S3Object] -> [PageRequest] -> s -> ProcessResult s -> (s, Int)
simulate requestTime items initialRequests initialState onResult =
  loop initialState (appendRequestTime 0 <$> initialRequests) 0
  where
    appendRequestTime t x = (t,x)
    loop state [] time = (state, time)
    loop state ((_,nextReq):requests) time =
      let
        nextResult = getResults items nextReq
        (_results, state', newRequests) = onResult nextResult state
        newRequestsWithTime = appendRequestTime time <$> newRequests
        requests' = requests ++ newRequestsWithTime
        time' = time + requestTime
      in loop state' requests' time'

tests :: TestTree
tests = testGroup "Tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [ testCase "Serial requests takes pages * time to complete" $
      let
        items = S3Object . T.pack . show <$> [(1::Integer)..1999]
        onResult (x, Nothing)    () = (x, (), [])
        onResult (x, startAfter) () = (x, (), [PageRequest startAfter])
      in simulate 123 items [PageRequest Nothing] () onResult @?= ((), 246)
  ]
