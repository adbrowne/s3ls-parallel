module Main where

import Test.Tasty
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit
import S3Parallel (PageResultNew,S3Object(..))

import Data.List
import Data.Ord
import Data.Text (Text)
import qualified Data.Text as T

main = defaultMain tests

data PageRequest = PageRequest { startAfter :: Maybe Text }

type ProcessResult s = PageResultNew -> s -> ([S3Object], s, Maybe [PageRequest])

simulate :: Int -> [S3Object] -> s -> ProcessResult s -> (s, Int)
simulate requestTime items initialState onResult = error "todo"

tests :: TestTree
tests = testGroup "Tests" [unitTests]

unitTests = testGroup "Unit tests"
  [ testCase "Serial requests takes pages * time to complete" $
      let
        items = S3Object . T.pack . show <$> [1..2000]
        onResult (x, Nothing)    () = (x, (), Nothing)
        onReuslt (x, startAfter) () = (x, (), Just $ (PageRequest startAfter))
      in simulate 123 items () onResult @?= ((), 246)
  ]
