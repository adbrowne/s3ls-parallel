{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module S3Parallel
  (PageResult
  ,PageResultNew
  ,S3Object(..)
  ,PageRequest(..)
  ,objectToS3Object
  ,splitKeySpace
  ,SearchBounds
  ,ProcessResult)
  where

import GHC.Generics (Generic)
import Control.Lens (view)
import Control.DeepSeq
import Data.Text (Text)
import qualified Data.Text as T
import           Data.Char (chr,ord)
import Network.AWS.S3
import           Control.Monad
import           Data.Monoid

data S3Object = S3Object
    { s3ObjectKey :: Text
    } deriving (Show,Eq,Ord,Generic)

instance NFData S3Object

type PageResult = ([S3Object], Maybe (Text, Maybe Text))

type PageResultNew rs = ([S3Object], Maybe Text, rs)

type ProcessResult s rs = PageResultNew rs -> s -> ([S3Object], s, [PageRequest rs])

data PageRequest rs = PageRequest
    { startAfter :: Maybe Text
    , requestState :: rs
    } deriving (((Show)))

type SearchBounds = (Maybe Text, Maybe Text)

ordText :: Text -> Int
ordText = ord . T.head

chrText :: Int -> Text
chrText = T.pack . (: []) . chr

-- todo add property to check end > start in all cases
-- todo add test for all common prefix

-- |
-- >>> :set -XOverloadedStrings
--
-- | Split a keyspace into smaller segments
--
-- Examples:
--
-- >>> splitKeySpace 1 (Nothing, Nothing)
-- [(Nothing,Nothing)]
--
-- >>> splitKeySpace 2 (Nothing, Nothing)
-- [(Nothing,Just "?"),(Just "?",Nothing)]
--
-- >>> splitKeySpace 2 (Just "a", Just "c")
-- [(Just "a",Just "b"),(Just "b",Just "c")]
--
-- >>> splitKeySpace 2 (Just "s", Just "t")
-- [(Just "s",Just "s?"),(Just "s?",Just "t")]
--
-- >>> splitKeySpace 2 (Just "sa", Just "sc")
-- [(Just "sa",Just "sb"),(Just "sb",Just "sc")]
--
-- >>> splitKeySpace 2 (Just "sab", Just "sb")
-- [(Just "sab",Just "sap"),(Just "sap",Just "sb")]
--
-- >>> splitKeySpace 10 (Just "geotiff/10/1/950.tif",Just "i")
-- [(Just "geotiff/10/1/950.tif",Just "h"),(Just "h",Just "i")]
--
-- >>> splitKeySpace 3 (Nothing, Nothing)
-- [(Nothing,Just "*"),(Just "*",Just "T"),(Just "T",Nothing)]
splitKeySpace :: Int -> (Maybe Text, Maybe Text) -> [(Maybe Text, Maybe Text)]
splitKeySpace 1 s = [s]
splitKeySpace n (startKey, endKey) =
  let
    sharedPrefix = join $ T.commonPrefixes <$> startKey <*> endKey
    end = maybe endKey (\(_,_,e) -> Just e) sharedPrefix
    start = maybe startKey (\(_,s,_) -> Just s) sharedPrefix
    prefix = maybe "" (\(c,_,_) -> c) sharedPrefix
    initialMaxKeys = maybe 127 ordText end
    initialMinKeys = maybe 0 ordText start
    (minKeys, maxKeys, startPrefix) = getStartPrefix initialMinKeys initialMaxKeys start
    stepSize = max 1 $ (maxKeys - minKeys) `div` n
    segments = take (n - 1) $ (startPrefix <>) . chrText <$> [minKeys + stepSize,minKeys + stepSize*2..]
    segmentsFilteredByEnd =
      case end of Nothing -> segments
                  Just e  -> filter (< e) segments
    startItems = ((prefix <>) <$>) <$> start : (Just <$> segmentsFilteredByEnd)
    endItems = ((prefix <>) <$>) <$> (Just <$> segmentsFilteredByEnd) ++ [end]
  in
    zip startItems endItems
  where
    getStartPrefix :: Int -> Int -> Maybe Text -> (Int, Int, Text)
    getStartPrefix initialMinKeys initialMaxKeys Nothing =
        (initialMinKeys, initialMaxKeys, "")
    getStartPrefix initialMinKeys initialMaxKeys (Just start) =
      if initialMaxKeys - initialMinKeys == 1 then
        if T.length start < 2 then
          (0, 127, start)
        else
          (ordText $ T.tail start, 127, T.take 1 start)
      else
        (initialMinKeys, initialMaxKeys, "")

filterEndBounds :: SearchBounds -> [S3Object] -> [S3Object]
filterEndBounds (_, Nothing) = id
filterEndBounds (_, Just endKey) =
  filter (\o -> s3ObjectKey o <= endKey)

onResult :: ProcessResult Int SearchBounds
onResult (results, Nothing, searchBounds) currentThreads =
  let
    filteredResults = filterEndBounds searchBounds results
  in (filteredResults, currentThreads, [])
onResult (results, Just startAfterNext, searchBounds) currentThreads =
  let
    maxThreads = 100
    filteredResults = filterEndBounds searchBounds results
    newRequests = if currentThreads < maxThreads then
                    let
                      subSpaces = splitKeySpace 10 (Just startAfterNext, snd searchBounds)
                    in (\(s,e) -> PageRequest s (s,e)) <$> subSpaces
                  else
                    []
    newRequestCount = currentThreads - 1 + length newRequests
  in (filteredResults, newRequestCount, newRequests)

objectToS3Object
    :: Object -> S3Object
objectToS3Object o =
    S3Object
    { s3ObjectKey = view (oKey . _ObjectKey) o
    }
