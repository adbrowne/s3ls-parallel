{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module S3Parallel
  (PageResult
  ,PageResultNew
  ,S3Object(..)
  ,PageRequest(..)
  ,objectToS3Object
  ,ProcessResult)
  where

import GHC.Generics (Generic)
import Control.DeepSeq
import Data.Text (Text)
import Network.AWS.S3
import Control.Lens (view)

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

{-
onResult :: ProcessResult Int
onResult (results, Nothing) currentThreads = (results, currentThreads, [])
onResult (results, Just startAfterNext) currentThreads =
  let maxThreads = 100
-}
objectToS3Object
    :: Object -> S3Object
objectToS3Object o =
    S3Object
    { s3ObjectKey = view (oKey . _ObjectKey) o
    }
