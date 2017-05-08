{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module S3Parallel (PageResult(..), PageResultNew(..), S3Object(..), objectToS3Object) where

import           GHC.Generics (Generic)
import           Control.DeepSeq
import           Data.Text               (Text)
import           Network.AWS.S3
import           Control.Lens (view)

data S3Object = S3Object { s3ObjectKey :: Text } deriving (Show, Eq, Ord, Generic)
instance NFData S3Object
type PageResult = ([S3Object], Maybe (Text, Maybe Text))
type PageResultNew = ([S3Object], Maybe Text)

objectToS3Object :: Object -> S3Object
objectToS3Object o = S3Object { s3ObjectKey = view (oKey . _ObjectKey) o }
