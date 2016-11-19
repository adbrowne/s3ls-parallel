{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns      #-}

module Main where

import           Control.Lens
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.AWS
import           Control.Monad.Trans.Resource
import           Data.Char (chr,ord)
import           Data.ByteString         (ByteString)
import           Data.Conduit
import qualified Data.Conduit.Binary     as CB
import qualified Data.Conduit.List       as CL
import qualified Data.Foldable           as Fold
import           Data.Monoid
import           Data.Text               (Text)
import qualified Data.Text.IO            as Text
import           Data.Time
import           Network.AWS.Data
import           Network.AWS.S3
import           System.IO
import qualified Data.Text as T


ordText :: Text -> Int
ordText = ord . T.head

chrText :: Int -> Text
chrText = T.pack . (: []) . chr

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
-- >>> splitKeySpace 2 (Just "sa", Just "sc")
-- [(Just "sa",Just "sb"),(Just "sb",Just "sc")]
--
-- >>> splitKeySpace 3 (Nothing, Nothing)
-- [(Nothing,Just "*"),(Just "*",Just "T"),(Just "T",Nothing)]
splitKeySpace :: Int -> (Maybe Text, Maybe Text) -> [(Maybe Text, Maybe Text)]
splitKeySpace 1 s = [s]
splitKeySpace n (startKey, endKey) =
  let
    commonPrefix = join $ T.commonPrefixes <$> startKey <*> endKey
    end = maybe endKey (\(c,s,e) -> Just e) commonPrefix
    start = maybe startKey (\(c,s,e) -> Just s) commonPrefix
    prefix = maybe "" (\(c,_,_) -> c) commonPrefix
    maxKeys = maybe 127 ordText end
    minKeys = maybe 0 ordText start
    stepSize = (maxKeys - minKeys) `div` n
    segments = take (n - 1) $ chrText <$> [minKeys + stepSize,minKeys + stepSize*2..]
    startItems = ((prefix <>) <$>) <$> start : (Just <$> segments)
    endItems = ((prefix <>) <$>) <$> (Just <$> segments) ++ [end]
  in
    zip startItems endItems

listAll :: Region -- ^ Region to operate in.
        -> IO ()
listAll r = do
    lgr <- newLogger Debug stdout
    env <- newEnv Discover <&> set envLogger lgr . set envRegion r

    let val :: ToText a => Maybe a -> Text
        val   = maybe "Nothing" toText

        lat v = maybe mempty (mappend " - " . toText) (v ^. ovIsLatest)
        key v = val (v ^. ovKey) <> ": " <> val (v ^. ovVersionId) <> lat v

    runResourceT . runAWST env $ go (Just "h")
  where
    go :: Maybe Text -> AWST (ResourceT IO) ()
    go marker = do
        say "Listing Items .."
        let request =
              listObjectsV (BucketName "elevation-tiles-prod")
              & lStartAfter .~ marker
        response <- send request
        let bs = view lrsContents response
        say $ "Found " <> toText (length bs) <> " Objects."
        say $ "Marker:" <> toText (maybe "EMPTY" id marker)
        if (view lrsIsTruncated response == Just True) then
          go $ Just (view (oKey . _ObjectKey) $ last bs)
        else
          return ()

main :: IO ()
main = listAll NorthVirginia

say :: MonadIO m => Text -> m ()
say = liftIO . Text.putStrLn
