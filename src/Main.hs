{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Debug.Trace
import           Control.Lens
import           Control.Concurrent (threadDelay)
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.AWS
import           Control.Monad.Trans.Resource
import           Data.Char (chr,ord)
import           Data.ByteString         (ByteString)
--import           Data.Conduit
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
import qualified Pipes.Prelude as P
import           Pipes (Producer, Consumer, lift, (>->), runEffect, each, yield, await, for, Pipe)
import           Pipes.Concurrent (Buffer(..), spawn, unbounded, fromInput, toOutput, forkIO, withSpawn, Input, Output)
import           System.Mem (performGC)
import System.Metrics hiding (Value)
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Distribution as Distribution
import System.Remote.Monitoring


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
-- >>> splitKeySpace 10 (Just "geotiff/10/1/950.tif",Just "i")
-- [(Just "geotiff/10/1/950.tif",Just "h"),(Just "h",Just "i")]
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
    getStartPrefix initialMinKeys initialMaxKeys Nothing =
        (initialMinKeys, initialMaxKeys, "")
    getStartPrefix initialMinKeys initialMaxKeys (Just start) =
      if initialMaxKeys - initialMinKeys == 1 then
        (0, 127, start)
      else
        (initialMinKeys, initialMaxKeys, "")

buildEnv :: Region -> IO Env
buildEnv r = do
    lgr <- newLogger Error stdout
    newEnv Discover <&> set envLogger lgr . set envRegion r

type PageResult = ([Object], Maybe (Text, Maybe Text))

getPage :: Env
  -> Text
  -> Distribution.Distribution
  -> Counter.Counter
  -> Counter.Counter
  -> Counter.Counter
  -> (Maybe Text, Maybe Text)
  -> IO PageResult
getPage
  env
  bucketName
  items_returned_distribution
  zero_items_counter
  non_zero_items_counter
  requests_counter
  (start, end) = do
    let request =
          listObjectsV (BucketName bucketName)
          & lStartAfter .~ start
    response <- runResourceT . runAWST env $ send request
    let objects = filterEnd end $ view lrsContents response
    let objectCount = length objects
    Distribution.add items_returned_distribution (fromIntegral objectCount)
    if objectCount == 0 then
      Counter.inc zero_items_counter
    else
      Counter.inc non_zero_items_counter

    Counter.inc requests_counter
    let nextSegment =
          if view lrsIsTruncated response == Just False then
            Nothing
          else if null objects then
            Nothing 
          else
            let
              start' = view (oKey . _ObjectKey) $ last objects
            in
              Just (start', end)
    return (objects, nextSegment)
  where
    filterEnd :: Maybe Text -> [Object] -> [Object]
    filterEnd Nothing x = x
    filterEnd (Just end) x =
      filter (\o -> view (oKey . _ObjectKey) o <= end) x


pipeBind :: Monad m => (a -> [b]) -> Pipe a b m r
pipeBind f = forever $ do
      a <- Pipes.await
      forM (f a) yield 

findAllItems :: (Maybe Text, Maybe Text) -> ((Maybe Text, Maybe Text) -> IO ([Object], Maybe (Text, Maybe Text))) -> Consumer Object IO () -> IO ()
findAllItems startBounds nextPage consumer =
  withSpawn unbounded go
  where
    go :: (Output PageResult, Input PageResult) -> IO ()
    go (output, input) = do
      asyncNextPage output startBounds 
      runEffect $ fromInput input >-> loop 1 output >-> consumer
    loop :: Int -> Output PageResult -> Pipe PageResult Object IO ()
    loop 0 _ = return ()
    loop c output = do
      lift $ putStrLn ("threads: " ++ show c)
      (page, next) <- Pipes.await
      forM_ page yield
      case next of Nothing -> loop (c - 1) output
                   (Just (start, end)) -> do
                     let subSpaces = splitKeySpace 10 (Just start, end)
                     lift . putStrLn $ "Splitting: " <>  show (Just start, end)
                     lift . putStrLn $ show subSpaces
                     lift $ forM_ subSpaces (asyncNextPage output)
                     loop (c - 1 + length subSpaces) output
    asyncNextPage output bounds = forkIO $ do
      result <- nextPage bounds
      runEffect $ Pipes.each [result] >-> toOutput output

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
main = do
  metricServer <- forkServer "localhost" 8001
  let store = serverMetricStore metricServer
  items_returned_distribution <- createDistribution "items_returned" store
  zero_items_counter <- createCounter "zero_items_counter" store
  non_zero_items_counter <- createCounter "non_zero_items_counter" store
  requests_counter <- createCounter "requests" store
  env <- buildEnv NorthVirginia
  let nextPage = getPage
                    env
                    "elevation-tiles-prod"
                    items_returned_distribution
                    zero_items_counter
                    non_zero_items_counter
                    requests_counter
  findAllItems (Nothing, Nothing) nextPage P.drain

say :: MonadIO m => Text -> m ()
say = liftIO . Text.putStrLn
