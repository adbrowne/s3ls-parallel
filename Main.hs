{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}

module Main where

import S3Parallel
import System.Exit (exitFailure)
import System.Random (randomIO)
import System.IO
import Data.UUID as UUID
import Control.DeepSeq
import Control.Lens
import Control.Concurrent
       (ThreadId, myThreadId, withMVar, newMVar, MVar, forkFinally,
        newEmptyMVar, putMVar, takeMVar)
import Control.Exception (try, SomeException, throwTo)
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.AWS
       (Env, runResourceT, runAWST, send, envRetryCheck, timeout,
        envLogger, envRegion, Credentials(..), newLogger, LogLevel(..),
        newEnv)
import Data.List (sort)
import qualified Data.Foldable as Fold
import Data.Text (Text)
import qualified Data.Text.IO as Text
import Data.Time
import Network.AWS.S3
import qualified Data.Text as T
import qualified Pipes.Prelude as P
import Pipes
       (Consumer, lift, (>->), runEffect, each, yield, await, Pipe)
import Pipes.Concurrent
       (unbounded, fromInput, toOutput, forkIO, withSpawn)
import System.Metrics hiding (Value)
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Distribution as Distribution
import System.Remote.Monitoring

buildEnv :: Region -> IO Env
buildEnv r = do
    lgr <- newLogger Error stdout
    newEnv Discover <&> set envLogger lgr . set envRegion r <&>
        set
            envRetryCheck
            (\_ _ ->
                  True)

type PageRequest m = Monad m =>
                     SearchBounds -> m PageResult

-- Examples:
--
-- |
-- >>> evalState (getPageTest (Nothing, Nothing)) [S3Object "andrew"]
-- ([S3Object {s3ObjectKey = "andrew"}],Nothing)
--
-- >>> evalState (getPageTest (Nothing, Just "a")) [S3Object "andrew"]
-- ([],Nothing)
--
-- >>> evalState (getPageTest (Just "c", Nothing)) [S3Object "andrew"]
-- ([],Nothing)
getPageTest
    :: MonadState [S3Object] m
    => SearchBounds -> m PageResult
getPageTest (startBound,endBound) = do
    allObjects <- get
    let relevantObjects = sort $ filter withinBounds allObjects
    let (thisResult,nextResults) = splitAt 1000 relevantObjects
    if Fold.null nextResults
        then return (thisResult, Nothing)
        else let lastResult = (s3ObjectKey . last) thisResult
             in return (thisResult, Just (lastResult, endBound))
  where
    withinBounds x = afterStartBound x && beforeEndBound x
    afterStartBound x =
        case startBound of
            Nothing -> True
            (Just bound) -> s3ObjectKey x > bound
    beforeEndBound x =
        case endBound of
            Nothing -> True
            (Just bound) -> s3ObjectKey x < bound

getPage
    :: Env
    -> Text
    -> Distribution.Distribution
    -> Counter.Counter
    -> Counter.Counter
    -> Counter.Counter
    -> (Maybe Text, Maybe Text)
    -> IO PageResult
getPage env bucketName items_returned_distribution zero_items_counter non_zero_items_counter requests_counter (start,end) = do
    let request = listObjectsV (BucketName bucketName) & lStartAfter .~ start
    response <- runResourceT . runAWST env $ timeout 10 $ send request
    let objects = filterEnd end $ view lrsContents response
    let objectCount = length objects
    Distribution.add items_returned_distribution (fromIntegral objectCount)
    if objectCount == 0
        then Counter.inc zero_items_counter
        else Counter.inc non_zero_items_counter
    Counter.inc requests_counter
    let nextSegment =
            if view lrsIsTruncated response == Just False
                then Nothing
                else if Fold.null objects
                         then Nothing
                         else let start' =
                                      view (oKey . _ObjectKey) $ last objects
                              in Just (start', end)
    return $ (fmap objectToS3Object objects, nextSegment)
  where
    filterEnd :: Maybe Text -> [Object] -> [Object]
    filterEnd Nothing x = x
    filterEnd (Just endKey) x =
        filter
            (\o ->
                  view (oKey . _ObjectKey) o <= endKey)
            x

log :: MVar () -> String -> IO ()
log lock text =
    withMVar lock $
    \_ ->
         putStrLn text

timeItem
    :: NFData b
    => (String -> IO ()) -> String -> (a -> IO b) -> a -> IO b
timeItem logger name f a = do
    startTime <- getCurrentTime
    result <- f a
    _ <- deepseq result (return ()) -- Ensure we include deserialization
    endTime <- getCurrentTime
    let diff = diffUTCTime endTime startTime
    logger $ "Timing: " ++ name ++ " " ++ show diff
    return result

pipeBind
    :: Monad m
    => (a -> [b]) -> Pipe a b m r
pipeBind f =
    forever $
    do a <- Pipes.await
       forM (f a) yield

maxThreads :: Int
maxThreads = 100

actionResult :: Int -> PageResult -> ([S3Object], [SearchBounds])
actionResult currentThreads (page,next) =
    case next of
        Nothing -> (page, [])
        (Just (start,end)) ->
            if currentThreads < maxThreads
                then do
                    let subSpaces = splitKeySpace 10 (Just start, end)
                    (page, subSpaces)
                else do
                    (page, [(Just start, end)])

findAllItems
    :: ThreadId
    -> (String -> IO ())
    -> b
    -> (b -> IO (Int -> ([a], [b])))
    -> Consumer a IO ()
    -> IO ()
findAllItems mainThreadId logger start next consumer = withSpawn unbounded go
  where
    go (output,input) = do
        requestId <- randomIO :: IO UUID.UUID
        _ <- asyncNextPage output (requestId, start)
        runEffect $ fromInput input >-> loop 1 output >-> consumer
    loop 0 _ = return ()
    loop c output = do
        lift $ logger ("threads: " ++ show c)
        result <- Pipes.await
        let (resultObjects,nextBounds) = result c
        forM_ resultObjects yield
        nextBounds' <-
            lift $
            traverse
                (\x ->
                      (randomIO :: IO UUID) >>=
                      \y ->
                           return (y, x))
                nextBounds
        lift $ forM_ nextBounds' (asyncNextPage output)
        loop (c - 1 + length nextBounds) output
    asyncNextPage output (requestId,bounds) =
        forkIO $
        do logger $ "Starting: " ++ show requestId
           resultEx <- try (next bounds)
           case resultEx of
               Left (ex :: SomeException) -> do
                   logger "Exception"
                   throwTo mainThreadId ex
                   exitFailure
               Right result -> do
                   runEffect $ Pipes.each [result] >-> toOutput output
                   logger $ "Complete: " ++ show requestId

buildNextPage :: IO (Store, (Maybe Text, Maybe Text) -> IO PageResult)
buildNextPage = do
    metricServer <- forkServer "localhost" 8001
    let store = serverMetricStore metricServer
    items_returned_distribution <- createDistribution "items_returned" store
    zero_items_counter <- createCounter "zero_items_counter" store
    non_zero_items_counter <- createCounter "non_zero_items_counter" store
    requests_counter <- createCounter "requests" store
    env <- buildEnv NorthVirginia
    return
        ( store
        , \x ->
               getPage
                   env
                   "elevation-tiles-prod"
                   items_returned_distribution
                   zero_items_counter
                   non_zero_items_counter
                   requests_counter
                   x)

runNormally :: IO ()
runNormally = do
    logLock <- newMVar ()
    let log' = Main.log logLock
    (store,nextPage) <- buildNextPage
    items_counter <- createCounter "items_counter" store
    let processResult =
            \x -> do
                r <- timeItem log' "nextPage" (nextPage) x
                return
                    (\c ->
                          actionResult c r)
    mainThreadId <- myThreadId
    findAllItems
        mainThreadId
        log'
        (Just "logs/2016-01-01", Just "m")
        processResult $
        P.map s3ObjectKey >->
        P.tee
            (P.mapM_ $
             \_ ->
                  Counter.inc items_counter) >->
        P.map
            (\t ->
                  "File: " ++ T.unpack t) >->
        P.stdoutLn

failOn10 :: Int -> IO (Int -> ([Int], [Int]))
failOn10 b = do
    let result _ =
            if b > 10
                then error "Bad"
                else ([b], [b + 1])
    return result

takeEveryNth :: Int -> String -> StateT Int IO Bool
takeEveryNth n = go
  where
    go _ = do
        v <- get
        if (v == 0)
            then put n >> return True
            else put (v - 1) >> return False

downloadSegment
    :: Counter.Counter
    -> ((Maybe Text, Maybe Text) -> IO PageResult)
    -> (Maybe Text, Maybe Text)
    -> IO ()
downloadSegment itemsCounter nextPage initial = do
    (items,next) <- nextPage initial
    forM_
        items
        (\t ->
              (putStrLn $ "File: " ++ T.unpack (s3ObjectKey t)) >>
              Counter.inc itemsCounter)
    case next of
        Nothing -> return ()
        (Just (start,end)) ->
            downloadSegment itemsCounter nextPage (Just start, end)

forkThread :: IO () -> IO (MVar ())
forkThread proc = do
    handle <- newEmptyMVar
    _ <-
        forkFinally
            proc
            (\_ ->
                  putMVar handle ())
    return handle

runStartingFromList :: FilePath -> IO ()
runStartingFromList filePath = do
    handle <- openFile filePath ReadMode
    handle2 <- openFile filePath ReadMode
    (store,nextPage) <- buildNextPage
    items_counter <- createCounter "items_counter" store
    r <- runEffect $ P.length (P.fromHandle handle)
    let itemsPerSegment = r `div` maxThreads
    print itemsPerSegment
    dividers <-
        evalStateT
            (P.toListM $ (P.fromHandle handle2) >->
             P.filterM (takeEveryNth itemsPerSegment))
            itemsPerSegment
    let dividers' = fmap (T.pack) dividers
    let startSegments = (Just "logs/2016-01-01") : (fmap Just dividers')
    let endSegments = fmap Just dividers' ++ [Just "m"]
    let segments = zip startSegments endSegments
    print segments
    print $ length segments
    threads <-
        forM
            segments
            (\s ->
                  forkThread (downloadSegment items_counter nextPage s))
    mapM_ takeMVar threads

main :: IO ()
main =
    --runStartingFromList "./tiles2.files"
    runNormally

--findAllItems 0 failOn10 $ P.print
say
    :: MonadIO m
    => Text -> m ()
say = liftIO . Text.putStrLn
