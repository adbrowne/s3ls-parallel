#!/usr/bin/env stack
-- stack --resolver lts-8.9 --install-ghc runghc --package parsec
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
import Text.Parsec
import Control.Monad
import Control.Monad.State
import Data.Set (Set)
import qualified Data.Set as Set
import Pipes
import qualified Pipes.Prelude as P

startLine :: Stream s m Char => ParsecT s u m [Char]
startLine = string "Starting: " >> many anyChar
endLine :: Stream s m Char => ParsecT s u m [Char]
endLine = string "Complete: " >> many anyChar

processLine :: String -> StateT (Set String, Set String) IO () 
processLine t = do
  (started, completed) <- get
  let started' = case (parse startLine "" t) of Left _ -> started
                                                Right x -> Set.insert x started
  let completed' = case (parse endLine "" t) of Left _ -> completed
                                                Right x -> Set.insert x completed
  put (started', completed')
  return ()

main = do
  (started, completed) <- execStateT (runEffect $ for P.stdinLn (lift . processLine)) (mempty, mempty)
  putStrLn $ show (started Set.\\ completed)
  return ()
