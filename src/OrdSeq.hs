{-# LANGUAGE MultiParamTypeClasses #-}

module OrdSeq
  (OrdSeq
  ,toListOrderSeq
  ,partition
  ,OrdSeq.fromList)
  where

import Data.Foldable (toList)
import Data.FingerTree

data Key a
    = NoKey
    | Key a
    deriving (Show,Eq,Ord)

instance Monoid (Key a) where
    mempty = NoKey
    k `mappend` NoKey = k
    _ `mappend` k = k

newtype Elem a =
    Elem a
    deriving ((Show))

newtype OrdSeq a =
    OrdSeq (FingerTree (Key a) (Elem a))
    deriving ((Show))

instance Measured (Key a) (Elem a) where
    measure (Elem x) = Key x

toListOrderSeq :: OrdSeq a -> [a]
toListOrderSeq (OrdSeq t) =
    (\(Elem a) ->
          a) <$>
    toList t

partition
    :: (Ord a)
    => a -> OrdSeq a -> (OrdSeq a, OrdSeq a)
partition k (OrdSeq xs) = (OrdSeq l, OrdSeq r)
  where
    (l,r) = split (> Key k) xs

fromList :: [a] -> OrdSeq a
fromList a = OrdSeq $ Data.FingerTree.fromList (Elem <$> a)
