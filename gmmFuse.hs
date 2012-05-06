module Main where

import qualified Data.ByteString.Char8 as B
import System.Environment
import Foreign.C.Error
import System.Posix.Types
import System.Posix.Files
import System.Posix.IO
import System.Locale

import System.Fuse
import System.FilePath

import Database.HSparql.Connection
import Database.HSparql.QueryGenerator
import Codec.Binary.UTF8.String

import Text.Printf 

import Data.List hiding (union)

import Control.Monad

-- |main takes two (system) args: first should be a SPARQL 
-- endpoint, the second a mountpoint
main :: IO ()
main = do
    args <- getArgs
    withArgs [last args] $ 
        fuseMain (gmmFSOps (head args) (tail $ init args)) 
                 defaultExceptionHandler

-- * FUSE Operations
-- |gmmFSOps is the top-level FUSE operations for the system; 
-- it takes a SPARQL endpoint to use throughout
gmmFSOps :: EndPoint -> [String] -> FuseOperations ()
gmmFSOps ep perf = defaultFuseOps 
        { fuseGetFileStat        = fileStat ep
        , fuseOpenDirectory      = openDirectory ep
        , fuseReadDirectory      = gmmReadDirectory ep
        , fuseGetFileSystemStats = gmmGetFileSystemStats ep
        , fuseReadSymbolicLink   = gmmReadSymbolicLink ep
        } 
        where fileStat = if "stat1" `elem` perf || "stat" `elem` perf
                         then gmmGetFileStat' 
                         else if "stat2" `elem` perf
                              then gmmGetFileStat''
                              else gmmGetFileStat
              openDirectory = if "opendir" `elem` perf
                              then gmmOpenDirectory' 
                              else gmmOpenDirectory

-- |gmmGetFileStat, given a SPARQL endpoint and a path, 
-- returns the relevant FileStat or eNOENT
gmmGetFileStat :: EndPoint -> FilePath -> IO (Either Errno FileStat)
gmmGetFileStat ep path
    | isFilesDir = do
        ctx <- getFuseContext
        rootPaths <- {-# SCC "rootPaths._file" #-} 
                     queryFileEntries ep rootPath
        return $ if tailPath `elem` fmap fst rootPaths
                    then case lookup tailPath rootPaths of
                              Just s -> Right $ symlinkStat ctx $ 
                                            B.length $ B.pack $ drop 7 s 
                              Nothing -> Left eNOENT
                    else Left eNOENT
    | path == "/" || tailPath `elem` ["_=", "_files"] = do
        ctx <- getFuseContext
        myPaths <- {-# SCC "myPaths._special" #-} 
                   queryDirectoryEntries ep processedPath
        return $ Right $ dirStat ctx (2 + length myPaths)
    | otherwise                     = do
        ctx <- getFuseContext
        rootPaths <- {-# SCC "rootPaths._regular" #-} 
                     queryDirectoryEntries ep rootPath
        myPaths <- {-# SCC "myPaths._regular" #-} 
                   queryDirectoryEntries ep processedPath
        return $ if tailPath `elem` rootPaths
                    then Right $ dirStat ctx (2 + length myPaths)
                    else Left eNOENT
    where processedPath = '/':dropWhileEnd isPathSeparator (tail path)
          (rootPath, tailPath) = splitFileName processedPath
          isFilesDir = "_files" == snd (splitFileName 
                                      (dropWhileEnd isPathSeparator rootPath))

-- |gmmGetFileStat' is a performant, but incorrect implementation of
-- 'gmmGetFileStat' (never checks for st_nlink values, otherwise the same)
gmmGetFileStat' :: EndPoint -> FilePath -> IO (Either Errno FileStat)
gmmGetFileStat' ep path
    | isFilesDir = do
        ctx <- getFuseContext
        rootPaths <- {-# SCC "rootPaths._file" #-} queryFileEntries ep rootPath
        return $ if tailPath `elem` fmap fst rootPaths
                    then case lookup tailPath rootPaths of
                              Just s -> Right $ symlinkStat ctx $ 
                                            B.length $ B.pack $ drop 7 s 
                              Nothing -> Left eNOENT
                    else Left eNOENT
    | path == "/" || tailPath `elem` ["_=", "_files"] = do
        ctx <- getFuseContext
        return $ Right $ dirStat ctx 2
    | otherwise                     = do
        ctx <- getFuseContext
        rootPaths <- {-# SCC "rootPaths._regular" #-} 
                     queryDirectoryEntries ep rootPath
        return $ if tailPath `elem` rootPaths
                    then Right $ dirStat ctx 2
                    else Left eNOENT
    where processedPath = '/':dropWhileEnd isPathSeparator (tail path)
          (rootPath, tailPath) = splitFileName processedPath
          isFilesDir = "_files" == snd (splitFileName 
                                      (dropWhileEnd isPathSeparator rootPath))

-- |gmmGetFileStat'' is a performant, but incorrect, implementation of
-- 'gmmGetFileStat' (for non-symlinks, never returns eNOENT, and does
-- not return st_nlink values that mean anything)
gmmGetFileStat'' :: EndPoint -> FilePath -> IO (Either Errno FileStat)
gmmGetFileStat'' ep path
    | isFilesDir = do
        ctx <- getFuseContext
        -- we still need to query here, since we care about sizes
        rootPaths <- {-# SCC "rootPaths._file" #-} 
                     queryFileEntries ep rootPath
        return $ if tailPath `elem` fmap fst rootPaths
                    then case lookup tailPath rootPaths of
                              Just s -> Right $ symlinkStat ctx $ 
                                            B.length $ B.pack $ drop 7 s 
                              Nothing -> Left eNOENT
                    else Left eNOENT
    | otherwise = do
        ctx <- getFuseContext
        return $ Right $ dirStat ctx 2
    where (rootPath, tailPath) = splitFileName path
          isFilesDir = "_files" == snd (splitFileName 
                                      (dropWhileEnd isPathSeparator rootPath))

-- |gmmOpenDirectory, given a SPARQL endpoint and a path, 
-- returns eOK or eNOENT depending on the existence of a directory
gmmOpenDirectory :: EndPoint -> FilePath -> IO Errno
gmmOpenDirectory ep path
    | path == "/" || tailPath `elem` ["_=", "_files"] = return eOK
    -- no subdirectories of _files, only symlinks
    | isFilesDir                                      = return eNOENT 
    | otherwise                                       = do
        rootPaths <- {-# SCC "rootPaths" #-} 
                     queryDirectoryEntries ep rootPath
        return $ if tailPath `elem` rootPaths
                    then eOK
                    else eNOENT
    where (rootPath, tailPath) = splitFileName path
          isFilesDir = "_files" == snd (splitFileName 
                                      (dropWhileEnd isPathSeparator rootPath))

-- | gmmOpenDirectory' is a performant, but incorrect, implementation of
-- 'gmmOpenDirectory' (always returns eOK)
gmmOpenDirectory' :: EndPoint -> FilePath -> IO Errno
gmmOpenDirectory' _ _   = return eOK

-- |gmmReadDirectory, given a SPARQL endpoint and a path, 
-- either returns eNOENT for a nonexistent directory
-- or returns a set of entries, presumably pulled from the RDF store
gmmReadDirectory :: 
    EndPoint -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
gmmReadDirectory ep path = do
    ctx <- getFuseContext
    dirEnts <- directoryEntries ep path ctx
    return $ case dirEnts of
                  Nothing  -> Left eNOENT
                  Just ent -> Right ent

gmmReadSymbolicLink :: EndPoint -> FilePath -> IO (Either Errno FilePath)
gmmReadSymbolicLink ep path
    | valid = do
        rootPaths <- {-# SCC "rootPaths" #-} 
                     queryFileEntries ep rootPath
        return $ if tailPath `elem` fmap fst rootPaths
                    then case lookup tailPath rootPaths of
                              Just s -> Right $ drop 7 s 
                              Nothing -> Left eNOENT
                    else Left eNOENT
    where (rootPath, tailPath) = splitFileName path
          valid = "_files" == snd (splitFileName 
                                      (dropWhileEnd isPathSeparator rootPath))

-- |gmmGetFileSystemStats returns a FileSystemStats object. 
-- I'm honestly not sure what it's supposed to return.
-- I should look into that.
gmmGetFileSystemStats :: 
    EndPoint -> FilePath -> IO (Either Errno FileSystemStats)
gmmGetFileSystemStats _ _ =
    return $ Right FileSystemStats
      { fsStatBlockSize = 512
      , fsStatBlockCount = 1
      , fsStatBlocksFree = 1
      , fsStatBlocksAvailable = 1
      , fsStatFileCount = 5
      , fsStatFilesFree = 10
      , fsStatMaxNameLength = 255
      }

-- * 'gmmGetFileStat' utilities
-- |dirStat provides a FileStat for a directory, 
-- given a FuseContext and a st_nlink number
dirStat :: Integral a => FuseContext -> a -> FileStat
dirStat ctx links = FileStat { statEntryType = Directory
                       , statFileMode = foldr1 unionFileModes
                                          [ ownerReadMode
                                          , ownerExecuteMode
                                          , groupReadMode
                                          , groupExecuteMode
                                          , otherReadMode
                                          , otherExecuteMode
                                          ]
                       , statLinkCount = fromIntegral links
                       , statFileOwner = fuseCtxUserID ctx
                       , statFileGroup = fuseCtxGroupID ctx
                       , statSpecialDeviceID = 0
                       , statFileSize = 4096
                       , statBlocks = 1
                       , statAccessTime = 0
                       , statModificationTime = 0
                       , statStatusChangeTime = 0
                       }

-- |symlinkStat provides a FileStat for a symlink, 
-- given a FuseContext and a size number
symlinkStat :: Integral a => FuseContext -> a -> FileStat
symlinkStat ctx size = FileStat { statEntryType = SymbolicLink
                                , statFileMode = foldr1 unionFileModes
                                                     [ ownerReadMode
                                                     , groupReadMode
                                                     , otherReadMode
                                                     ]
                                , statLinkCount = 1
                                , statFileOwner = fuseCtxUserID ctx
                                , statFileGroup = fuseCtxGroupID ctx
                                , statSpecialDeviceID = 0
                                , statFileSize = fromIntegral size
                                , statBlocks = 1
                                , statAccessTime = 0
                                , statModificationTime = 0
                                , statStatusChangeTime = 0
                                }

-- * 'gmmReadDirectory' utilities
-- |directoryEntries returns a list of directory entries for 
-- a given path; it calls out to helper functions to determine 
-- what should be listed for a given path
directoryEntries :: 
    EndPoint -> FilePath -> FuseContext -> IO (Maybe [(FilePath, FileStat)])
directoryEntries ep path ctx 
    | last pathParts `elem` ["_files", "_files/"] = do
        rootPaths <- {-# SCC "rootPaths" #-} 
                     queryFileEntries ep path
        return $ Just $ 
            defaultDirectoryEntries ctx
            ++ zip (fmap fst rootPaths) 
                   (fmap (symlinkStat ctx . symlinkSize) rootPaths)
    | otherwise = do
        rootPaths <- {-# SCC "rootPaths" #-} 
                     queryDirectoryEntries ep path
        return $ Just $ defaultDirectoryEntries ctx 
                        ++ zip rootPaths (repeat $ dirStat ctx 2) 
                        ++ case take 2 $ reverse parts of
                              []        -> filesDirectoryEntry ctx
                              [_, "_="] -> filesDirectoryEntry ctx
                              ["_=", _] -> []
                              _         -> eqDirectoryEntry ctx
    where parts = last $ extractPaths path
          pathParts = splitPath path
          symlinkSize = B.length . B.pack . drop 7 . snd

-- |reusable default directory entries . and ..
defaultDirectoryEntries :: FuseContext -> [(FilePath, FileStat)]
defaultDirectoryEntries ctx = [(".", dirStat ctx 2), ("..", dirStat ctx 2)]

-- |reusable _files listing
filesDirectoryEntry :: FuseContext -> [(FilePath, FileStat)]
filesDirectoryEntry ctx = [("_files", dirStat ctx 2)]

-- |reusable _= listing
eqDirectoryEntry :: FuseContext -> [(FilePath, FileStat)]
eqDirectoryEntry ctx = [("_=", dirStat ctx 2)]

-- * SPARQL Querying
-- |queryDirectoryEntries runs a query to gather directory-entry results
queryDirectoryEntries :: EndPoint -> FilePath -> IO [String]
queryDirectoryEntries ep full@('/':path) = do
    (Just results) <- {-# SCC "query" #-} query ep $ 
                          directoryEntriesQuery $ processedPath
    return $ fmap ((!! 0) . fmap (escapePathChunk . illiterate)) results
    where processedPath = '/':dropWhileEnd isPathSeparator path

-- |queryFileEntries runs a query to gather directory-entry 
-- results for the _files folder
queryFileEntries :: EndPoint -> FilePath -> IO [(String, String)]
queryFileEntries ep full@('/':path)
    | last pathParts `elem` ["_files", "_files/"] = do
        (Just results) <- {-# SCC "query" #-} query ep $ 
                              directoryEntriesQuery $ processedPath
        return $ fmap packFilename $ zip (numbers results) 
                                         (fmap (fmap illiterate) results)
    where pathParts = splitPath full
          processedPath = '/':dropWhileEnd isPathSeparator path
          printfDigits list = printf $ 
                                  "%0" ++ show (numDigits $ length list) ++ "d"
          numbers list = (fmap (printfDigits list) ([1..] :: [Integer]))
          packFilename (n, [res, filename]) = 
              (n ++ '_':filename, encodeString res)

-- |count the number of digits in a number; for use in 
-- 'queryFileEntries' for uniquifying entries
numDigits = length . map (`mod` 10) . reverse . 
                takeWhile (> 0) . iterate (`div` 10)

-- |illiterate takes Literal (or URI) x and returns a utf-8-decoded x.
-- clever, I know :P
illiterate :: BindingValue -> String
illiterate (Literal x) = decodeString x
illiterate (URI x) = decodeString x

-- |SPARQL query to gather directory entries 
-- (to be called by 'queryDirectoryEntries' and 'queryFileEntries')
directoryEntriesQuery :: FilePath -> Query [Variable]
directoryEntriesQuery path 
    | last pathParts == "_files" = do
        (name, gmm, metaobj, file) <- baseQuery
        name <- foldM (pathQuery gmm metaobj) name parts
        filename <- var
        triple file (gmm .:. "filename") filename
        orderNext filename
        orderNext file
        return [file, filename]
    | otherwise                  = do
        (name, gmm, metaobj, file) <- baseQuery
        name <- foldM (pathQuery gmm metaobj) name parts
        return [name]
    where parts = extractPaths path
          pathParts = splitPath path

-- pathQuery :: Prefix -> Variable -> Variable -> GMMPath -> Query [Variable]
-- |SPARQL: combinable query chunk for one GMMPath
pathQuery gmm metaobj name path
    | "_=" `notElem` path = do
        lasthop <- foldM (predicateCase gmm) metaobj pathPredicates
        predicateCase gmm lasthop name
        filterExpr $ isLiteral name
        return name
    | "_=" == last path   = do 
        lasthop <- foldM (predicateCase gmm) metaobj pathPredicates
        filterExpr $ isLiteral lasthop
        return lasthop
    | completePath path   = do
        lasthop <- foldM (predicateCase gmm) metaobj (init pathPredicates)
        pred <- var
        predicateCaseVars gmm lasthop (last pathPredicates) (last path) pred
        return name
    where pathPredicates = takeWhile (/= "_=") path

-- |SPARQL query base: SELECT DISTINCT plus { start gmm:fileContent file. }
-- plus some variables to reuse
baseQuery = do
    name <- var
    metaobj <- var
    file <- var

    gmm <- prefix (iriRef "http://ianmcorvidae.net/gmm-ns/")

    distinct
    triple metaobj (gmm .:. "fileContent") file

    return (name, gmm, metaobj, file)

-- |SPARQL: { one-hop predicate two-hops. predicate gmm:niceName name. }
predicateCase gmm hop name = do
    pred <- var
    hop1 <- var
    predicateCaseVars gmm hop name hop1 pred
    return hop1

-- |Same as 'predicateCase' except it doesn't set its 
-- own vars, so it can be used in a 'union'.
predicateCaseVars gmm hop name hop1 pred = do
    triple hop pred hop1
    triple pred (gmm .:. "niceName") name

-- * File and GMMPath processing utility functions
-- | takes a filePath (rather, a chunk thereof) and escapes it
escapePathChunk :: FilePath -> FilePath
escapePathChunk path =
    concat $ fmap escapeChar path

-- | take a single char and return a string that should appear
-- in an escaped string
escapeChar :: Char -> String
escapeChar ('/') = "#%"
escapeChar ('#') = "##"
escapeChar char  = char:[]

-- | inverse of 'escapePathChunk'; turn an escaped path-chunk 
-- into an unescaped one
unescapePathChunk :: FilePath -> FilePath
unescapePathChunk [] = []
unescapePathChunk path
    | head path == '#' =
        unescapeChar (path !! 1) : unescapePathChunk (drop 2 path)
    | otherwise        =
        head path : unescapePathChunk (tail path)

-- | inverse of 'escapeChar', roughly, takes the character following
-- the escape character and returns the appropriate translation
unescapeChar :: Char -> Char
unescapeChar ('%') = '/'
unescapeChar ('#') = '#'
unescapeChar char  = char

-- | a GMMPath is a collection of strings (from FilePaths) that describe
-- a path to take within the RDF store; 'pathQuery' processes one into
-- SPARQL
type GMMPath = [FilePath]

-- | completePath tests if a GMMPath is a complete filter
completePath :: GMMPath -> Bool
completePath path@(_:_:_:_) =
    "_=" == last (init path)
completePath _ = False

-- | take a FilePath and turn it into a list of GMMPaths
extractPaths :: FilePath -> [GMMPath]
extractPaths path  
    | paths == []               = [[]]
    | completePath (last paths) = paths ++ extractPaths "/"
    | otherwise                 = paths
    where filterPart = unescapePathChunk . encodeString . 
                           dropWhileEnd isPathSeparator 
          paths = groupPaths $ 
                      fmap filterPart
                           (takeWhile (/= "_files") $ tail $ splitPath path)

-- | take a split FilePath and group it into GMMPaths
groupPaths :: [FilePath] -> [GMMPath]
groupPaths [] = []
groupPaths partList =
    (fst partSpan ++ fst sndSplit) : groupPaths (snd sndSplit)
    where partSpan = span (/= "_=") partList
          sndSplit = splitAt 2 $ snd partSpan

