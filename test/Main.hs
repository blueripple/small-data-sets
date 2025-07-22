{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main
  (main)
where

import BlueRipple.Data.Small.DataFrames
import BlueRipple.Data.Small.Loaders
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.LoadersCore as BRLC
import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Data.Map as M
import qualified System.Environment as Env

templateVars :: Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate :: K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

main :: IO ()
main = do
  pandocWriterConfig <-
    K.mkPandocWriterConfig
    pandocTemplate
    templateVars
    (BRK.brWriterOptionsF . K.mindocOptionsF)
  cacheDir <- toText . fromMaybe ".kh-cache" <$> Env.lookupEnv("BR_CACHE_DIR")
  let knitConfig :: K.KnitConfig BRCC.SerializerC BRCC.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "SmallDataLoaderTest"
          , K.logIf = const True
          , K.lcSeverity = mempty --M.fromList [("KH_Cache", K.Special), ("KH_Serialize", K.Special)]
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRCC.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
  resE <- K.consumeKnitEffectStack knitConfig $ do
    prri_C <- prriLoader
    prri <- K.ignoreCacheTime prri_C
    BRLC.logFrame prri
  case resE of
    Left err -> putTextLn $ show err
    Right () -> pure ()
