{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}
{-# OPTIONS_GHC -freduction-depth=0 #-}

module BlueRipple.Data.Small.Loaders
  (
    module BlueRipple.Data.Small.Loaders
  )
where

import qualified BlueRipple.Data.Small.DataFrames as BR
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.LoadersCore as BRL
import qualified BlueRipple.Data.CachingCore as BRC
import qualified Control.Foldl as FL
import Control.Lens ((%~), view, (^.))
import qualified Data.Map as M
import qualified Data.Sequence as Sequence
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.OrMissing as FS
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Melt as F
import qualified Frames.MaybeUtils as FM
import qualified Frames.Transform as FT
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as KS

useLocal :: Text -> BRL.DataPath
useLocal = BRL.LocalData

electoralCollegeFrame ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ElectoralCollege))
electoralCollegeFrame = BRL.cachedFrameLoader (BRL.LocalData $ T.pack BR.electorsCSV) Nothing Nothing id Nothing "electoralCollege.bin"

parsePEParty :: T.Text -> ET.PartyT
parsePEParty t
  | T.isInfixOf "democrat" t = ET.Democratic
  | T.isInfixOf "republican" t = ET.Republican
  | T.isInfixOf "DEMOCRAT" t = ET.Democratic
  | T.isInfixOf "REPUBLICAN" t = ET.Republican
  | otherwise = ET.Other

type PEFromCols = [BR.Year, BR.State, BR.StatePo, BR.StateFips, BR.Candidate, BR.Party, BR.Candidatevotes, BR.Totalvotes]

type ElectionDataCols = [ET.Office, BR.Candidate, ET.Party, ET.Votes, ET.TotalVotes]

type PresidentialElectionCols = [BR.Year, GT.StateName, GT.StateAbbreviation, GT.StateFIPS] V.++ ElectionDataCols
type PresidentialElectionColsI = PresidentialElectionCols V.++ '[ET.Incumbent]

fixPresidentialElectionRow ::
  F.Record PEFromCols -> F.Record PresidentialElectionCols
fixPresidentialElectionRow = F.rcast . addCols
  where
    addCols =
      (FT.addName @BR.StatePo @GT.StateAbbreviation)
      . (FT.addName @BR.State @GT.StateName)
        . (FT.addName @BR.StateFips @GT.StateFIPS)
        . (FT.addName @BR.Candidatevotes @ET.Votes)
        . (FT.addName @BR.Totalvotes @ET.TotalVotes)
        . (FT.addOneFromValue @ET.Office ET.President)
        . (FT.addOneFromOne @BR.Party @ET.Party parsePEParty)

presidentialByStateFrame ::
  (K.KnitEffects r, BRC.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PresidentialElectionCols))
presidentialByStateFrame =
  BRL.cachedMaybeFrameLoader @(F.RecordColumns BR.PresidentialByState) @PEFromCols @PEFromCols @PresidentialElectionCols
    (BRL.LocalData $ toText BR.presidentialByStateCSV)
    (Just BR.presidentialByStateParser)
    Nothing
    id
    fixPresidentialElectionRow
    Nothing
    "presByState.bin"

presidentialElectionKey :: F.Record PresidentialElectionCols -> (Text, Int)
presidentialElectionKey r = (F.rgetField @GT.StateAbbreviation r, F.rgetField @BR.Year r)

presidentialElectionsWithIncumbency ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec PresidentialElectionColsI))
presidentialElectionsWithIncumbency = do
  presidentialElex_C <- presidentialByStateFrame
  let g elex = fmap (let wm = winnerMap (F.rgetField @ET.Votes) presidentialElectionKey elex in addIncumbency 1 presidentialElectionKey sameCandidate (const False) wm) elex
  --  K.clearIfPresent "data/presidentialWithIncumbency.bin"
  BRC.retrieveOrMakeFrame "data/presidentialWithIncumbency.bin" presidentialElex_C (return . g)

type ElectionIntegrityCols = [BR.Year, GT.StateAbbreviation, BR.StateFIPS
                             , BR.PEIRatingstate
                             , BR.PEIVoting, BR.PEIVotingi
                             ]
                             {-
                             , BR.PEILaws, BR.PEILawsi
                             , BR.PEIProcedures, BR.PEIProceduresi
                             , BR.PEIBoundaries, BR.PEIBoundariesi
                             , BR.PEIVotereg, BR.PEIVoteregi
                             , BR.PEIPartyreg, BR.PEIPartyregi
                             , BR.PEIMedia, BR.PEIMediai
                             , BR.PEIFinance, BR.PEIFinancei
                             , BR.PEIVoting, BR.PEIVotingi
                             , BR.PEICount, BR.PEICounti
                             , BR.PEIResults, BR.PEIResultsi
                             , BR.PEIEMBs, BR.PEIEMBsi]
-}
type ElectionIntegrityColsRaw = [BR.PEIYear, BR.PEIStateAbbreviation, BR.PEIStateFIPS
                                , BR.PEIRatingstate
                                , BR.PEIVoting, BR.PEIVotingi
                                ]
{-
                                , BR.PEILaws, BR.PEILawsi
                                , BR.PEIProcedures, BR.PEIProceduresi
                                , BR.PEIBoundaries, BR.PEIBoundariesi
                                , BR.PEIVotereg, BR.PEIVoteregi
                                , BR.PEIPartyreg, BR.PEIPartyregi
                                , BR.PEIMedia, BR.PEIMediai
                                , BR.PEIFinance, BR.PEIFinancei
                                , BR.PEICount, BR.PEICounti
                                , BR.PEIResults, BR.PEIResultsi
                                , BR.PEIEMBs, BR.PEIEMBsi]
-}
electionIntegrityByState2016 ::  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec ElectionIntegrityCols))
electionIntegrityByState2016 = BRL.cachedMaybeFrameLoader
                               @(F.RecordColumns BR.ElectionIntegrityByState2016)
                               @ElectionIntegrityColsRaw
                               (BRL.LocalData $ T.pack BR.electionIntegrityByState2016CSV)
                               Nothing
                               Nothing
                               id
                               (F.rcast @ElectionIntegrityCols . addCols)
                               Nothing "electionIntegrityByState2016.bin"
  where
    addCols = (FT.addName @BR.PEIStateAbbreviation @GT.StateAbbreviation)
              . (FT.addName @BR.PEIYear @BR.Year)
              . (FT.addName @BR.PEIStateFIPS @BR.StateFIPS)


electionIntegrityByState2018 ::  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec ElectionIntegrityCols))
electionIntegrityByState2018 = BRL.cachedMaybeFrameLoader
                               @(F.RecordColumns BR.ElectionIntegrityByState2018)
                               @ElectionIntegrityColsRaw
                               (BRL.LocalData $ T.pack BR.electionIntegrityByState2018CSV)
                               Nothing
                               Nothing
                               id
                               (F.rcast @ElectionIntegrityCols . addCols)
                               Nothing "electionIntegrityByState2018.bin"
  where
    addCols = (FT.addName @BR.PEIStateAbbreviation @GT.StateAbbreviation)
              . (FT.addName @BR.PEIYear @BR.Year)
              . (FT.addName @BR.PEIStateFIPS @BR.StateFIPS)

-- starting with 118th congress/2022, we use 2020 PUMAs
type CDFromPUMA2012R = FT.ReType BR.CongressionalDistrict GT.CongressionalDistrict
                       (FT.ReType BR.StateAbbreviation GT.StateAbbreviation
                       (F.RecordColumns BR.CDFromPUMA2012))

cdFromPUMA2012Loader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  Int ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec CDFromPUMA2012R))
cdFromPUMA2012Loader congress = do
  (csvPath, cacheKey) <- case congress of
    113 -> return (BR.cd113FromPUMA2012CSV, "data/cd113FromPUMA2012.bin")
    114 -> return (BR.cd114FromPUMA2012CSV, "data/cd114FromPUMA2012.bin")
    115 -> return (BR.cd115FromPUMA2012CSV, "data/cd115FromPUMA2012.bin")
    116 -> return (BR.cd116FromPUMA2012CSV, "data/cd116FromPUMA2012.bin")
    117 -> return (BR.cd117FromPUMA2012CSV, "data/cd117FromPUMA2012.bin")
    118 -> return (BR.cd118FromPUMA2020CSV, "data/cd118FromPUMA2020.bin")
    _ -> K.knitError "PUMA for congressional district crosswalk only available for 113th, 114th, 115th, 116th, 117th, 118th congresses"
  BRL.cachedFrameLoader (BRL.LocalData $ toText csvPath) Nothing Nothing id Nothing cacheKey --"cd116FromPUMA2012.bin"

type DatedCDFromPUMA2012 = '[BR.Year] V.++ CDFromPUMA2012R

allCDFromPUMA2012Loader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec DatedCDFromPUMA2012))
allCDFromPUMA2012Loader = do
  let addYear :: Int -> F.Record CDFromPUMA2012R -> F.Record DatedCDFromPUMA2012
      addYear y r = (y F.&: V.RNil) `V.rappend` r
      loadWithYear :: (K.KnitEffects r, BRC.CacheEffects r) => (Int, Int) -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec DatedCDFromPUMA2012))
      loadWithYear (year, congress) = fmap (fmap (addYear year)) <$> cdFromPUMA2012Loader congress
  withYears_C <- sequenceA <$> traverse loadWithYear [(2012, 113), (2014, 114), (2016, 115), (2018, 116),(2019,116), (2020,117), (2021, 117), (2022, 118)]
  BRC.retrieveOrMakeFrame "data/cdFromPUMA.bin" withYears_C $ \withYears -> return $ mconcat withYears

{-
puma2000ToCD116Loader :: (K.KnitEffects r, BRC.CacheEffects r)
                      => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.PUMA2000ToCD116))
puma2000ToCD116Loader = BRL.cachedFrameLoader (BRL.LocalData $ T.pack BR.puma2000ToCD116CSV) Nothing Nothing id Nothing "puma2000ToCD116.sbin"
-}
county2010ToCD116Loader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyToCD116))
county2010ToCD116Loader = BRL.cachedFrameLoader (BRL.LocalData $ toText BR.countyToCD116CSV) Nothing Nothing id Nothing "county2010ToCD116.sbin"

countyToPUMALoader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyFromPUMA))
countyToPUMALoader = BRL.cachedFrameLoader (BRL.LocalData $ toText BR.county2014FromPUMA2012CSV) Nothing Nothing id Nothing "countyFromPUMA.bin"

rawStateAbbrCrosswalkLoader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.States))
rawStateAbbrCrosswalkLoader = BRL.cachedFrameLoader (BRL.LocalData $ toText BR.statesCSV) Nothing Nothing id Nothing "statesRaw.bin"
{-# INLINEABLE rawStateAbbrCrosswalkLoader #-}

stateAbbrCrosswalkLoader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateName, GT.StateFIPS, GT.StateAbbreviation, GT.CensusRegionC, GT.CensusDivisionC, BR.OneDistrict, BR.SLDUpperOnly]))
stateAbbrCrosswalkLoader = do
  statesRaw_C <- rawStateAbbrCrosswalkLoader
  BRC.retrieveOrMakeFrame "data/stateAbbr.bin" statesRaw_C $ \fRaw ->
    K.knitEither $ F.toFrame <$> (traverse parseCensusCols $ FL.fold FL.list fRaw)
{-# INLINEABLE stateAbbrCrosswalkLoader #-}

stateUpperOnlyMap :: (K.KnitEffects r, BRC.CacheEffects r) => K.Sem r (Map Text Bool)
stateUpperOnlyMap = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. BR.sLDUpperOnly)) FL.map)
                    <$> K.ignoreCacheTimeM stateAbbrCrosswalkLoader
{-# INLINEABLE stateUpperOnlyMap #-}


stateSingleCDMap :: (K.KnitEffects r, BRC.CacheEffects r) => K.Sem r (Map Text Bool)
stateSingleCDMap = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. BR.oneDistrict)) FL.map)
                   <$> K.ignoreCacheTimeM stateAbbrCrosswalkLoader
{-# INLINEABLE stateSingleCDMap #-}

stateAbbrFromFIPSMapLoader :: (K.KnitEffects r, BRC.CacheEffects r) => K.Sem r (Map Int Text)
stateAbbrFromFIPSMapLoader = do
  stateAbbrCrosswalk <- K.ignoreCacheTimeM stateAbbrCrosswalkLoader
  let assoc r = (view GT.stateFIPS r, view GT.stateAbbreviation r)
  pure $ FL.fold (FL.premap assoc FL.map) stateAbbrCrosswalk

addStateAbbrUsingFIPS ::  (K.KnitEffects r, BRC.CacheEffects r, F.ElemOf rs GT.StateFIPS, FI.RecVec rs)
                      => F.FrameRec rs -> K.Sem r (F.FrameRec (GT.StateAbbreviation ': rs))
addStateAbbrUsingFIPS rs = do
  stateAbbrCrosswalk <- K.ignoreCacheTimeM stateAbbrCrosswalkLoader
  let assoc r = (view GT.stateFIPS r, view GT.stateAbbreviation r)
      m = FL.fold (FL.premap assoc FL.map) stateAbbrCrosswalk
      lookupAndAdd r = do
        let fips = view GT.stateFIPS r
        case M.lookup fips m of
          Nothing -> KS.errStreamly $ "addStateAbbrUsingFIPS: missing FIPS=" <> show fips <> " in stateAbbrCrosswalk."
          Just sa -> pure $ sa F.&: r
  KS.streamlyToKnit $ FST.concurrentMapM lookupAndAdd rs
{-# INLINEABLE addStateAbbrUsingFIPS #-}

parseCensusRegion :: T.Text -> Either Text GT.CensusRegion
parseCensusRegion "Northeast" = Right GT.Northeast
parseCensusRegion "Midwest" = Right GT.Midwest
parseCensusRegion "South" = Right GT.South
parseCensusRegion "West" = Right GT.West
parseCensusRegion "OtherRegion" = Right GT.OtherRegion
parseCensusRegion x = Left $ "Unparsed census region: " <> x

parseCensusDivision :: T.Text -> Either Text GT.CensusDivision
parseCensusDivision "NewEngland" = Right GT.NewEngland
parseCensusDivision "MiddleAtlantic" = Right GT.MiddleAtlantic
parseCensusDivision "EastNorthCentral" = Right GT.EastNorthCentral
parseCensusDivision "WestNorthCentral" = Right GT.WestNorthCentral
parseCensusDivision "SouthAtlantic" = Right GT.SouthAtlantic
parseCensusDivision "EastSouthCentral" = Right GT.EastSouthCentral
parseCensusDivision "WestSouthCentral" = Right GT.WestSouthCentral
parseCensusDivision "Mountain" = Right GT.Mountain
parseCensusDivision "Pacific" = Right GT.Pacific
parseCensusDivision "OtherDivision" = Right GT.OtherDivision
parseCensusDivision x = Left $ "Unparsed census division: " <> x


parseCensusCols :: BR.States
                -> Either Text (F.Record [GT.StateName, GT.StateFIPS, GT.StateAbbreviation, GT.CensusRegionC, GT.CensusDivisionC, BR.OneDistrict, BR.SLDUpperOnly])
parseCensusCols r = do
  region <- parseCensusRegion $ F.rgetField @BR.Region r
  division <- parseCensusDivision $ F.rgetField @BR.Division r
  return $ F.rcast @[GT.StateName, GT.StateFIPS, GT.StateAbbreviation] (FT.retypeColumn @BR.StateAbbreviation @GT.StateAbbreviation r)
    `V.rappend` (region F.&: division F.&: V.RNil)
    `V.rappend` F.rcast @[BR.OneDistrict, BR.SLDUpperOnly] r

type StateTurnoutColsRaw = F.RecordColumns BR.StateTurnout
type StateTurnoutCols = [BR.Year, BR.State, GT.StateAbbreviation, BR.BallotsCountedVAP, BR.BallotsCountedVEP, BR.VotesHighestOffice, BR.VAP,BR.VEP,BR.PctNonCitizen,BR.Prison,BR.Probation, BR.Parole,BR.TotalIneligibleFelon, BR.OverseasEligible]

stateTurnoutLoader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec StateTurnoutCols))
stateTurnoutLoader =
  BRL.cachedMaybeFrameLoader @StateTurnoutColsRaw @_ @_ @StateTurnoutCols
    (BRL.LocalData $ toText BR.stateTurnoutCSV)
    Nothing
    Nothing
    fixMaybes
    fixSA
    Nothing
    "stateTurnout.bin"
  where
    missingOETo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.OverseasEligible] -> F.Rec (Maybe F.:. F.ElField) '[BR.OverseasEligible]
    missingOETo0 = FM.fromMaybeMono 0
    missingBCVEPTo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCountedVEP] -> F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCountedVEP]
    missingBCVEPTo0 = FM.fromMaybeMono 0
    missingBCTo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCounted] -> F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCounted]
    missingBCTo0 = FM.fromMaybeMono 0
    fixMaybes = (F.rsubset %~ missingOETo0) . (F.rsubset %~ missingBCVEPTo0) . (F.rsubset %~ missingBCTo0)
    fixSA :: F.Record StateTurnoutColsRaw -> F.Record StateTurnoutCols
    fixSA = F.rcast . FT.retypeColumn @BR.StateAbbreviation @GT.StateAbbreviation


type HouseElectionCols = [BR.Year, BR.State, GT.StateAbbreviation, BR.StateFIPS, GT.CongressionalDistrict] V.++ ([BR.Special, BR.Runoff, BR.Stage] V.++ ElectionDataCols)

type HouseElectionColsI = HouseElectionCols V.++ '[ET.Incumbent]

isRunoff :: F.ElemOf rs BR.Stage => F.Record rs -> Bool
isRunoff r = F.rgetField @BR.Stage r == "runoff"

processHouseElectionRow :: BR.HouseElections -> F.Record HouseElectionCols
processHouseElectionRow r = F.rcast @HouseElectionCols (mutate r)
  where
    mutate =
      FT.retypeColumn @BR.StatePo @GT.StateAbbreviation
        . FT.retypeColumn @BR.StateFips @BR.StateFIPS
        . FT.mutate (const $ FT.recordSingleton @ET.Office ET.House)
        . FT.retypeColumn @BR.District @GT.CongressionalDistrict
        . FT.retypeColumn @BR.Candidatevotes @ET.Votes
        . FT.retypeColumn @BR.Totalvotes @ET.TotalVotes
        . FT.mutate
        (FT.recordSingleton @ET.Party . parsePEParty . F.rgetField @BR.Party)
        . FT.mutate
        (FT.recordSingleton @BR.Special . FS.orMissing False id . F.rgetField @BR.SpecialOM)
        . FT.mutate
        (FT.recordSingleton @BR.Runoff . FS.orMissing False id . F.rgetField @BR.RunoffOM)

houseElectionsRawLoader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionCols))
houseElectionsRawLoader = BRL.cachedFrameLoader
                          (useLocal $ toText BR.houseElectionsCSV)
                          (Just BR.houseElectionsParser) -- this is key
                          Nothing
                          processHouseElectionRow
                          Nothing
                          "houseElectionsRaw.bin"

houseElectionsLoader ::   (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionCols))
houseElectionsLoader = do
  elexRaw_C <- houseElectionsRawLoader
  BRC.retrieveOrMakeFrame "data/houseElections.bin" elexRaw_C $ return . fixRunoffYear houseRaceKey (F.rgetField @BR.Runoff)


houseElectionsWithIncumbency ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionColsI))
houseElectionsWithIncumbency = do
  houseElex_C <- houseElectionsLoader
  --K.ignoreCacheTime houseElex_C >>= K.logLE K.Diagnostic . T.pack . show . winnerMap
  let g elex = fmap (let wm = winnerMap (F.rgetField @ET.Votes) houseElectionKey elex in addIncumbency 1 houseElectionKey sameCandidate isRunoff wm) elex
  --  K.clearIfPresent "data/houseWithIncumbency.bin"
  BRC.retrieveOrMakeFrame "data/houseWithIncumbency.bin" houseElex_C (return . g)

fixRunoffYear :: forall rs raceKey f. (FI.RecVec rs, Foldable f, Ord raceKey, F.ElemOf rs BR.Year)
  => (F.Record rs -> raceKey) -> (F.Record rs -> Bool) -> f (F.Record rs) -> F.FrameRec rs
fixRunoffYear rKey runoff rows = flip evalState M.empty $ FL.foldM g rows where
  year = F.rgetField @BR.Year
  g :: FL.FoldM (State (Map raceKey Int)) (F.Record rs) (F.FrameRec rs)
  g = FL.FoldM step (return Sequence.empty) (return . F.toFrame) where
    step :: Sequence.Seq (F.Record rs) -> F.Record rs -> State (Map raceKey Int) (Sequence.Seq (F.Record rs))
    step s r = if not $ runoff r
               then modify (M.insert (rKey r) (year r)) >> return (s <> Sequence.singleton r)
               else (do
                        year' <- fromMaybe (year r) <$> gets (M.lookup $ rKey r)
                        let r' = FT.fieldEndo @BR.Year (const year') r
                        return (s <> Sequence.singleton r')
                    )

winnerMap :: forall rs f kl ky.(Foldable f, Ord kl, Ord ky)
          => (F.Record rs -> Int)
          -> (F.Record rs -> (kl, ky))
          -> f (F.Record rs)
          -> M.Map kl (Map ky (F.Record rs))
winnerMap votes key = FL.fold (FL.Fold g mempty id)
  where
    chooseWinner :: F.Record rs -> F.Record rs -> F.Record rs
    chooseWinner a b = if votes a > votes b then a else b
    g :: M.Map kl (M.Map ky (F.Record rs)) -> F.Record rs -> M.Map kl (M.Map ky (F.Record rs))
    g m r =
      let (kl, ky) = key r
          mi =  case M.lookup kl m of
            Nothing -> one (ky, r)
            Just mi' -> M.insertWith chooseWinner ky r mi'
      in M.insert kl mi m

lastWinners :: (Ord kl, Ord ky) => Int -> (F.Record rs -> (kl, ky)) -> M.Map kl (Map ky (F.Record rs)) -> F.Record rs -> Maybe [F.Record rs]
lastWinners n key winnerMap' r = do
  let (kl, ky) = key r
  ml <- M.lookup kl winnerMap'
  let ascendingWinners = M.toAscList ml
      lastLess :: forall a b.Ord a => Int -> a -> [(a, b)] -> Maybe (a, b)
      lastLess n' a abs' =
        let paired = zip abs' $ drop n' abs'
            go :: [((a, b), (a, b))] -> Maybe (a, b)
            go [] = Nothing
            go (((al, bl), (ar, _)) : pabs) = if a > al && a <= ar then Just (al, bl) else go pabs
        in go paired
  fmap snd <$> traverse (\m -> lastLess m ky ascendingWinners) [1..n]

houseRaceKey :: F.Record HouseElectionCols -> (Text, Int)
houseRaceKey r = (F.rgetField @GT.StateAbbreviation r, F.rgetField @GT.CongressionalDistrict r)
houseElectionKey :: F.Record HouseElectionCols -> ((Text, Int), Int)
houseElectionKey r = (houseRaceKey r, F.rgetField @BR.Year r)

sameCandidate :: F.ElemOf rs BR.Candidate => F.Record rs -> F.Record rs -> Bool
sameCandidate a b = F.rgetField @BR.Candidate a == F.rgetField @BR.Candidate b

addIncumbency :: (Ord kl, Ord ky)
  => Int
  -> (F.Record rs -> (kl, ky))
  -> (F.Record rs -> F.Record rs -> Bool)
  -> (F.Record rs -> Bool)
  -> M.Map kl (M.Map ky (F.Record rs))
  -> F.Record rs
  -> F.Record (rs V.++ '[ET.Incumbent])
addIncumbency n key sameCand runoff wm r =
  let lws = lastWinners (n + 1) key wm r
      lwsFixed = if runoff r
                 then fmap (take n) lws -- we got n + 1 and drop the last since that's the general leading to this runoff, not the previous
                 else fmap (drop 1) lws -- not a runoff so get rid of the extra one
      incumbent = case lwsFixed of
        Nothing -> False
        Just prs -> not $ null $ filter (sameCand r) prs
  in r V.<+> ((incumbent F.&: V.RNil) :: F.Record '[ET.Incumbent])

atLargeDistrictStates :: Int -> [Text]
atLargeDistrictStates n
  | n < 2022 = ["AK", "DE", "MT", "ND", "SD", "VT", "WY"]
  | otherwise = ["AK", "DE", "ND", "SD", "VT", "WY"]

fixSingleDistricts :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict, Functor f)
                   => [Text] -> Int -> f (F.Record rs) -> f (F.Record rs)
fixSingleDistricts districts setTo recs = fmap fixOne recs where
  fixOne r = if F.rgetField @GT.StateAbbreviation r `elem` districts then F.rputField @GT.CongressionalDistrict setTo r else r


fixDCDistrict :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict, Functor f) => Int -> f (F.Record rs) -> f (F.Record rs)
fixDCDistrict = fixSingleDistricts ["DC"]

fixAtLargeDistricts :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict, Functor f) => Int -> Int -> f (F.Record rs) -> f (F.Record rs)
fixAtLargeDistricts year = fixSingleDistricts (atLargeDistrictStates year)

type SenateElectionCols = [BR.Year, BR.State, GT.StateAbbreviation, BR.StateFIPS] V.++ ([BR.Special, BR.Stage] V.++ ElectionDataCols)

type SenateElectionColsI = SenateElectionCols V.++ '[ET.Incumbent]
-- NB: If there are 2 specials at the same time, this will fail to distinguish them. :(
senateRaceKey :: F.Record SenateElectionCols -> (Text, Bool)
senateRaceKey r = (F.rgetField @GT.StateAbbreviation r, F.rgetField @BR.Special r)

senateElectionKey :: F.Record SenateElectionCols -> ((Text, Bool), Int)
senateElectionKey r = (senateRaceKey r, F.rgetField @BR.Year r)

processSenateElectionRow :: BR.SenateElections -> F.Record SenateElectionCols
processSenateElectionRow r = F.rcast @SenateElectionCols (mutate r)
  where
    mutate =
      FT.retypeColumn @BR.SenateStatePo @GT.StateAbbreviation
        . FT.retypeColumn @BR.SenateStateFips @BR.StateFIPS
        . FT.retypeColumn @BR.SenateYear @BR.Year
        . FT.retypeColumn @BR.SenateState @BR.State
        . FT.retypeColumn @BR.SenateStage @BR.Stage
        . FT.retypeColumn @BR.SenateSpecial @BR.Special
        . FT.mutate (const $ FT.recordSingleton @ET.Office ET.Senate)
        . FT.retypeColumn @BR.SenateCandidatevotes @ET.Votes
        . FT.retypeColumn @BR.SenateCandidate @BR.Candidate
        . FT.retypeColumn @BR.SenateTotalvotes @ET.TotalVotes
        . FT.mutate
          (FT.recordSingleton @ET.Party . parsePEParty . F.rgetField @BR.SenatePartyDetailed)

senateElectionsRawLoader ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateElectionCols))
senateElectionsRawLoader = BRL.cachedFrameLoader (BRL.LocalData $ toText BR.senateElectionsCSV) Nothing Nothing processSenateElectionRow Nothing "senateElectionsRaw.bin"

senateElectionsLoader ::   (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateElectionCols))
senateElectionsLoader = do
  elexRaw_C <- senateElectionsRawLoader
  BRC.retrieveOrMakeFrame "data/senateElections.bin" elexRaw_C $ return . fixRunoffYear senateRaceKey isRunoff


senateElectionsWithIncumbency ::
  (K.KnitEffects r, BRC.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateElectionColsI))
senateElectionsWithIncumbency = do
  senateElex_C <- senateElectionsLoader
  --K.ignoreCacheTime houseElex_C >>= K.logLE K.Diagnostic . T.pack . show . winnerMap
  let g elex = fmap (let wm = winnerMap (F.rgetField @ET.Votes) senateElectionKey elex in addIncumbency 2 senateElectionKey sameCandidate isRunoff wm) elex
  --  K.clearIfPresent "data/houseWithIncumbency.bin"
  BRC.retrieveOrMakeFrame "data/senateWithIncumbency.bin" senateElex_C (return . g)
