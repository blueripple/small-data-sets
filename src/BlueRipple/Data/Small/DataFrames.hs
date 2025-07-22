{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module BlueRipple.Data.Small.DataFrames
  ( module BlueRipple.Data.Small.DataSourcePaths
  , module BlueRipple.Data.Small.DataFrames
  )
where

import Prelude hiding (State)
import           BlueRipple.Data.Small.DataSourcePaths

import qualified Frames                        as F

import qualified Streamly.Data.Stream as Streamly

import qualified Frames.ParseableTypes         as FP
import qualified Frames.Streamly.TH as FS
import qualified Frames.Streamly.ColumnUniverse as FS

import Frames.Streamly.Streaming.Streamly (StreamlyStream(..))

#if MIN_VERSION_streamly(0,9,0)
type Stream = Streamly.Stream
#else
type Stream = Streamly.SerialT
#endif
type StreamlyS = StreamlyStream Stream

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay
F.declareColumn "StartDate" ''FP.FrameDay
F.declareColumn "EndDate" ''FP.FrameDay
F.declareColumn "ElectionDate" ''FP.FrameDay

FS.tableTypes "TractsByDistrict" (framesPath tracts2022ByDistrict2024CSV)

FS.tableTypes "TotalSpending" (framesPath totalSpendingCSV)

FS.tableTypes' (FS.rowGen (framesPath forecastAndSpendingCSV)) { FS.rowTypeName = "ForecastAndSpending"
                                                               , FS.columnParsers = FS.parseableParseHowRec @FP.ColumnsWithDayAndLocalTime
                                                               }

FS.tableTypes' prriRowGen
FS.tableTypes "ElectionResults" (framesPath electionResultsCSV)
FS.tableTypes "AngryDems" (framesPath angryDemsCSV)
FS.tableTypes "AllMoney2020" (framesPath allMoney2020CSV)
--FS.tableTypes "HouseElections" (framesPath houseElectionsCSV)
FS.tableTypes' houseElectionsRowGen
FS.declareColumn "Special" ''Bool
FS.declareColumn "Runoff" ''Bool


FS.tableTypes' (FS.rowGen (framesPath senateElectionsCSV)) { FS.rowTypeName = "SenateElections"
                                                           , FS.tablePrefix = "Senate"
                                                           }
FS.tableTypes' (FS.rowGen  (framesPath electionIntegrityByState2016CSV)) { FS.rowTypeName = "ElectionIntegrityByState2016"
                                                                         , FS.tablePrefix = "PEI"
                                                                         }

FS.tableTypes' (FS.rowGen  (framesPath electionIntegrityByState2018CSV)) { FS.rowTypeName = "ElectionIntegrityByState2018"
                                                                         , FS.tablePrefix = "PEI"
                                                                         }


--F.tableTypes' (F.rowGen  (framesPath electionIntegrityByState2016CSV)) { F.rowTypeName = "ElectionIntegrityByState"
--                                                                       , F.tablePrefix = "PEI"


FS.tableTypes' presidentialByStateRowGen -- "PresidentialByState" (framesPath presidentialByStateCSV)
FS.tableTypes' (FS.rowGen (framesPath housePolls2020CSV)) { FS.rowTypeName = "HousePolls2020"
                                                          , FS.columnParsers = FS.parseableParseHowRec @FP.ColumnsWithDayAndLocalTime --Proxy :: Proxy FP.ColumnsWithDayAndLocalTime
                                                          }
FS.tableTypes "ContextDemographics" (framesPath contextDemographicsCSV)
FS.tableTypes "CVAPByCDAndRace_Raw" (framesPath cvapByCDAndRace2014_2018CSV)
FS.tableTypes "PopulationsByCounty_Raw" (framesPath popsByCountyCSV)
-- NB: cd115, cd114, cd113 are all also present and have the same table-types
FS.tableTypes "CDFromPUMA2012"       (framesPath cd116FromPUMA2012CSV)


FS.tableTypes "TurnoutASR"          (framesPath detailedASRTurnoutCSV)
FS.tableTypes "TurnoutASE"          (framesPath detailedASETurnoutCSV)
FS.tableTypes "StateTurnout"        (framesPath stateTurnoutCSV)
FS.tableTypes "ASRDemographics" (framesPath ageSexRaceDemographicsLongCSV)
FS.tableTypes "ASEDemographics" (framesPath ageSexEducationDemographicsLongCSV)
FS.tableTypes "EdisonExit2018" (framesPath exitPoll2018CSV)

FS.tableTypes "ElectoralCollege" (framesPath electorsCSV)

FS.tableTypes "States" (framesPath statesCSV)
FS.tableTypes "StateCountyCD" (framesPath stateCounty116CDCSV)
FS.tableTypes "StateCountyTractPUMA" (framesPath stateCountyTractPUMACSV)
FS.tableTypes "CountyToCD116" (framesPath countyToCD116CSV)
FS.tableTypes "CD116FromStateLower2016" (framesPath cd116FromStateLower2016CSV)
FS.tableTypes "CD116FromStateUpper2016" (framesPath cd116FromStateUpper2016CSV)
FS.tableTypes "StateLower2016FromPUMA" (framesPath stateLower2016FromPUMACSV)
FS.tableTypes "StateUpper2016FromPUMA" (framesPath stateUpper2016FromPUMACSV)
FS.tableTypes "CountyFromPUMA" (framesPath county2014FromPUMA2012CSV)
