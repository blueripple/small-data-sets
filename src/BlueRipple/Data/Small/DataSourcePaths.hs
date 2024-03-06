{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module BlueRipple.Data.Small.DataSourcePaths where

--import qualified Paths_blueripple_data_sets    as Paths
import qualified Frames.Streamly.TH            as FS
import qualified Frames.Streamly.ColumnUniverse as FS
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Language.Haskell.TH.Env as Env

thFramesDataDir :: FilePath
thFramesDataDir = fromMaybe "./data/" $$(Env.envQ "BR_SMALL_DATA")

electionDir :: FilePath
electionDir = "election/"

demographicDir :: FilePath
demographicDir = "demographic/"

campaignFinanceDir :: FilePath
campaignFinanceDir = "campaign-finance/"

dictionariesDir :: FilePath
dictionariesDir = "dictionaries/"

otherDir :: FilePath
otherDir = "other/"

totalSpendingCSV :: FilePath
totalSpendingCSV =
  campaignFinanceDir ++ "allSpendingThrough20181106.csv"

totalSpendingBeforeCSV :: FilePath
totalSpendingBeforeCSV =
  campaignFinanceDir ++ "allSpendingThrough20180731.csv"

totalSpendingDuringCSV :: FilePath
totalSpendingDuringCSV =
  campaignFinanceDir ++ "allSpendingFrom20180801Through20181106.csv"

forecastAndSpendingCSV :: FilePath
forecastAndSpendingCSV =
  campaignFinanceDir ++ "forecastAndSpending.csv"

houseElections2018CSV :: FilePath
houseElections2018CSV = electionDir ++ "1976-2018-house_v5_u1.csv"

houseElections2020CSV :: FilePath
houseElections2020CSV = electionDir ++ "1976-2020-house.csv"


houseElectionsCSV :: FilePath
houseElectionsCSV = electionDir ++ "1976-2022-house.csv"

houseElectionsCols :: Set FS.HeaderText
houseElectionsCols = S.fromList
                     $ FS.HeaderText
                     <$> ["year", "state", "state_po","state_fips","district","stage","runoff","special","candidate","party","candidatevotes","totalvotes"]

houseElectionRenames :: M.Map FS.HeaderText FS.ColTypeName
houseElectionRenames = M.mapKeys FS.HeaderText
                       $ fmap FS.ColTypeName
                       $ M.fromList [("runoff","runoffOM"),("special","specialOM")]

houseElectionsRowGen :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FS.CommonColumns
houseElectionsRowGen = setOrMissingWhen $ FS.modifyColumnSelector modSelector rg
  where
    setOrMissingWhen =  FS.setOrMissingWhen (FS.HeaderText "runoff") FS.AlwaysPossible
                        . FS.setOrMissingWhen (FS.HeaderText "special") FS.AlwaysPossible
    modSelector = FS.renameSomeUsingNames houseElectionRenames
                  . FS.columnSubset houseElectionsCols
    rg =  (FS.rowGen (framesPath $ houseElectionsCSV))
          {
            FS.rowTypeName = "HouseElections"
          , FS.tablePrefix = ""
          , FS.separator = FS.CharSeparator ','
          }

senateElectionsCSV :: FilePath
senateElectionsCSV = electionDir ++ "1976-2020-senate_u1.csv"


presidentialByStateCSV :: FilePath
presidentialByStateCSV = electionDir ++ "1976-2020-president.csv"

presidentialByStateCols ::  Set FS.HeaderText
presidentialByStateCols = S.fromList
                     $ FS.HeaderText
                     <$> ["year", "state", "state_po","state_fips","candidate","candidatevotes","totalvotes","party_simplified"]


presElectionRenames :: M.Map FS.HeaderText FS.ColTypeName
presElectionRenames = M.mapKeys FS.HeaderText
                       $ fmap FS.ColTypeName
                       $ M.fromList [("party_simplified","party")]

presidentialByStateRowGen :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FS.CommonColumns
presidentialByStateRowGen = FS.modifyColumnSelector modSelector rg
  where
    modSelector = FS.renameSomeUsingNames presElectionRenames . FS.columnSubset presidentialByStateCols
    rg =  (FS.rowGen (framesPath $ presidentialByStateCSV))
          {
            FS.rowTypeName = "PresidentialByState"
          , FS.tablePrefix = ""
          , FS.separator = FS.CharSeparator ','
          }


allMoney2020CSV :: FilePath
allMoney2020CSV = campaignFinanceDir ++ "allMoney_20200902.csv"

detailedASRTurnoutCSV :: FilePath
detailedASRTurnoutCSV =
  electionDir ++ "DetailedTurnoutByAgeSexRace2010-2018.csv"

detailedASETurnoutCSV :: FilePath
detailedASETurnoutCSV =
  electionDir ++ "DetailedTurnoutByAgeSexEducation2010-2018.csv"

stateTurnoutCSV :: FilePath
stateTurnoutCSV = electionDir ++ "StateTurnout.csv"

electionResultsCSV :: FilePath
electionResultsCSV = electionDir ++ "electionResult2018.csv"

exitPoll2018CSV :: FilePath
exitPoll2018CSV = electionDir ++ "EdisonExitPoll2018.csv"

electorsCSV :: FilePath
electorsCSV = electionDir ++ "electoral_college.csv"

housePolls2020CSV :: FilePath
housePolls2020CSV = electionDir ++ "HousePolls538_20200904.csv"

contextDemographicsCSV :: FilePath
contextDemographicsCSV =
  demographicDir ++ "contextDemographicsByDistrict.csv"

ageSexRaceDemographicsLongCSV :: FilePath
ageSexRaceDemographicsLongCSV =
  demographicDir ++ "ageSexRaceDemographics2010-2018.csv"

ageSexEducationDemographicsLongCSV :: FilePath
ageSexEducationDemographicsLongCSV =
  demographicDir ++ "ageSexEducationDemographics2010-2018.csv"

cvapByCDAndRace2014_2018CSV :: FilePath
cvapByCDAndRace2014_2018CSV =
  demographicDir ++ "CVAPByCD2014-2018.csv"

popsByCountyCSV :: FilePath
popsByCountyCSV = demographicDir ++ "populationsByCounty.csv"

electionIntegrityByState2016CSV :: FilePath
electionIntegrityByState2016CSV = electionDir ++ "EIP_Electoral_Integrity_2016.csv"

electionIntegrityByState2018CSV :: FilePath
electionIntegrityByState2018CSV = electionDir ++ "EIP_Electoral_Integrity_2018.csv"


{-
puma2012ToCD116CSV :: FilePath
puma2012ToCD116CSV = demographicDir ++ "puma2012ToCD116.csv"

puma2000ToCD116CSV :: FilePath
puma2000ToCD116CSV = demographicDir ++ "puma2000ToCD116.csv"
-}
cd118FromPUMA2012CSV:: FilePath
cd118FromPUMA2012CSV = dictionariesDir ++ "cd118FromPUMA2012.csv"

cd117FromPUMA2012CSV:: FilePath
cd117FromPUMA2012CSV = dictionariesDir ++ "cd117FromPUMA2012.csv"

cd116FromPUMA2012CSV:: FilePath
cd116FromPUMA2012CSV = dictionariesDir ++ "cd116FromPUMA2012.csv"

cd115FromPUMA2012CSV:: FilePath
cd115FromPUMA2012CSV = dictionariesDir ++ "cd115FromPUMA2012.csv"

cd114FromPUMA2012CSV:: FilePath
cd114FromPUMA2012CSV = dictionariesDir ++ "cd114FromPUMA2012.csv"

cd113FromPUMA2012CSV:: FilePath
cd113FromPUMA2012CSV = dictionariesDir ++ "cd113FromPUMA2012.csv"

county2014FromPUMA2012CSV :: FilePath
county2014FromPUMA2012CSV = dictionariesDir ++ "2012PUMATo2014County.csv"


statesCSV :: FilePath
statesCSV = dictionariesDir ++ "states.csv"

stateCounty116CDCSV :: FilePath
stateCounty116CDCSV = dictionariesDir ++ "StateCounty116CD.csv"

stateCountyTractPUMACSV :: FilePath
stateCountyTractPUMACSV = dictionariesDir ++ "2010StateCountyTractPUMA.csv"

countyToCD116CSV :: FilePath
countyToCD116CSV = dictionariesDir ++ "2010CountyToCD116.csv"

cd116FromStateLower2016CSV :: FilePath
cd116FromStateLower2016CSV = dictionariesDir ++ "cd116FromStateLower2016.csv"

cd116FromStateUpper2016CSV :: FilePath
cd116FromStateUpper2016CSV = dictionariesDir ++ "cd116FromStateUpper2016.csv"

stateLower2016FromPUMACSV :: FilePath
stateLower2016FromPUMACSV = dictionariesDir ++ "StateLower2016FromPUMA.csv"

stateUpper2016FromPUMACSV :: FilePath
stateUpper2016FromPUMACSV = dictionariesDir ++ "StateUpper2016FromPUMA.csv"

angryDemsCSV :: FilePath
angryDemsCSV = otherDir ++ "angryDemsContributions20181203.csv"

framesPath :: FilePath -> FilePath
framesPath x = thFramesDataDir ++ x

{-
usePath :: FilePath -> IO FilePath
usePath x = fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir

dataPath :: FilePath -> IO FilePath
dataPath = Paths.getDataFileName
-}
