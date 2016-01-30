using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MathNet.Numerics.Statistics;
using System.Globalization;
using System.Threading;
using System.Text;

namespace MDR
{
    public class Program
    {
        private static Dictionary<string, TickerInfo> perTickData = Configuration.GetAvailableTickerInfos();
        private static Dictionary<string, TickerInfo> completeData = Configuration.GetAvailableTickerInfos();
        private static int currentTick = 0;
        public static Dictionary<string, IList<Alert>> Alerts = new Dictionary<string, IList<Alert>>();
        private static string Filename = "";
        public static Dictionary<string, TickerInfo> PerTickRecords
        {
            get
            {
                return perTickData;
            }
            set
            {
                perTickData = value;
            }
        }

        public static Dictionary<string, TickerInfo> AllTickRecords
        {
            get
            {
                return completeData;
            }
            set
            {
                completeData = value;
            }
        }
        static void Main(string[] args)
        {
            try
            {
                
                Console.WriteLine("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Started processing asset classes $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
               // ProcessBondData();
                //Console.WriteLine("Please enter key for processing fx data");
                //Console.ReadLine();
                //ProcessFxData();
                ProcessEquityData();
                Console.WriteLine("Successfully completed process");
                foreach (var alert in Program.Alerts)
                {
                    Console.WriteLine("Alerts count for Ticker {0} : {1}", alert.Key, alert.Value.Count);
                }
                Console.ReadLine();
                //for (; ; ) { }
                //stData.GetStreamingData(PerTickRecords);
                //timer.Start();

            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("Exception occoured ex:{0}", ex.Message));
                Console.ReadLine();
            }
        }

        private static void ProcessBondData()
        {
            TimeSliceBucketLookUpAlgo timeSlice = new TimeSliceBucketLookUpAlgo(0, 5);
            var bucket = timeSlice.getTimeBucket();
            var bondFolderLocation = StreamData.ReadAppSetting("BondFolderLocation");
            var bondFolders = bondFolderLocation.Split(',');
            var bondFiles = StreamData.ReadAppSetting("BondFileName").Split(',');
            //Program p = new Program();
            Console.WriteLine("**************Start processing bonds records **************************");
            for (var i = 0; i < bucket.GetLength(0); i++)
            {
                for (var index = 0; index < bondFolders.Length; index++)
                {
                    var fileName = bondFiles[index] + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];
                    ProcesStreamData(fileName, PerTickRecords, AllTickRecords, bondFolders[index], Configuration.Bond, string.Empty, string.Empty);
                    PrintData();
                }

                //Console.WriteLine(string.Format("PerTickMarketDepth:{0},AllTick{1}", PerTickRecords[Configuration.T3].MarketDepth.Count, AllTickRecords[Configuration.T3].MarketDepth.Count));

                //Thread.Sleep(1000);
            }

            Console.WriteLine("**************Finished processing bonds records **************************");
        }

        private static void ResetDictionaries()
        {
            PerTickRecords.Clear();
            AllTickRecords.Clear();
            PerTickRecords = Configuration.GetAvailableTickerInfos();
            AllTickRecords = Configuration.GetAvailableTickerInfos();
        }

        private static void ProcessFxData()
        {
            TimeSliceBucketLookUpAlgo timeSlice = new TimeSliceBucketLookUpAlgo(0, 5);
            var bucket = timeSlice.getTimeBucket();
            currentTick = 0;
            ResetDictionaries();
            var fxFolderLocation = StreamData.ReadAppSetting("FxFolderLocation");
            var fxTradeFolderLocation = StreamData.ReadAppSetting("FxTradeFolderLocation");
            string fxTradeFileName = string.Empty;
            //Program p = new Program();
            Console.WriteLine("**************Start processing Fx records **************************");
            for (var i = 0; i < bucket.GetLength(0); i++)
            {
                var fileName = StreamData.ReadAppSetting("FxFileName") + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];
                fxTradeFileName = StreamData.ReadAppSetting("FxTradeFileName") + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];//StreamData.ReadAppSetting("FxTradeFileName");
                ProcesStreamData(fileName, PerTickRecords, AllTickRecords, fxFolderLocation, Configuration.Fx, fxTradeFolderLocation, fxTradeFileName);

                //Console.WriteLine(string.Format("PerTickMarketDepth:{0}--,AllTickMarketDepth:{1}--PerTickTradeData:{2}--,AllTickTradeData:{3}", PerTickRecords[Configuration.T5].MarketDepth.Count, AllTickRecords[Configuration.T5].MarketDepth.Count, PerTickRecords[Configuration.T5].TradeData.Count, AllTickRecords[Configuration.T5].TradeData.Count));
                // Thread.Sleep(500);
            }

            Console.WriteLine("**************Finished processing Fx records **************************");
        }

        private static void ProcessEquityData()
        {
            TimeSliceBucketLookUpAlgo timeSlice = new TimeSliceBucketLookUpAlgo(0, 5);
            var bucket = timeSlice.getTimeBucket();
            currentTick = 0;
            ResetDictionaries();
            var eqFolderLocation = StreamData.ReadAppSetting("EqFolderLocation");
            var eqTradeFolderLocation = StreamData.ReadAppSetting("EqTradeFolderLocation");
            var tradeFolderLocations = eqTradeFolderLocation.Split(',');
            var fxFoldersLocations = eqFolderLocation.Split(',');
            var eqFileNames = StreamData.ReadAppSetting("EqFileName").Split(',');
            for (var index = 0; index < tradeFolderLocations.Length; index++)
            {
                string eqTradeFileName = string.Empty;
                //Program p = new Program();
                Console.WriteLine("**************Start processing Equity records **************************");
                //StreamWriter.Write("**************Start processing Equity records **************************");
                for (var i = 0; i < bucket.GetLength(0); i++)
                {
                    //var fxDataFileName = StreamData.ReadAppSetting("EqFileName") + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];
                    //eqTradeFileName = StreamData.ReadAppSetting("EqTradeFileName") + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];//StreamData.ReadAppSetting("FxTradeFileName");
                    var fxDataFileName = eqFileNames[index] + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];
                    eqTradeFileName = eqFileNames[index] + "_" + bucket[i, 2] + "-" + bucket[i, 3] + "_" + bucket[i, 4] + "-" + bucket[i, 5];//StreamData.ReadAppSetting("FxTradeFileName");
                    ProcesStreamData(fxDataFileName, PerTickRecords, AllTickRecords, fxFoldersLocations[index], Configuration.Equity, tradeFolderLocations[index], eqTradeFileName);
                    PrintData();
                    //Console.WriteLine(string.Format("PerTickMarketDepth:{0}--,AllTickMarketDepth:{1}--PerTickTradeData:{2}--,AllTickTradeData:{3}", PerTickRecords[Configuration.T5].MarketDepth.Count, AllTickRecords[Configuration.T5].MarketDepth.Count, PerTickRecords[Configuration.T5].TradeData.Count, AllTickRecords[Configuration.T5].TradeData.Count));
                    //Thread.Sleep(500);
                }
            }


            Console.WriteLine("**************Finished processing Equity records **************************");
        }

        private static void PrintData()
        {
            StringBuilder strPrint = new StringBuilder();
            string perTicketPrintString = "PerTicker market depth Data:";
            DateTime maxTime, minTime;
            foreach (var rec in PerTickRecords)
            {
                if (rec.Value.MarketDepth.Count > 0)
                {
                    maxTime = rec.Value.MarketDepth.Max(x => x.Time);
                    minTime = rec.Value.MarketDepth.Min(x => x.Time);
                    strPrint.Append(string.Format(" [PerTick] market depth data:--MarketSymbol: {0} : and MarketDepth Count is: {1}--between time :{2}--{3} ", rec.Value.Ticker.TickerCode, rec.Value.MarketDepth.Count, minTime, maxTime));
                }
                //perTicketPrintString += "----Trade data:";
                if (rec.Value.TradeData != null && rec.Value.TradeData.Count > 0)
                {
                    maxTime = rec.Value.TradeData.Max(x => x.Time);
                    minTime = rec.Value.TradeData.Min(x => x.Time);
                    strPrint.Append(string.Format(" [PerTick] Trade data :---Symbol: {0} : and Trade data count: {1}--between time : {2}--{3} ", rec.Value.Ticker.TickerCode, rec.Value.TradeData.Count, minTime, maxTime));
                }
            }
            //perTicketPrintString += "All Tick records:";
            foreach (var rec in AllTickRecords)
            {
                if (rec.Value.MarketDepth != null && rec.Value.MarketDepth.Count > 0)
                {
                    maxTime = rec.Value.MarketDepth.Max(x => x.Time);
                    minTime = rec.Value.MarketDepth.Min(x => x.Time);
                    strPrint.Append(string.Format(" [AllTick] market depth:-- MarketSymbol: {0} : and Market Depth Count is : {1}--- between time:{2}--{3} ", rec.Value.Ticker.TickerCode, rec.Value.MarketDepth.Count, minTime, maxTime));
                }
                //perTicketPrintString += "----Trade data:";
                if (rec.Value.TradeData != null && rec.Value.TradeData.Count > 0)
                {
                    maxTime = rec.Value.TradeData.Max(x => x.Time);
                    minTime = rec.Value.TradeData.Min(x => x.Time);
                    strPrint.Append(string.Format(" [AllTick] Trade data:--Symbol : {0} and Trade Data Count is : {1} --- between time:{2}--{3} ", rec.Value.Ticker.TickerCode, rec.Value.TradeData.Count, minTime, maxTime));
                    //perTicketPrintString += string.Format("AllTickTrade:{0}:{1}", rec.Value.Ticker.TickerCode, rec.Value.TradeData.Count);
                }
            }

            Console.WriteLine(strPrint.ToString());
            WriteTextFile.Write(strPrint.ToString(), "OtherLogFileLocation");
            //Console.WriteLine(string.Format("PerTickMarketDepth:{0}--,AllTickMarketDepth:{1}--PerTickTradeData:{2}--,AllTickTradeData:{3}", PerTickRecords[Configuration.T5].MarketDepth.Count, AllTickRecords[Configuration.T5].MarketDepth.Count, PerTickRecords[Configuration.T5].TradeData.Count, AllTickRecords[Configuration.T5].TradeData.Count));
        }
        public static void BackupPreviousRecords(Dictionary<string, TickerInfo> sourceDicts, Dictionary<string, TickerInfo> destinationDicts)
        {
            var groupedMdList = sourceDicts.GroupBy(x => x.Key).Select(y => y.Key).ToList();
            foreach (var key in groupedMdList)
            {
                if (sourceDicts[key].MarketDepth.Count > 0)
                {
                    var minTime = sourceDicts[key].MarketDepth.Min(x => x.Time);
                    var maxTime = sourceDicts[key].MarketDepth.Max(x => x.Time);
                    var diff = (maxTime - minTime).Minutes;
                    //if ((maxTime-minTime)+1 >= 15) 
                    //if ((maxMinute - 15) > 0)
                    if (diff > 15)
                    {
                        var mintsToTruncate = diff - 15;
                        var timeToRemoveInMint = minTime.AddMinutes(mintsToTruncate);
                        //var timeToRemoveInMint = maxMinute - 15;// + 5;
                        var remainingMarketDepth = sourceDicts[key].MarketDepth.Where(x => x.Time > timeToRemoveInMint).ToList();
                        var remainingTradeData = sourceDicts[key].TradeData.Where(x => x.Time > timeToRemoveInMint).ToList();
                        sourceDicts[key].MarketDepth.Clear();
                        sourceDicts[key].TradeData.Clear();
                        sourceDicts[key].MarketDepth = remainingMarketDepth;
                        sourceDicts[key].TradeData = remainingTradeData;
                    }
                }
            }

        }

        private static void ProcesStreamData(string fileName, Dictionary<string, TickerInfo> PerTickRecords, Dictionary<string, TickerInfo> AllTickRecords, string folderLocation, string typeOfAssetClass, string tradeDataFolderLocation, string tradeDataFileName)
        {
            StreamData stData = new StreamData();
            currentTick = currentTick + Configuration.TickIntervalTimeInMinutes;

            stData.GetStreamingData(PerTickRecords, fileName, folderLocation, typeOfAssetClass, tradeDataFolderLocation, tradeDataFileName);
            //if (currentTick >= Configuration.BackupIntervalTimeInMinutes)
            //{
            Console.WriteLine("*****************************inside backup*******************************");
            Program.BackupPreviousRecords(PerTickRecords, AllTickRecords);

            foreach (var ticker in AllTickRecords)
            {
                ticker.Value.CheckForMarketActivity();

                if (Configuration.AvailableTickers[ticker.Key] == Configuration.Bond)
                {
                    ticker.Value.CheckBondPriceVolatality();
                    ticker.Value.CheckForBondHighLowPriceRange();
                }
                else
                {
                    ticker.Value.CheckForPriceVolatility();
                    ticker.Value.CheckForVolume();
                    ticker.Value.CheckForHighLowPriceRange();
                }
            }
            currentTick = 0;
            //}
        }
    }

    public class StreamData
    {
        //Dictionary<string, TickerInfo> currentStream = new Dictionary<string, TickerInfo>();

        public void GetStreamingData(Dictionary<string, TickerInfo> perTickData, string fileName, string folderLocation, string typeOfAssetClass, string tradeDataFolderLocation, string tradeDataFileName)
        {

            var bondFileName = ReadAppSetting("BondFileName");
            //var finalBondFileName = string.Format("{0}_{1}-{2}_{3}-{4}.csv", bondFileName, MDR.Program.FileVariables.FromHH, Program.FileVariables.FromMM, Program.FileVariables.ToHH, Program.FileVariables.ToMM);
            var finalFileName = string.Format("{0}.csv", fileName);
            Console.WriteLine(string.Format("processing file:{0}", fileName));
            finalFileName = string.Format(@"{0}\\{1}", folderLocation, finalFileName);
            var dataMd = File.ReadLines(finalFileName).Select(line => line.Split(',')).ToList();
            // var dict = File.ReadLines("test_file.csv"); 
            // ResolveTickers(currentStream, dataBondMd, dataEqMd, dataEqTd, dataFxMd, dataFxTd);
            if (dataMd.Count > 0)
            {
                ResolveTickers(Program.PerTickRecords, dataMd, typeOfAssetClass, false);
                ResolveTickers(Program.AllTickRecords, dataMd, typeOfAssetClass, false);
            }

            if (!string.IsNullOrEmpty(tradeDataFileName))
            {
                var finalTradeFileName = string.Format("{0}.csv", tradeDataFileName);
                Console.WriteLine(string.Format("processing Trade File file:{0}.csv", tradeDataFileName));
                finalTradeFileName = string.Format(@"{0}\\{1}.csv", tradeDataFolderLocation, tradeDataFileName);
                try
                {
                    var tradeDataMd = File.ReadLines(finalTradeFileName).Select(line => line.Split(',')).ToList();
                    if (tradeDataMd.Count > 0)
                    {
                        ResolveTickers(Program.PerTickRecords, tradeDataMd, typeOfAssetClass, true);
                        ResolveTickers(Program.AllTickRecords, tradeDataMd, typeOfAssetClass, true);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Unable to processed trade data for asset class:{0} and file name:{1}--Ex:{2}", typeOfAssetClass, finalTradeFileName, ex.Message);
                }
            }
            //foreach (var ticker in perTickData)
            //{
            //    //Console.WriteLine("{0} Market Data & {1} Trade Data rows processed for ticker {2} from Asset Class {3}", ticker.Value.MarketDepth.Count, ticker.Value.TradeData, ticker.Key, ticker.Value.Ticker.AssetClassType);
            //    //Console.WriteLine(" Bid Ask Spread Details=> Mean: {0} Standard Dev: {1}", ticker.Value.Value.MeanBas, ticker.Value.Value.StdDevBAas);
            //}
            foreach (var ticker in Program.PerTickRecords)
            {
                ticker.Value.CheckForLiquidity(ticker.Value);
                if (ticker.Value.Ticker.AssetClassType != Configuration.Bond)
                {
                    ticker.Value.CheckForOrderImbalance();
                }
                //Console.WriteLine("{0} Market Data & {1} Trade Data rows processed for ticker {2} from Asset Class {3}", ticker.Value.MarketDepth.Count, ticker.Value.TradeData, ticker.Key, ticker.Value.Ticker.AssetClassType);
                //Console.WriteLine(" Bid Ask Spread Details=> Mean: {0} Standard Dev: {1}", ticker.Value.Value.MeanBas, ticker.Value.Value.StdDevBAas);
            }

            //Program.FileVariables.FromMM =
            // Console.ReadLine();
        }

        public static string ReadAppSetting(string key)
        {
            var result = string.Empty;
            try
            {
                var appSettings = System.Configuration.ConfigurationSettings.AppSettings;
                result = appSettings[key] ?? string.Empty;
                Console.WriteLine(result);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error reading app settings");
            }
            return result;
        }

        private void ResolveTickers(Dictionary<string, TickerInfo> currentStream, List<string[]> dataMd, string typeOfAssetClass, bool isTradeData)
        {
            IList<MarketDepth> mdList = new List<MarketDepth>();
            IList<TradeData> mdTradeList = new List<TradeData>();
            switch (typeOfAssetClass)
            {
                case Configuration.Bond:
                    if (!isTradeData)
                    {
                        mdList = ResolveBondMd(dataMd);
                    }
                    break;
                case Configuration.Fx:
                    if (!isTradeData)
                    {
                        mdList = ResolveFxMd(dataMd);
                    }
                    else
                    {
                        mdTradeList = ResolveFxTradeMd(dataMd);
                    }
                    break;
                case Configuration.Equity:
                    if (!isTradeData)
                    {
                        mdList = ResolveEqMd(dataMd);
                    }
                    else
                    {
                        mdTradeList = ResolveEqTrade(dataMd);
                    }
                    break;
                //case Configuration.Bond:
                //    mdList = ResolveBondMd(dataBondMd);
                //    break;
            }

            if (!isTradeData)
            {
                var groupedMdList = mdList.GroupBy(x => x.Symbol).ToList();
                foreach (var key in groupedMdList)
                {
                    TickerInfo value;
                    if (!currentStream.TryGetValue(key.Key.Trim(), out value))
                    {
                        var tickerInfo = new TickerInfo(key.Key.Trim())
                        {
                            MarketDepth = key.ToList(),
                            Ticker = new Ticker
                            {
                                TickerCode = key.Key.Trim(),
                                AssetClassType = Configuration.AvailableTickers[key.Key.Trim()],
                            },
                        };
                        tickerInfo.Calc();
                        currentStream.Add(key.Key.Trim(), tickerInfo);
                    }
                    else
                    {
                        var matchedDict = currentStream[key.Key.Trim()];
                        if (matchedDict != null)
                        {
                            foreach (var lst in key.ToList())
                            {
                                matchedDict.MarketDepth.Add(lst);
                            }

                            matchedDict.Ticker = new Ticker()
                            {
                                TickerCode = key.Key.Trim(),
                                AssetClassType = Configuration.AvailableTickers[key.Key.Trim()]
                            };
                            matchedDict.Calc();
                        }
                    }
                }
            }
            else
            {
                //considering key will already be created while inserting trade data.so handling situation for update stuff only.
                var groupedTradeMdList = mdTradeList.GroupBy(x => x.Symbol).ToList();
                foreach (var key in groupedTradeMdList)
                {
                    var matchedDict = currentStream[key.Key.Trim()];
                    if (matchedDict != null)
                    {
                        foreach (var lst in key.ToList())
                        {
                            matchedDict.TradeData.Add(lst);
                        }

                        matchedDict.Calc();
                    }
                }
            }
        }


        private IList<MarketDepth> ResolveEqMd(List<string[]> dataEqMd)
        {
            var list = new List<MarketDepth>();
            foreach (var data in dataEqMd)
            {
                var md = new MarketDepth();
                md.Symbol = data[Convert.ToInt32(Configuration.EnumEqMd.RIC)];
                md.Time = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumEqMd.Time)], new CultureInfo("en-US"));
                md.FileDate = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumEqMd.Date)], new CultureInfo("en-US"));
                //md.AskPrice1 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqMd.L1_AskPrice)]);
                //md.BidPrice1 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqMd.L1_BidPrice)]);
                //md.AskPrice2 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqMd.L2_AskPrice)]);
                //md.BidPrice2 = ParseDouble(dtaa[Convert.ToInt32(Configuration.EnumEqMd.L2_BidPrice)]);
                //md.AskPrice3 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqMd.L3_AskPrice)]);
                //md.BidPrice3 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqMd.L3_BidPrice)]);
                md.AskPrice1 = ParseDouble(ParseArray(data, Convert.ToInt32(Configuration.EnumEqMd.L1_AskPrice)));
                md.BidPrice1 = ParseDouble(ParseArray(data, Convert.ToInt32(Configuration.EnumEqMd.L1_BidPrice)));
                md.AskPrice2 = ParseDouble(ParseArray(data, Convert.ToInt32(Configuration.EnumEqMd.L2_AskPrice)));
                md.BidPrice2 = ParseDouble(ParseArray(data, Convert.ToInt32(Configuration.EnumEqMd.L2_BidPrice)));
                md.AskPrice3 = ParseDouble(ParseArray(data, Convert.ToInt32(Configuration.EnumEqMd.L3_AskPrice)));
                md.BidPrice3 = ParseDouble(ParseArray(data, Convert.ToInt32(Configuration.EnumEqMd.L3_BidPrice)));

                list.Add(md);
            }
            return list;
        }
        private string ParseArray(string[] arr, int index)
        {
            if (index < arr.Length)
            {
                return arr[index];
            }
            return string.Empty;
        }

        private IList<TradeData> ResolveEqTrade(List<string[]> dataEqMd)
        {
            var list = new List<TradeData>();
            foreach (var data in dataEqMd)
            {
                var md = new TradeData();
                md.Symbol = data[Convert.ToInt32(Configuration.EnumEqTd.RIC)];
                md.Time = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumEqTd.Time)], new CultureInfo("en-US"));
                md.Volume = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqTd.Volume)]);
                md.Price = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqTd.Price)]);
                md.Date = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumEqTd.Date)], new CultureInfo("en-US"));
                //md.Quality= ParseDouble(data[Convert.ToInt32(Configuration.EnumEqTd.Q)]);
                //md.Interval= ParseDouble(data[Convert.ToInt32(Configuration.EnumEqTd.L2_BidPrice)]);
                //md.AskPrice3 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqTd.L3_AskPrice)]);
                //md.BidPrice3 = ParseDouble(data[Convert.ToInt32(Configuration.EnumEqTd.L3_BidPrice)]);
                list.Add(md);
            }
            return list;
        }


        private IList<MarketDepth> ResolveBondMd(List<string[]> dataBondMd)
        {
            var list = new List<MarketDepth>();
            CultureInfo c = CultureInfo.InvariantCulture;
            //var format = "mm dd yyyy h:mm tt zzz";            
            foreach (var data in dataBondMd)
            {

                var md = new MarketDepth();
                md.Symbol = data[Convert.ToInt32(Configuration.EnumBondMd.Symbol)];
                md.Time = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumBondMd.Time)], new CultureInfo("en-US"));
                md.AskPrice1 = Convert.ToDouble(data[Convert.ToInt32(Configuration.EnumBondMd.ASK_PRICE)]);
                md.BidPrice1 = Convert.ToDouble(data[Convert.ToInt32(Configuration.EnumBondMd.BID_PRICE)]);


                list.Add(md);
            }
            return list;
        }

        private IList<MarketDepth> ResolveFxMd(List<string[]> dataFxMd)
        {
            var list = new List<MarketDepth>();
            foreach (var data in dataFxMd)
            {
                var md = new MarketDepth();
                md.Symbol = Configuration.GetTickerCodeBasedOnValue(data[Convert.ToInt32(Configuration.EnumFxMd.Symbol)]);
                md.Time = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumFxMd.Time)]);
                md.AskPrice1 = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxMd.ASK_PRICE1)]);
                md.BidPrice1 = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxMd.BID_PRICE1)]);
                md.AskPrice2 = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxMd.ASK_PRICE2)]);
                md.BidPrice2 = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxMd.BID_PRICE2)]);
                md.AskPrice3 = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxMd.ASK_PRICE3)]);
                md.BidPrice3 = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxMd.BID_PRICE3)]);
                list.Add(md);
            }
            return list;
        }

        private IList<TradeData> ResolveFxTradeMd(List<string[]> tradedDataFxMd)
        {
            var list = new List<TradeData>();
            foreach (var data in tradedDataFxMd)
            {
                var md = new TradeData();
                md.Index = Convert.ToInt32(data[Convert.ToInt32(Configuration.EnumFxTd.Index)]);
                md.Symbol = Configuration.GetTickerCodeBasedOnValue(data[Convert.ToInt32(Configuration.EnumFxTd.Symbol)]);
                //md.Time = Convert.ToDateTime(data[Convert.ToInt32(Configuration.EnumFxTd.Time)]);
                //md.Quality = data[Convert.ToInt32(Configuration.EnumFxTd.QUALITY)];
                //md.Price = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxTd.PRICE)]);
                //md.Volume = ParseDouble(data[Convert.ToInt32(Configuration.EnumFxTd.VOLUME)]);
                list.Add(md);
            }
            return list;
        }
        public static double ParseDouble(string val)
        {
            double res = 0.0;
            try
            {
                res = string.IsNullOrEmpty(val.Trim()) ? 0.0 : Convert.ToDouble(val);
            }
            catch (Exception ex)
            {
                throw new ApplicationException(string.Format("Unable to parse double value:{0} ex:{1}", val, ex.Message));
            }

            return res;
        }
    }


    public static class Configuration
    {
        static Configuration()
        {
            GetNormalvariables();
        }
        public enum EnumFxMd { Index, Symbol, Time, BID_PRICE1, BID_UPDATE_TIME1, BID_SIZE1, ASK_PRICE1, ASK_UPDATE_TIME1, ASK_SIZE1, BID_PRICE2, BID_UPDATE_TIME2, BID_SIZE2, ASK_PRICE2, ASK_UPDATE_TIME2, ASK_SIZE2, BID_PRICE3, BID_UPDATE_TIME3, BID_SIZE3, ASK_PRICE3, ASK_UPDATE_TIME3, ASK_SIZE3, BID_PRICE4, BID_UPDATE_TIME4, BID_SIZE4, ASK_PRICE4, ASK_UPDATE_TIME4, ASK_SIZE4, BID_PRICE5, BID_UPDATE_TIME5, BID_SIZE5, ASK_PRICE5, ASK_UPDATE_TIME5, ASK_SIZE5, BID_PRICE6, BID_UPDATE_TIME6, BID_SIZE6, ASK_PRICE6, ASK_UPDATE_TIME6, ASK_SIZE6, BID_PRICE7, BID_UPDATE_TIME7, BID_SIZE7, ASK_PRICE7, ASK_UPDATE_TIME7, ASK_SIZE7, BID_PRICE8, BID_UPDATE_TIME8, BID_SIZE8, ASK_PRICE8, ASK_UPDATE_TIME8, ASK_SIZE8, BID_PRICE9, BID_UPDATE_TIME9, BID_SIZE9, ASK_PRICE9, ASK_UPDATE_TIME9, ASK_SIZE9, BID_PRICE10, BID_UPDATE_TIME10, BID_SIZE10, ASK_PRICE10, ASK_UPDATE_TIME10, ASK_SIZE10 };
        public enum EnumFxTd { Index, Symbol, Time, QUALITY, PRICE, UPDATE_DATE_TIME, VOLUME, INTERVAL, COUNT, ADAPTOR_TIMESTAMP, OT_TIMESTAMP };
        public enum EnumBondMd { Index, Symbol, Time, BID_PRICE, ASK_PRICE, PROD_PERM, TIMEACT };
        public enum EnumEqMd { RIC, Current_RIC, Date, Time, GMT_Offset, Type, L1_BidPrice, L1_BidSize, L1_AskPrice, L1_AskSize, L2_BidPrice, L2_BidSize, L2_AskPrice, L2_AskSize, L3_BidPrice, L3_BidSize, L3_AskPrice, L3_AskSize, L4_BidPrice, L4_BidSize, L4_AskPrice, L4_AskSize, L5_BidPrice, L5_BidSize, L5_AskPrice, L5_AskSize };
        public enum EnumEqTd { RIC, Current_RIC, Date, Time, GMT_Offset, Type, Price, Volume, Acc_Volume };
        public enum EnumNormalVar { Symbol, MeanBidAskSpread, SdBidAskSpread, MeanDailyPriceVolat, SdDailyPriceVola, AvgVolume, AvgTickCount, MeanHighLowRange, SdHighLowRange };

        public const string T1 = "CBBT::ISIN:CH0286864027.CBBT";
        public const string T2 = "CBBT::ISIN:GB00B84Z9V04.CBBT";
        public const string T3 = "CBBT::ISIN:US912828J272.CBBT";
        public const string T4 = "CBBT::ISIN:USG84228CE61.CBBT";
        public const string T5 = "USD_SGD";
        public const string T6 = "AUD_USD";
        public const string T7 = "EUR_USD";
        public const string T8 = "GBP_USD";
        public const string T9 = "USD_JPY";
        public const string T10 = "1398.HK";
        public const string T11 = "2823.HK";
        public const string T12 = "CNKY.L";
        public const string T13 = "ESH6";
        public const string T14 = ".HSCE";
        public const string T15 = "T15";
        public const string T16 = "BGN_NY::XS0847042024";
        public const string T17 = "CBBT::ISIN:XS0847042024.CBBT";
        //

        public const string Equity = "Equity";
        public const string Bond = "Bond";
        public const string Fx = "Fx";

        public static int TimeSpan = 5;
        public static int NumberOfIterations = 5;
        public static double PercentVolumeForVolatality = 25.00;

        //public const string T5 = "LQ2::USD.SGD.SPOT.EBSAiFLon.LQ2";
        //public const string T6 = "LQ2::AUD.USD.SPOT.EBSLiveA1FLon.LQ2";
        //public const string T7 = "LQ2::EUR.USD.SPOT.EBSAiFLon.LQ2";
        //public const string T8 = "LQ2::GBP.USD.SPOT.EBSAiFLon.LQ2";
        //public const string T9 = "LQ2::USD.JPY.SPOT.EBSAiFLon.LQ2";

        public static Dictionary<string, List<string>> KeyValueMapping = new Dictionary<string, List<string>> { 
        {T5,new List<string>{"LQ2::USD.SGD.SPOT.EBSAiFLon.LQ2"}},
         {T6,new List<string>{"LQ2::AUD.USD.SPOT.EBSLiveA1FLon.LQ2","BLACKBIRD::AUD.USD.VWAP.EBSIndicator.BLACKBIRD"}},
          {T7,new List<string>{"LQ2::EUR.USD.SPOT.EBSAiFLon.LQ2","BLACKBIRD::EUR.USD.VWAP.EBSIndicator.BLACKBIRD"}},
           {T8,new List<string>{"LQ2::GBP.USD.SPOT.EBSAiFLon.LQ2","BLACKBIRD::GBP.USD.VWAP.EBSIndicator.BLACKBIRD"}},
            {T9,new List<string>{"LQ2::USD.JPY.SPOT.EBSAiFLon.LQ2","BLACKBIRD::USD.JPY.VWAP.EBSIndicator.BLACKBIRD"}}           
            };

        public static string GetTickerCodeBasedOnValue(string val)
        {
            foreach (var ticker in KeyValueMapping)
            {
                if (ticker.Value.Contains(val.Trim()))
                {
                    return ticker.Key;
                }
            }
            return val;
        }

        public static Dictionary<string, string> AvailableTickers = new Dictionary<string, string> 
         { 
             {T1,Bond}, 
             {T2,Bond}, 
             {T3,Bond}, 
             {T4,Bond}, 
             {T5,Fx}, 
             {T6,Fx}, 
             {T7,Fx}, 
             {T8,Fx}, 
             {T9,Fx}, 
             {T10,Equity}, 
             {T11,Equity}, 
             {T12,Equity}, 
             {T13,Equity}, 
             {T14,Equity}, 
             {T15,Equity},
             {T16,Bond},
             {T17,Bond}
         };

        public static IList<string> RedundantTickers=new List<string>{T16,T17};

        public static int TickIntervalTimeInMinutes = 5;
        public static int BackupIntervalTimeInMinutes = 30;

        public static Dictionary<string, TickerInfo> GetAvailableTickerInfos()
        {
            return new Dictionary<string, TickerInfo>()
         { 
             {T1,new TickerInfo(T1)}, 
             {T2,new TickerInfo(T2)}, 
             {T3,new TickerInfo(T3)}, 
             {T4,new TickerInfo(T4)}, 
             {T5,new TickerInfo(T5)}, 
             {T6,new TickerInfo(T6)}, 
             {T7,new TickerInfo(T7)}, 
             {T8,new TickerInfo(T8)}, 
             {T9,new TickerInfo(T9)}, 
             {T10,new TickerInfo(T10)}, 
             {T11,new TickerInfo(T11)}, 
             {T12,new TickerInfo(T12)}, 
             {T13,new TickerInfo(T13)}, 
             {T14,new TickerInfo(T14)}, 
             {T15,new TickerInfo(T15)},
             {T16,new TickerInfo(T16)},
             {T17,new TickerInfo(T16)}
 
         };
        }

        public static Dictionary<string, TuningParameters> tuningParameters = new Dictionary<string, TuningParameters> { 
        {Fx,new TuningParameters{
            BidAskSprdSdMul=1.618,BidAskSprdSdPct=0.25,OrderImbalancePct=0.25,PriceVolatalitySdMul=1.618,
        MinVolumeFilter=0.5,HighVolMul=1.5,VeryHighVolMul=2,ExtremelyHighVolMul=3,TradingHours=8,HighLowSdMul=1.618,HighTickCountMul=1.5}},
        {Bond,new TuningParameters{
            BidAskSprdSdMul=1.618,BidAskSprdSdPct=0.25,PriceVolatalitySdMul=1.618,
             MinVolumeFilter=0.5,HighVolMul=1.5,VeryHighVolMul=2,ExtremelyHighVolMul=3,TradingHours=8,HighLowSdMul=0,HighTickCountMul=1.5
      }},
        {Equity,new TuningParameters{
            BidAskSprdSdMul=1.618,BidAskSprdSdPct=0.25,OrderImbalancePct=0.25,PriceVolatalitySdMul=1.618,
        MinVolumeFilter=0.5,HighVolMul=1.5,VeryHighVolMul=2,ExtremelyHighVolMul=3,TradingHours=8,HighLowSdMul=1.618,HighTickCountMul=1.5}}        
        };

        public static Dictionary<string, NormalVariables> NormalVariables = new Dictionary<string, NormalVariables>();
        //public static Dictionary<string, NormalVariables> NormalVariables = new Dictionary<string, NormalVariables>()
        //{
        //     {T1,new NormalVariables{
        //     MeanBidAskSpread=0.548228725943495,StdDevBidAskSpread=0.0710575790351316,MeanDailyPriceVolatility=0.0574140805624918,StdDevDailyPriceVolatility=0.0432378112871989
        //     }}, 
        //     {T2,new NormalVariables{
        //     MeanBidAskSpread=0.0559658559989652,StdDevBidAskSpread=0.0240375554247955,MeanDailyPriceVolatility=0.301156068968559,StdDevDailyPriceVolatility=0.163514319192513
        //     }}, 
        //     {T3,new NormalVariables{
        //     MeanBidAskSpread=0.0224296052141681,StdDevBidAskSpread=0.00899149017456689,MeanDailyPriceVolatility=0.13287893938824,StdDevDailyPriceVolatility=0.0887286324072255
        //     }}, 
        //     {T4,new NormalVariables{
        //     MeanBidAskSpread=0.62360657915293,StdDevBidAskSpread=0.109028541044475,MeanDailyPriceVolatility=0.116402795345409,StdDevDailyPriceVolatility=0.0987531273340545
        //     }}, 
        //     {T5,new NormalVariables()}, 
        //     {T6,new NormalVariables()}, 
        //     {T7,new NormalVariables()}, 
        //     {T8,new NormalVariables()}, 
        //     {T9,new NormalVariables()}, 
        //     {T10,new NormalVariables()}, 
        //     {T11,new NormalVariables()}, 
        //     {T12,new NormalVariables()}, 
        //     {T13,new NormalVariables()}, 
        //     {T14,new NormalVariables()}, 
        //     {T15,new NormalVariables()} 
        //};

        public static void GetNormalvariables()
        {
            var dict = File.ReadLines(@"Normal_Variables.csv").Select(line => line.Split(',')).ToList();
            // dict = dict.Skip(1);
            var list = new List<NormalVariables>();
            foreach (var data in dict)
            {
                var nv = new NormalVariables();
                nv.Ticker = data[Convert.ToInt32(Configuration.EnumNormalVar.Symbol)];
                nv.MeanBidAskSpread = StreamData.ParseDouble(data[Convert.ToInt32(Configuration.EnumNormalVar.MeanBidAskSpread)]);
                nv.StdDevBidAskSpread = StreamData.ParseDouble(data[Convert.ToInt32(Configuration.EnumNormalVar.SdBidAskSpread)]);
                nv.MeanDailyPriceVolatility = StreamData.ParseDouble(data[Convert.ToInt32(Configuration.EnumNormalVar.MeanDailyPriceVolat)]);
                nv.StdDevDailyPriceVolatility = StreamData.ParseDouble(data[Convert.ToInt32(Configuration.EnumNormalVar.SdDailyPriceVola)]);
                nv.Exp50DMoovingAvg = StreamData.ParseDouble(data[Convert.ToInt32(Configuration.EnumNormalVar.AvgVolume)]);
                list.Add(nv);
            }

            foreach (var data in list)
            {
                if (!NormalVariables.ContainsKey(data.Ticker))
                {
                    NormalVariables.Add(data.Ticker, data);
                }
            }
        }

        public static double HighCoRelRatio = 1.2;
        public static double LowCoRelRatio = 0.8;

        public const string Liquidity = "Liquidity";
        public const string VolumeVolatility = "VolumeVolatility";
        public const string PriceVolatility = "PriceVolatility";
        public const string OrderImbalance = "OrderImbalance";
        public const string MarketActivity = "MarketActivity";
        public const string HighLowPriceRange = "HighLowPriceRange";

        public static Dictionary<string, double> TradingHours = new Dictionary<string, double> 
        { 
             {T1,14*60}, 
             {T2,10*60}, 
             {T3,21*60}, 
             {T4,21*60}, 
             {T5,19*60}, 
             {T6,18*60}, 
             {T7,24*60}, 
             {T8,20*60}, 
             {T9,24*60}, 
             {T10,6.5*60}, 
             {T11,6.5*60}, 
             {T12,8.5*60}, 
             {T13,23*60}, 
             {T14,5.5*60}, 
             {T15,6*60},
             {T16,6*60},
             {T17,6*60}
        };

        public const string TradeData = "TradeData";
        public const string MarketDepth = "MarketDepth";
        public const string AlertFileLocationKey = "LogFileLocation";
    }

    public class NormalVariables
    {
        public string Ticker { get; set; }
        public double BidAskSpread { get; set; }
        public double MeanBidAskSpread { get; set; }
        public double StdDevBidAskSpread { get; set; }
        public double DailyPriceVolatility { get; set; }
        public double MeanDailyPriceVolatility { get; set; }
        public double StdDevDailyPriceVolatility { get; set; }
        public double DailyVolume { get; set; }
        public double Exp50DMoovingAvg { get; set; }
        public double AvgTickCount { get; set; }
        public double MeanHighLowRange { get; set; }
        public double SdHighLowRange { get; set; }
    }

    public class TuningParameters
    {
        public double BidAskSprdSdMul { get; set; }
        public double BidAskSprdSdPct { get; set; }
        public double OrderImbalancePct { get; set; }
        public double PriceVolatalitySdMul { get; set; }
        public double MinVolumeFilter { get; set; }
        public double HighVolMul { get; set; }
        public double VeryHighVolMul { get; set; }
        public double ExtremelyHighVolMul { get; set; }
        public double TradingHours { get; set; }
        public double HighLowSdMul { get; set; }
        public double HighTickCountMul { get; set; }
    }

    public class Ticker
    {
        public int Id { get; set; }
        public string TickerCode { get; set; }
        public string AssetClassType { get; set; }
    }


    public class MarketDepth
    {
        public int Index { get; set; }
        public string Symbol { get; set; }
        public DateTime Time { get; set; }
        public double BidPrice1 { get; set; }
        public double AskPrice1 { get; set; }
        public double BidSize1 { get; set; }
        public double AskSize1 { get; set; }
        public DateTime BidUpDateTime1 { get; set; }
        public DateTime AskUpDateTime1 { get; set; }
        public double BidPrice2 { get; set; }
        public double AskPrice2 { get; set; }
        public double BidSize2 { get; set; }
        public double AskSize2 { get; set; }
        public DateTime BidUpDateTime2 { get; set; }
        public DateTime AskUpDateTime2 { get; set; }
        public double BidPrice3 { get; set; }
        public double AskPrice3 { get; set; }
        public double BidSize3 { get; set; }
        public double AskSize3 { get; set; }
        public DateTime BidUpDateTime3 { get; set; }
        public DateTime AskUpDateTime3 { get; set; }
        public string TimeAct { get; set; }
        public double BidAskSpread { get { return AskPrice1 - BidPrice1; } }
        public DateTime FileDate { get; set; }
        public bool IsBidOrderLow
        {
            get
            {
                if ((BidSize1 + BidSize2) < AskSize1) { return true; }
                return false;
            }

        }
        public bool IsAskOrderLow
        {
            get
            {
                if ((AskSize1 + AskSize2) < BidSize1) { return true; }
                return false;
            }
        }
    }


    public class TradeData
    {
        public int Index { get; set; }
        public string Symbol { get; set; }
        public DateTime Date { get; set; }
        public DateTime Time { get; set; }
        public string Quality { get; set; }
        public double Price { get; set; }
        public DateTime UpdateDateTime { get; set; }
        public double Volume { get; set; }
        public double Interval { get; set; }
        public int Count { get; set; }
        public double AccumulatedVolume { get; set; }
    }


    public class CalculatedValues
    {
        public double MeanBas { get; set; }
        public double VarianceBas { get; set; }
        public double StdDevBAas { get; set; }
        public double AccumulatedVolume { get; set; }
        public double BondDailyPriceVolatality { get; set; }
        public double DailyPriceVolatility { get; set; }
        public bool IsExtremelyHighVolume { get; set; }
        public bool IsVeryHighVolume { get; set; }
        public bool IsHighVolume { get; set; }
    }


    public class TickerData
    {
        public MarketDepth MarketDepth { get; set; }
        public TradeData TradeData { get; set; }
    }

    public class Alert
    {
        public string ticker { get; set; }
        public string Message { get; set; }
        public string Indicator { get; set; }
        public DateTime Time { get; set; }
    }

    public class TickerInfo
    {
        public TickerInfo(string key = "")
        {
            Value = new CalculatedValues();
            MarketDepth = new List<MarketDepth>();
            TradeData = new List<TradeData>();
            Ticker = new Ticker()
                       {
                           TickerCode = key,
                           AssetClassType = Configuration.AvailableTickers[key.Trim()]
                       };
        }


        public Ticker Ticker { get; set; }
        public CalculatedValues Value { get; set; }
        public IList<MarketDepth> MarketDepth { get; set; }
        public IList<TradeData> TradeData { get; set; }
        public NormalVariables NormalVariable
        {
            get
            {
                return Configuration.NormalVariables[this.Ticker.TickerCode];
            }
        }

        public TuningParameters Param
        {
            get
            {
                return Configuration.tuningParameters[Configuration.AvailableTickers[this.Ticker.TickerCode]];
            }
        }

        public void Calc()
        {
            var statistics = new DescriptiveStatistics(MarketDepth.Select(x => x.BidAskSpread));
            Value.MeanBas = statistics.Mean;
            Value.VarianceBas = statistics.Variance;
            Value.StdDevBAas = statistics.StandardDeviation;
        }

        public void CheckForLiquidity(TickerInfo tickerInfo)
        {
            var currentTickerInfo = tickerInfo;
            if (tickerInfo.MarketDepth.Count > 0)
            {
                var no = currentTickerInfo.MarketDepth.Where(x => NumberOfOutliersForLiquidity(x, x.Symbol)).ToList();
                if (no.Count / currentTickerInfo.MarketDepth.Count >= this.Param.BidAskSprdSdPct)
                {
                    var lastTickTime = this.MarketDepth.LastOrDefault().Time;
                    var date = this.MarketDepth.LastOrDefault().FileDate;
                    var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);
                    var msg = string.Format("Higher Than Normal Bid Ask Spread observed for {0}. Time:", this.Ticker.TickerCode, dateTime);
                    //Console.WriteLine("Higher Than Normal Bid Ask Spread observed for {0}. Days's volume so far: {1}", this.Ticker.TickerCode, string.Empty);
                    //AddAlerts(Configuration.Liquidity);
                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.Liquidity).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.Liquidity, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.Liquidity, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.Liquidity, lastTickTime, msg);
                    }
                }
            }
        }

        public void CheckForOrderImbalance()
        {
            var currentTickerInfo = Program.PerTickRecords[this.Ticker.TickerCode];
            if (currentTickerInfo.MarketDepth.Count > 0)
            {
                var bidOrderLowCount = currentTickerInfo.MarketDepth.Where(x => x.IsBidOrderLow).ToList().Count;
                var askOrderLowCount = currentTickerInfo.MarketDepth.Where(x => x.IsAskOrderLow).ToList().Count;
                if (bidOrderLowCount > 5 * askOrderLowCount)
                {
                    if ((bidOrderLowCount / currentTickerInfo.MarketDepth.Count) > this.Param.OrderImbalancePct)
                    {
                        var lastTickTime = this.MarketDepth.LastOrDefault().Time;
                        var date = this.MarketDepth.LastOrDefault().FileDate;
                        var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);
                        //Console.WriteLine("*************************Low Order size observed on the bid side compared to ask side for Ticker {0}. Days's volume so far: {1}***************************", this.Ticker.TickerCode, string.Empty);
                        //AddAlerts(Configuration.OrderImbalance);
                        var msg = string.Format("*************************Low Order size observed on the bid side compared to ask side for Ticker {0}. Time: {1}***************************", this.Ticker.TickerCode, dateTime);
                        if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                        {
                            var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.OrderImbalance).ToList();
                            if (existingAlert != null && existingAlert.Count > 0)
                            {
                                if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                                {
                                    AddAlerts(Configuration.OrderImbalance, lastTickTime, msg);
                                }
                            }
                            else
                            {
                                AddAlerts(Configuration.OrderImbalance, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.OrderImbalance, lastTickTime, msg);
                        }
                    }
                }
            }
        }

        public void CheckForHighLowPriceRange()
        {
            if (this.TradeData.Count > 0)
            {

                var lastTickTime = this.TradeData.LastOrDefault().Time;
                var highLowRange = this.TradeData.Max(x => x.Price) - this.TradeData.Min(x => x.Price);
                var date = this.TradeData.LastOrDefault().Date;
                var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);
                if ((highLowRange - this.NormalVariable.MeanHighLowRange) > (this.Param.HighLowSdMul * this.NormalVariable.SdHighLowRange))
                {
                    var msg = string.Format("**********************Higher than normal price move has been observed today for   {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.HighLowPriceRange).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.HighLowPriceRange, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.HighLowPriceRange, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.HighLowPriceRange, lastTickTime, msg);
                    }
                }
            }
        }

        public void CheckForBondHighLowPriceRange()
        {
            if (this.MarketDepth.Count > 0)
            {
                var lastTickTime = this.MarketDepth.LastOrDefault().Time;
                var highLowRange = this.MarketDepth.Max(x => x.BidPrice1) - this.MarketDepth.Min(x => x.BidPrice1);
                if ((highLowRange - this.NormalVariable.MeanHighLowRange) > (this.Param.HighLowSdMul * this.NormalVariable.SdHighLowRange))
                {
                    var msg = string.Format("**********************Higher than normal price move has been observed today for   {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, lastTickTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.HighLowPriceRange).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.HighLowPriceRange, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.HighLowPriceRange, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.HighLowPriceRange, lastTickTime, msg);
                    }
                }
            }
        }

        public void CheckForMarketActivity()
        {
            if (this.MarketDepth.Count > 0)
            {
                var tickCount = this.MarketDepth.Count;
                var firstTickTime = this.MarketDepth.FirstOrDefault().Time;
                var lastTickTime = this.MarketDepth.LastOrDefault().Time;
                var tradedTimeInMinutes = (lastTickTime - firstTickTime).TotalMinutes;
                var tradingHours = Configuration.TradingHours[this.Ticker.TickerCode];
                var date = this.MarketDepth.LastOrDefault().FileDate;
                var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);
                if ((tickCount / this.NormalVariable.AvgTickCount) / (tradedTimeInMinutes / tradingHours) >= this.Param.HighTickCountMul)
                {
                    var msg = string.Format("**********************High than usual Market Activity has been observed today for  {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.MarketActivity).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.MarketActivity, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.MarketActivity, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.MarketActivity, lastTickTime, msg);
                    }
                }
            }
        }

        public void CheckBondPriceVolatality()
        {
            if (this.MarketDepth.Count > 0)
            {
                var statistics = new DescriptiveStatistics(this.MarketDepth.Select(x => x.BidPrice1));
                Value.BondDailyPriceVolatality = statistics.StandardDeviation;
                var lastTickTime = this.MarketDepth.LastOrDefault().Time;
                var date = this.MarketDepth.LastOrDefault().FileDate;
                var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);
                if ((this.Value.BondDailyPriceVolatality - this.NormalVariable.MeanDailyPriceVolatility) > this.Param.PriceVolatalitySdMul * this.NormalVariable.StdDevDailyPriceVolatility)
                {
                    //Console.WriteLine("**********************Higher Than Normal Price Volatility has been observed {0}. Days's volume so far: {1} **********************************", this.Ticker.TickerCode);
                    //AddAlerts(Configuration.PriceVolatility);
                    var msg = string.Format("**********************Higher Than Normal Price Volatility has been observed {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.PriceVolatility).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.PriceVolatility, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.PriceVolatility, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.PriceVolatility, lastTickTime, msg);
                    }
                }
            }
        }

        public void CheckForPriceVolatility()
        {
            if (this.TradeData.Count > 0)
            {
                var statistics = new DescriptiveStatistics(this.TradeData.Select(x => x.Price));
                Value.DailyPriceVolatility = statistics.StandardDeviation;
                var lastTickTime = this.TradeData.LastOrDefault().Time;
                if ((this.Value.DailyPriceVolatility - this.NormalVariable.MeanDailyPriceVolatility) > this.Param.PriceVolatalitySdMul * this.NormalVariable.StdDevDailyPriceVolatility)
                {
                    var date = this.TradeData.LastOrDefault().Date;
                    var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);

                    var msg = string.Format("**********************Higher Than Normal Price Volatility has been observed {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);
                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.PriceVolatility).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.PriceVolatility, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.PriceVolatility, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.PriceVolatility, lastTickTime, msg);
                    }

                    // AddAlerts(Configuration.PriceVolatility);
                }
            }
        }

        private string GetDateTimeLastTicker(DateTime lastTickTime, DateTime date, string assetClass)
        {

            if (assetClass == Configuration.Equity)
            {
                return string.Format("Date : {0}, Time : {1}", date.Date, lastTickTime.TimeOfDay, Configuration.AvailableTickers[this.Ticker.TickerCode]);
            }
            else
            {
                return string.Format("Date : {0}, Time : {1}", lastTickTime.Date, lastTickTime.TimeOfDay, Configuration.AvailableTickers[this.Ticker.TickerCode]);
            }
        }

        private void AddAlerts(string indicator, DateTime time, string msg)
        {
            if(!Configuration.RedundantTickers.Contains(this.Ticker.TickerCode)){

            if (!Program.Alerts.ContainsKey(this.Ticker.TickerCode))
            {
                Program.Alerts.Add(this.Ticker.TickerCode, new List<Alert> { new Alert { Time = time, Indicator = indicator, Message = msg } });
            }
            else
            {
                Program.Alerts[this.Ticker.TickerCode].Add(new Alert { Time = time, Indicator = indicator, Message = msg });
            }
            Console.WriteLine(msg);
            WriteTextFile.Write(msg, "LogFileLocation");
            }
        }

        public void CheckForVolume()
        {
            if (this.TradeData.Count > 0)
            {
                Value.AccumulatedVolume = this.TradeData.Sum(x => x.Volume);
                var firstTickTime = this.TradeData.FirstOrDefault().Time;
                var lastTickTime = this.TradeData.LastOrDefault().Time;
                var tradedTimeInMinutes = (lastTickTime - firstTickTime).TotalMinutes;
                var tradingHours = Configuration.TradingHours[this.Ticker.TickerCode];
                var date = this.TradeData.LastOrDefault().Date;
                var dateTime = GetDateTimeLastTicker(lastTickTime, date, Configuration.AvailableTickers[this.Ticker.TickerCode]);

                if (((Value.AccumulatedVolume / this.NormalVariable.Exp50DMoovingAvg) / (tradedTimeInMinutes / tradingHours)) >= this.Param.ExtremelyHighVolMul)
                {
                    var msg = string.Format("*************************************Extremely High Trading Volume has been observed today for Ticker {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.VolumeVolatility).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            var lastAlert = existingAlert.LastOrDefault();
                            if ((lastTickTime - lastAlert.Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                    }

                    Value.IsExtremelyHighVolume = true;
                    //Console.WriteLine("*************************************Extremely High Trading Volume has been observed today for Ticker {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, lastTickTime);
                    //WriteTextFile.Write(, "LogFileLocation");
                    //AddAlerts(Configuration.VolumeVolatility);
                }
                else if ((Value.AccumulatedVolume / this.NormalVariable.Exp50DMoovingAvg) / (tradedTimeInMinutes / tradingHours) >= this.Param.VeryHighVolMul)
                {
                    Value.IsVeryHighVolume = true;
                    var msg = string.Format("*************************************Extremely High Trading Volume has been observed today for Ticker {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.VolumeVolatility).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                    }
                    //Console.WriteLine("*************************************Very High Trading Volume has been observed today for Ticker {0} :Last tick time: {1}**************************", this.Ticker.TickerCode, lastTickTime);
                    //AddAlerts(Configuration.VolumeVolatility);
                }
                else if ((Value.AccumulatedVolume / this.NormalVariable.Exp50DMoovingAvg) / (tradedTimeInMinutes / tradingHours) >= this.Param.HighVolMul)
                {
                    Value.IsHighVolume = true;
                    // Console.WriteLine("************************************* High Trading Volume has been observed today for Ticker {0}:Last tick time: {1} **************************", this.Ticker.TickerCode, lastTickTime);
                    //AddAlerts(Configuration.VolumeVolatility);
                    var msg = string.Format("*************************************Extremely High Trading Volume has been observed today for Ticker {0} : Last tick time: {1} **************************", this.Ticker.TickerCode, dateTime);

                    if (Program.Alerts.ContainsKey(this.Ticker.TickerCode))
                    {
                        var existingAlert = Program.Alerts[this.Ticker.TickerCode].Where(x => x.Indicator == Configuration.VolumeVolatility).ToList();
                        if (existingAlert != null && existingAlert.Count > 0)
                        {
                            if ((lastTickTime - existingAlert.LastOrDefault().Time).TotalMinutes >= 60)
                            {
                                AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                            }
                        }
                        else
                        {
                            AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                        }
                    }
                    else
                    {
                        AddAlerts(Configuration.VolumeVolatility, lastTickTime, msg);
                    }
                }
            }
        }

        private bool NumberOfOutliersForLiquidity(MarketDepth marketDepth, string tickerCode)
        {
            if ((marketDepth.BidAskSpread - this.NormalVariable.MeanBidAskSpread) > (this.Param.BidAskSprdSdMul * this.NormalVariable.StdDevBidAskSpread))
            {
                return true;
            }

            return false;
        }
    }

    public class TimeSliceBucketLookUpAlgo
    {

        private static int END_MM = 5;
        private static int END_HH = 4;
        private static int START_MM = 3;
        private static int START_HH = 2;
        private static int BUCKET_START_VALUE = 0;
        private static int BUCKET_END_VALUE = 1;
        /**
         * @param startTime if start time = 0 then from 12.00 A.M will be default start time
         * @param timeSlice
         */

        private int startTime;
        private int timeSlice;
        private int noOfBuckets;
        private static int END_TIME = 1440;
        private int[,] timeBucket;

        public TimeSliceBucketLookUpAlgo(int startTime, int timeSlice)
        {
            this.startTime = startTime;
            this.timeSlice = timeSlice;
            noOfBuckets = END_TIME / timeSlice;
            Console.WriteLine(noOfBuckets);
            timeBucket = new int[noOfBuckets, 6];
            initBuckets();
        }

        private void initBuckets()
        {
            timeBucket[0, BUCKET_START_VALUE] = startTime;
            timeBucket[0, BUCKET_END_VALUE] = timeSlice;
            timeBucket[0, START_HH] = startTime / 60;
            timeBucket[0, START_MM] = startTime % 60;
            timeBucket[0, END_HH] = timeSlice / 60;
            timeBucket[0, END_MM] = timeSlice % 60;

            int i = 1;
            int start = timeSlice;

            while (start < END_TIME)
            {
                int temp = start + timeSlice;
                timeBucket[i, BUCKET_START_VALUE] = start;
                timeBucket[i, BUCKET_END_VALUE] = temp;
                timeBucket[i, START_HH] = start / 60;
                timeBucket[i, START_MM] = start % 60;
                timeBucket[i, END_HH] = temp / 60;
                timeBucket[i, END_MM] = temp % 60;

                start = temp;
                i++;
            }

        }

        public int[,] getTimeBucket()
        {
            return timeBucket;
        }

        public int getBucketSize()
        {
            return noOfBuckets;
        }
    }

    public static class WriteTextFile
    {
        public static void Write(string lines, string logKey)
        {
            // Write the string to a file.
            //System.IO.StreamWriter file = new System.IO.StreamWriter(StreamData.ReadAppSetting("LogFileLocation"));
            //file.Appe(lines);
            //file.Close();            
            var path = StreamData.ReadAppSetting(logKey);
            if (!File.Exists(path))
            {
                // Create a file to write to.
                using (StreamWriter sw = File.CreateText(path))
                {
                    sw.WriteLine(lines);
                }
            }
            else
            {
                // This text is always added, making the file longer over time
                // if it is not deleted.
                using (StreamWriter sw = File.AppendText(path))
                {
                    sw.WriteLine(lines);
                }
            }
        }
    }
}
