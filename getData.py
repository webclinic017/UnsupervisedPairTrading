from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockQuotesRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus


from eod import EodHistoricalData
from datetime import datetime
from dateutil.relativedelta import relativedelta

import statsmodels.api as sm
import numpy as np
import pandas as pd
import json
import os

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



tradingClient = TradingClient(api_key="PK8ESWJ3MELVNJRAEFZS",secret_key="mwfKCEJn6AKdQGM3Kpo9y50sntZQJDsHjgov0zSG",paper=True)
dataClient = StockHistoricalDataClient(api_key="PK8ESWJ3MELVNJRAEFZS",secret_key="mwfKCEJn6AKdQGM3Kpo9y50sntZQJDsHjgov0zSG")
fundamentalsClient = EodHistoricalData("638cc8a7414471.39408510")

assets = tradingClient.get_all_assets()
validAssets = [asset.symbol for asset in assets if (asset.fractionable==True and \
                                            asset.shortable==True and \
                                            asset.easy_to_borrow==True and \
                                            asset.status==AssetStatus.ACTIVE and \
                                            asset.exchange in (AssetExchange.NYSE, AssetExchange.AMEX, AssetExchange.NASDAQ))]

result = None

print(f"{len(validAssets)} potential assets")


def parseData(arr: list, feature:str, initial:int=0, duration:int=4) -> int:
    i = 0
    while i < initial+duration:
        res = arr[i][feature]
        if res:
            break
        i += 1
    return float(res) if res else 0

def sumData(arr: list, feature, initial=0, duration=4) -> int:
    res = 0
    for i in range(initial, initial+duration):
        
        val = arr[i][feature]
        res = res + float(val) if val else res
                      
    return res
        

retrievedAssets = [".".join(a.split(".")[:-1]) for a in os.listdir("fundamentals")]

for asset in retrievedAssets:
    print(asset)
    barSet = dataClient.get_stock_bars(StockBarsRequest(symbol_or_symbols=asset, 
                                        timeframe=TimeFrame.Month, 
                                        adjustment=Adjustment.ALL,
                                        limit=49, 
                                        feed=DataFeed.SIP, 
                                            start=datetime.today() - relativedelta(months=49)))
    
    if len(barSet.df) < 49:
        continue
    
    # get technical features
    priceMomentums = barSet.df["close"].pct_change().dropna()
    priceMomentums = priceMomentums.reset_index(drop=True).add_prefix("month_")
    priceMomentums = priceMomentums.rename(asset)
    
    try: 
        # get fundamental JSON data
        with open(f"fundamentals/{asset}.json") as file:
            fundamentalsData = json.load(file)
        
        # set up container for fundamental features
        firmCharacteristics = {}
        
        if "Highlights" not in fundamentalsData.keys() or "Technicals" not in fundamentalsData.keys():
            continue
        
        # beta
        beta = fundamentalsData["Technicals"]["Beta"]
        # if beta doesn't exist, we don't evaluate this stock
        if not beta:
            continue
        
        if not fundamentalsData["Highlights"]["EarningsShare"]:
            continue
        
        if len(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()) < 8 or \
            len(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()) < 8 or \
            len(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()) < 8:
                continue
              
        betasq = beta * beta
        
        firmCharacteristics["beta"] = beta
        firmCharacteristics["betasq"] = betasq
        
        # bm -- book-to-market
        bookValue = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets") - \
                    parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalLiab")
        bm = bookValue / fundamentalsData["Highlights"]["MarketCapitalization"]

        firmCharacteristics["bm"] = bm
        
        #cash -- (cash and cash equivalents) / average total assets
        cashEquivalents = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "cash")
        avgTotalAssets = sumData(
            list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), 
            "cash") / 4

        cash = cashEquivalents / avgTotalAssets if avgTotalAssets else 0

        firmCharacteristics["cash"] = cash
        
        # cashdebt -- Earnings before depreciation and extraordinary items (ib+dp) divided by avg. total liabilities
        earnings = fundamentalsData["SharesStats"]["SharesOutstanding"] * fundamentalsData["Highlights"]["EarningsShare"]

        cashdebt = earnings / avgTotalAssets

        firmCharacteristics["cashdebt"] = cashdebt
        
        # cashpr -- Fiscal year end market capitalization plus long term debt (dltt) minus total assets (at) 
        # divided by cash and equivalents
        capDebtAsset = fundamentalsData["Highlights"]["MarketCapitalization"] + \
                parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "longTermDebt") - \
                parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets")

        cashpr = capDebtAsset / cashEquivalents if cashEquivalents else 0

        firmCharacteristics["cashpr"] = cashpr
        
        # cfp -- Operating cash flows divided by fiscal-year-end market capitalization
        operatingCashFlow = parseData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "totalCashFromOperatingActivities")
        yodMarketCapitalization = float(fundamentalsData["Highlights"]["MarketCapitalization"])

        cfp = operatingCashFlow / yodMarketCapitalization

        firmCharacteristics["cfp"] = cfp
        
        # chcsho -- Annual percent change in shares outstanding (csho).
        currYearSharesOutstanding = parseData(
            list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), 
            "commonStockSharesOutstanding")
        prevYearSharesOutstanding = parseData(
            list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()),
            "commonStockSharesOutstanding",
            initial=4) 

        chcsho = (currYearSharesOutstanding - prevYearSharesOutstanding) / prevYearSharesOutstanding if prevYearSharesOutstanding else 0

        firmCharacteristics["chcsho"] = chcsho
        
        # chinv -- change in inventory scaled by average total assets
        changeInInventory = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory") - \
                            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory", 4)

        chinv = changeInInventory / avgTotalAssets if avgTotalAssets else 0

        firmCharacteristics["chinv"] = chinv
        
        # agr -- annual percent change in total assets
        currentYearAssets = parseData(
            list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), 
            "totalAssets") 
        previousYearAssets = parseData(
            list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()),
            "totalAssets",
            initial=4) 

        agr = (currentYearAssets - previousYearAssets) / previousYearAssets if previousYearAssets else 0

        firmCharacteristics["agr"] = agr
        
        # currat -- Current assets / current liabilities
        currentAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets")
        currentLiability = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities")

        currat = currentAsset / currentLiability if currentLiability else 0

        firmCharacteristics["currat"] = currat
        
        # pchcurrat -- Percent change in currat
        currentAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets")
        prevAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets", 4)

        currentLiab = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities")
        prevLiab = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities", 4)

        pchcurrat = ((currentAsset/currentLiab) - (prevAsset/prevLiab)) / (prevAsset/prevLiab)

        firmCharacteristics["pchcurrat"] = pchcurrat
        
        # roaq -- Income before extraordinary items (ibq) divided by one quarter lagged total assets (atq).
        roaq = fundamentalsData["Highlights"]["ReturnOnAssetsTTM"]

        firmCharacteristics["roaq"] = roaq
        
        # roeq -- Earnings before extraordinary items divided by lagged common shareholders' equity.
        roeq = fundamentalsData["Highlights"]["ReturnOnEquityTTM"]

        firmCharacteristics["roeq"] = roeq
        
        # dy -- Total dividends (dvt) divided by market capitalization at fiscal year-end
        dividendsPaid = sumData(
            list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), 
            "dividendsPaid")

        dy = dividendsPaid / yodMarketCapitalization

        firmCharacteristics["dy"]= dy
        
        # rd -- An indicator variable equal to 1 if R&D expense as a percentage of total assets has an increase greater than 5%.
        currRD = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "researchDevelopment") / \
                parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets")

        prevRD = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "researchDevelopment", 4) / \
                parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 4)

        rd = 1 if prevRD==0 or (currRD - prevRD) / prevRD > 0.05 else 0

        firmCharacteristics["rd"] = rd
        
        # chtx -- Percent change in total taxes (txtq) from quarter t-4 to t.
        currTaxExpense = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "taxProvision")
        prevTaxExpense = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "taxProvision", 3)

        chtx = (currTaxExpense - prevTaxExpense) / prevTaxExpense if prevTaxExpense else 0

        firmCharacteristics["chtx"] = chtx
        
        # ep -- Annual income before extraordinary items (ib) divided by end of fiscal year market cap.
        income = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "incomeBeforeTax")

        ep = income / yodMarketCapitalization

        firmCharacteristics["ep"] = ep
        
        # gma -- Revenues (revt) minus cost of goods sold (cogs) divided by lagged total assets (at).
        gma = fundamentalsData["Highlights"]["GrossProfitTTM"] / \
            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 3)

        firmCharacteristics["gma"] = gma
        
        # lev -- Total liabilities (lt) divided by fiscal year end market capitalization.
        currentLiability = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalLiab")

        lev = currentLiability / yodMarketCapitalization

        firmCharacteristics["lev"] = lev
        
        # invest -- Annual change in gross property, plant, and equipment (ppegt) + 
        # annual change in inventories (invt) all scaled by lagged total assets (at).

        ppegChange = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantEquipment") - \
                    parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantEquipment", 3)

        inventoryChange = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory") - \
                    parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory", 3)

        laggedTotalAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 3)

        invest = (ppegChange + inventoryChange) / laggedTotalAsset if laggedTotalAsset else 0

        firmCharacteristics["invest"] = invest
        
        # quick -- (current assets - inventory) / current liabilities.
        currentAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets")
        currentLiability = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities")
        inventory = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory")

        quick = (currentAsset - inventory) / currentLiability if currentLiability else 0

        firmCharacteristics["quick"] = quick
        
        # pchquick -- Percent change in quick
        
        currentAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets")
        currentLiability = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities")
        currInventory = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory")
        
        prevAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets", 1)
        prevLiability = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities", 1)
        prevInventory = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory", 1)

        pchquick = ((currentAsset-currInventory)/currentLiability - (prevAsset-prevInventory)/prevLiability) / ((prevAsset-prevInventory)/prevLiability)

        firmCharacteristics["pchquick"] = pchquick
        
        # dolvol -- Natural log of trading volume times price per share from month t-2.
        dolvol = np.log(barSet.data[asset][-2].volume * barSet.data[asset][-2].close)

        firmCharacteristics["dolvol"] = dolvol
        
        # egr -- Annual percent change in book value of equity (ceq).
        currBookValue = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets") - \
                        parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalLiab")
        prevBookValue = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 3) - \
                        parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalLiab", 3)

        egr = (currBookValue - prevBookValue) / prevBookValue if prevBookValue else 0

        firmCharacteristics["egr"] = egr
        
        # lgr -- Annual percent change in total liabilities (lt).
        currLiab = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalLiab")
        prevLiab = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalLiab", 4)

        lgr = (currLiab - prevLiab) / prevLiab 

        firmCharacteristics["lgr"] = lgr
        
        # ps -- Sum of 8 indicator variables for fundamental performance
        # https://www.investopedia.com/terms/p/piotroski-score.asp

        # profitability criteria
        netIncomeInd = 1 if parseData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "netIncome") > 0 else 0
        roaInd = 1 if fundamentalsData["Highlights"]["ReturnOnAssetsTTM"] > 0 else 0

        operatingCashFlow = sumData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "totalCashFromOperatingActivities")
        netIncome = sumData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "netIncome")

        operatingCashFlowInd = 1 if operatingCashFlow > 0 else 0
        netIncomeInd = 1 if operatingCashFlow > netIncome else 0

        # Leverage, Liquidity, and Source of Funds Criteria
        currLongTermDebt = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "longTermDebt")
        prevLongTermDebt = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "longTermDebt", 4)
        longTermDebtInd = 1 if currLongTermDebt < prevLongTermDebt else 0

        currCurrentRatio = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets") / \
                            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities")
        prevCurrentRatio = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentAssets", 4) / \
                            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalCurrentLiabilities", 4)
                            
        currRatioInd = 1 if currCurrentRatio > prevCurrentRatio else 0

        # Operating Efficiency Criteria
        currYearGrossProfit = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "grossProfit")
        prevYearGrossProfit = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "grossProfit", 4)
        grossMarginInd = 1 if currYearGrossProfit > prevYearGrossProfit else 0

        currAssetTurnOver = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue") / \
                            ((parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets") + \
                            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 3))/2)
        
        prevAssetTurnOver = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", 4) / \
                            ((parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 4) + \
                            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets", 7))/2)
                            
        assetTurnOverInd = 1 if currAssetTurnOver > prevAssetTurnOver else 0

        ps = netIncomeInd + roaInd + operatingCashFlowInd + netIncomeInd + longTermDebtInd + currRatioInd + grossMarginInd + assetTurnOverInd

        firmCharacteristics["ps"] = ps
            
        # maxret -- Maximum daily return from returns during calendar month t-1.
        dailyBarSet = dataClient.get_stock_bars(StockBarsRequest(symbol_or_symbols=asset, 
                                                timeframe=TimeFrame.Day, 
                                                adjustment=Adjustment.ALL,
                                                limit=30, 
                                                feed=DataFeed.SIP, 
                                                    start=datetime.today() - relativedelta(days=30)))

        dailyReturn = (dailyBarSet.df["close"] - dailyBarSet.df["open"]) / dailyBarSet.df["open"]
        maxret = dailyReturn.max()

        firmCharacteristics["maxret"] = maxret
        
        # roic -- Annual earnings before interest and taxes (ebit) minus non-operating income (nopi) 
        # divided by non-cash enterprise value (ceq+lt-che).
        # enterprise value = market cap + total debt - cash and cash equivalents

        ebit = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "ebit")
        nonOperatingIncome = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "nonOperatingIncomeNetOther")

        ev = fundamentalsData["Highlights"]["MarketCapitalization"] + \
            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "shortTermDebt") + \
            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "longTermDebt") - \
            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "cash")

        roic = (ebit - nonOperatingIncome) / ev 

        firmCharacteristics["roic"] = roic
        
        # depr -- Depreciation divided by PP&E.
        depreciation = parseData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "depreciation")
        ppeg = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantEquipment")

        depr = depreciation / ppeg if ppeg else 0

        firmCharacteristics["depr"] = depr
        
        # pchdepr -- Percent change in depr
        currDepreciation = parseData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "depreciation")
        currPpeg = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantEquipment")

        prevDepreciation = parseData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "depreciation", 4)
        prevPpeg = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantEquipment", 4)

        pchdepr = ((currDepreciation/currPpeg) - (prevDepreciation/prevPpeg)) / (prevDepreciation/prevPpeg) if (prevDepreciation/prevPpeg) else 0

        firmCharacteristics["pchdepr"] = pchdepr
        
        # sgr -- Annual percent change in sales (sale).
        currYearRevenue = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        prevYearRevenue = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", 4)
        
        sgr = (currYearRevenue - prevYearRevenue) / prevYearRevenue if prevYearRevenue else 0

        firmCharacteristics["sgr"] = sgr
        
        # SP -- Annual revenue (sale) divided by fiscal-year-end market capitalization.
        currYearRevenue = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")

        SP = currYearRevenue / yodMarketCapitalization

        firmCharacteristics["SP"] = SP
        
        # divi -- An indicator variable equal to 1 if company pays dividends but did not in prior year
        currDividends = sumData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "dividendsPaid")
        prevDividends = sumData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "dividendsPaid", 4)
        
        divi = 1 if (currDividends > 0 and prevDividends < 0) else 0

        firmCharacteristics["divi"] = divi
        
        # divo -- An indicator variable equal to 1 if company does not pay dividend but did in prior year.
        divo = 1 if (currDividends < 0 and prevDividends > 0) else 0

        firmCharacteristics["divo"] = divo
        
        # turn -- Average monthly trading volume for most recent 3 months scaled by number of shares outstanding in current month
        avgTradeVol = sum([d.volume for d in barSet.data[asset][-3:]]) / 3
        sharesOutstanding = fundamentalsData["SharesStats"]["SharesOutstanding"]

        turn = avgTradeVol / sharesOutstanding

        firmCharacteristics["turn"] = turn
        
        # mve -- Natural log of market capitalization at end of month t-1.
        mve = np.log(fundamentalsData["Highlights"]["MarketCapitalization"])

        firmCharacteristics["mve"] = mve
        
        # salecash -- Annual sales divided by cash and cash equivalents
        currYearRevenue = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        cashEquivalents = sumData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "cash")

        salecash = currYearRevenue / cashEquivalents if cashEquivalents else 0

        firmCharacteristics["salecash"] = salecash
        
        # salerec -- Annual sales divided by accounts receivable.
        currYearRevenue = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        currYearRec = sumData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "netReceivables")

        salerec = currYearRevenue / currYearRec if currYearRec else 0

        firmCharacteristics["salerec"] = salerec
        
        # sin -- An indicator variable equal to 1 if a company's primary industry classification is in 
        # smoke or tobacco, beer or alcohol, or gaming.
        sin = 1 if fundamentalsData["General"]["GicSubIndustry"] in ("Tobacco", "Brewers", "Casinos & Gaming") else 0

        firmCharacteristics["sin"] = sin
        
        # retvol -- Standard deviation of daily returns from month t-1.
        retvol = dailyReturn.std()

        firmCharacteristics["retvol"] = retvol
        
        # chmom -- Cumulative returns from months t-6 to t-1 minus months t-12 to t-7.
        currMomentum = (barSet[asset][-1].close - barSet[asset][-6].close) / barSet[asset][-6].close
        prevMomentum = (barSet[asset][-7].close - barSet[asset][-12].close) / barSet[asset][-12].close

        chmom = currMomentum - prevMomentum

        firmCharacteristics["chmom"] = chmom
        
        # tb -- Tax income, calculated from current tax expense divided by maximum federal tax rate, 
        # divided by income before extraordinary items
        taxProvision = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "taxProvision")
        income = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "incomeBeforeTax")

        fedRate = 0.37

        tb = (taxProvision / fedRate) / income if income else 0

        firmCharacteristics["tb"] = tb
        
        # operprof -- Revenue minus cost of goods sold - SG&A expense - interest expense divided by lagged common shareholders' equity
        currRevenue = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        costOfRevenue = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "costOfRevenue")
        sga = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "sellingGeneralAdministrative")
        interestExpense = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "interestExpense")
        
        equity = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "commonStock")

        operprof = (currRevenue - costOfRevenue - sga - interestExpense) / equity if equity else 0

        firmCharacteristics["operprof"] = operprof
        
        # pchgm_pchsale -- Percent change in gross margin (sale-cogs) minus percent change in sales (sale).
        currGrossMargin = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "grossProfit")
        prevGrossMargin = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "grossProfit", 4)

        currSales = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        prevSales = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", 4)

        pchgm_pchsale = (currGrossMargin-prevGrossMargin)/prevGrossMargin - (currSales-prevSales)/prevSales

        firmCharacteristics["pchgm_pchsale"] = pchgm_pchsale
        
        # cinvest -- Change over one quarter in net PP&E (ppentq) divided by sales (saleq) - average of this variable 
        # for prior 3 quarters; if saleq = 0, then scale by 0.01.
        currInvest = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantAndEquipmentNet") / \
                    parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue") - \
                    parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantAndEquipmentNet", 4) / \
                    parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", 4)
                    
        avgInvest = sum([parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantAndEquipmentNet", i) / \
                    parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", i) - \
                    parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantAndEquipmentNet", i+4) / \
                    parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", i+4) for i in range(1, 4)])/3


        cinvest = currInvest - avgInvest

        firmCharacteristics["cinvest"] = cinvest
        
        # acc -- Annual income before extraordinary items (ib) minus operating cash flows (oancf) divided by average total assets (at); 
        # if oancf is missing then set to change in act - change in che - change in lct + change in dlc + change in txp-dp.

        income = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "incomeBeforeTax")
        cashFlow = parseData(list(fundamentalsData["Financials"]["Cash_Flow"]["quarterly"].values()), "totalCashFromOperatingActivities")
        avgAssets = sumData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets") / 4

        acc = (income - cashFlow) / avgAssets if avgAssets else 0

        firmCharacteristics["acc"] = acc
        
        # absacc -- Absolute value of acc
        absacc = abs(acc)

        firmCharacteristics["absacc"] = absacc
        
        # std_turn -- Monthly standard deviation of daily share turnover
        shareOutstanding = fundamentalsData["SharesStats"]["SharesOutstanding"] if fundamentalsData["SharesStats"]["SharesOutstanding"] else \
            parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "commonStockSharesOutstanding")
        shareTurnovers = [d.volume/shareOutstanding for d in dailyBarSet[asset]]

        std_turn = np.array(shareTurnovers).std()

        firmCharacteristics["std_turn"] = std_turn
        
        # tang -- Cash holdings + 0.715 × receivables + 0.547 × inventory + 0.535 × PPE/ total assets
        cash = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "cash")
        receivables = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "netReceivables")
        inventory = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "inventory")
        ppeg = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "propertyPlantAndEquipmentNet")
        totalAsset = parseData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "totalAssets")

        tang = (cash + 0.715*receivables + 0.547*inventory + 0.535*ppeg) / totalAsset if totalAsset else 0

        firmCharacteristics["tang"] = tang
        
        # std_dolvol -- Monthly standard deviation of daily dollar trading volume
        dollarVolumes = [b.volume*b.vwap for b in dailyBarSet.data[asset]]

        std_dolvol = np.array(dollarVolumes).std()

        firmCharacteristics["std_dolvol"] = std_dolvol
        
        # idiovol -- Standard deviation of residuals of weekly returns on weekly equal weighted market returns for 3 years 
        # prior to month end
        weeklyBarSet = dataClient.get_stock_bars(StockBarsRequest(symbol_or_symbols=asset, 
                                                timeframe=TimeFrame.Week, 
                                                adjustment=Adjustment.ALL, 
                                                feed=DataFeed.SIP, 
                                                    start=datetime.today() - relativedelta(years=3),
                                                    end=datetime.today())).df
        weeklyBarSet.insert(0, "x", range(1, 1+len(weeklyBarSet)))

        y = weeklyBarSet["close"]
        x = weeklyBarSet["x"]
        x = sm.add_constant(x)

        model = sm.OLS(y, x).fit()

        idiovol = model.resid.std()

        firmCharacteristics["idiovol"] = idiovol
        
        # ill -- Average of daily (absolute return / dollar volume)
        ill = ((dailyBarSet.df["close"]-dailyBarSet.df["open"]) / dailyBarSet.df["vwap"]).mean()

        firmCharacteristics["ill"] = ill
        
        # rsup -- Sales from quarter t minus sales from quarter t-4 (saleq) divided by fiscal-quarter-end market capitalization 
        # (cshoq * prccq).
        currSales = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        prevSales = parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", 3)

        marketCap = fundamentalsData["Highlights"]["MarketCapitalization"]

        rsup = (currSales - prevSales) / marketCap if (marketCap and prevSales) else 0

        firmCharacteristics["rsup"] = rsup
        
        # pchsale_pchrect -- Annual percent change in sales (sale) minus annual percent change in receivables (rect)
        currAnnualSales = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue")
        prevAnnualSales = sumData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "totalRevenue", 4)

        currAnnualRec = sumData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "netReceivables")
        prevAnnualRec = sumData(list(fundamentalsData["Financials"]["Balance_Sheet"]["quarterly"].values()), "netReceivables", 4)

        pchsale_pchrect = ((currAnnualSales-prevAnnualSales)/prevAnnualSales) - ((currAnnualRec-prevAnnualRec)/prevAnnualRec) if \
            prevAnnualSales and prevAnnualRec else 0
        
        # nincr -- Number of consecutive quarters (up to eight quarters) with an increase in earnings (ibq) 
        # over same quarter in the prior year.
        earningsList = [parseData(list(fundamentalsData["Financials"]["Income_Statement"]["quarterly"].values()), "netIncome", 4, 8) \
                    for i in range(4, 12)]
        earningsList.reverse()

        nincr = 1 
        currNincr = 1
        for i in range(len(earningsList)-1):
            if earningsList[i] < earningsList[i+1]:
                currNincr += 1
            else:
                currNincr = 1
            nincr = max(nincr, currNincr)
            
        firmCharacteristics["nincr"] = nincr

        # consolidate features
        firmCharSeries = pd.Series(firmCharacteristics)
        combinedFeatures = priceMomentums.append(firmCharSeries)
        combinedFeatures = combinedFeatures.rename(asset)
        featuresDF = pd.DataFrame([combinedFeatures])
        
        result = featuresDF if result is None else pd.concat([result, featuresDF])
        
        print(f"data for {asset} completed -- ({len(result)} stocks)")
        
        if len(result) % 3 == 0:
            result.to_csv("raw_data.csv")
    except ZeroDivisionError as ex:
        print(ex)
        continue



result.to_csv("raw_data.csv")


