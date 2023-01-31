from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from lib.patterns import Base, Singleton

from alpaca.trading.models import Asset
from alpaca.trading.requests import GetAssetsRequest

from enum import Enum
from tqdm import tqdm

class ETF_TYPES(Enum):
    OPTIONS = "options"
    NON_OPTIONS = "non_options"

class ETFs(Base, metaclass=Singleton):
    unleveraged:dict[str, list] = {
        ETF_TYPES.OPTIONS: [
            "WEAT", 
        ],
        ETF_TYPES.NON_OPTIONS: [
            "FDRV", "PSJ", "ARKF", "PSI", "FTXL", "PAVE", "FXZ", "AOTG", "ISHP", "FILL", "METV", "DVLU", "SCHG", 
            "HLAL", "PYZ", "FCG", "QQD", "NIB", "DRIV", "PXI", "SATO", "XSW", "ARVR", "SMH", "QQMG", "QTR", "IGM", 
            "BRKY", "PLAT", "KBUY", "CUT", "XSD", "GK", "TCHP", "MGK", "SOXX", "INDS", "CANE", "FXN", "IDAT", "PWB", 
            "ONEQ", "FMET", "PXQ", "ILCG", "FPA", "VUG", "SPHB", "QQQE", "QQEW", "FLKR", "AMZA", "REMX", "LOUP", 
            "BICK", "VGT", "FTEC", "SNSR", "BITS", "RPG", "FINX", "XLK", "AIA", "IVES", "DJCB", "GENY", "INNO", "ZGEN",
            "QGRW", "FCLD", "CLIX", "IWY", "XLC", "JJA", "IXN", "FDNI", "FGRO", "GINN", "TSME", "CGGR", "FDG", "WINN", 
            "QSPT", "EDOC", "IETC", "FLTW", "HERD", "QQH", "FXL", "HERO", "SOXQ", "TMFC", "RYT", "CCSO", "DAM", "IHAK", 
            "FCOM", "VONG", "MCHI", "RATE", "RXI", "FV", "NULG", "MOTI", "QMAR", "BUZZ", "IWF", "SVIX", "VERS", "ZSB", 
            "ESPO", "KSET", "QPX", "ROBT", "NSPL", "DBA", "SPYG", "GXTG", "RDOG", "TECB", "FCPI", "NBDS", "FOVL", "BIBL", 
            "SPUS", "QQJG", "IYC", "QQQN", "VPN", "AFTY", "BFTR"
        ]
    }   
    leveraged:dict[str, list] = {
            ETF_TYPES.OPTIONS: [
                "SOXL", "GDXD", "USD", "GLL", "UYM", "SMN", "AGQ", "DZZ", "SCO",
                "DGP", "UGL", "UVIX", "SOXS"
            ],            
            ETF_TYPES.NON_OPTIONS: [
                "FNGU", "BULZ", "WEBL", "ERX", "TQQQ", "CWEB", "KORU", "FNGO", "TECL", "HIBL", "FNGG", "JDST",  "UCYB",
                "ROM", "SKYU", "TARK", "QLD", "YINN", "DRN", "UPRO", "SPXL", "EDC", "UGE", "SWAR", "DUST", "XPP", "RETL",
                "CLDL", "MIDU", "DFEN", "CHAU", "AWYX", "EVAV", "URE", "SSO", "OOTO", "DPST", "EET", "IWFL", "PFES", "SPUU", "TNA",
                "UDOW", "URTY", "DUSL", "SAA", "EVEN", "TMV", "BNKU", "MVV", "MNM", "TTT", "FAS", "UYG", "UTSL", "UWM", "NKEL", "DDM",
                "YCS", "TYO", "LABD", "TBT", "XDJA", "XTJL", "XBJA", "UXI", "NAIL", "PST", "XBOC", "INDL", "QTJL", "QTOC", "SMHB",
                "XBJL", "XDOC", "PILL", "EWV", "BRZU", "EUO", "KLNE", "XJUN", "CURE", "XDJL", "UJB", "XDQQ", "QTJA", "PFFL", "XTJA", "XDSQ",
                "FLYU", "EFO", "XDEC", "UBOT", "BIB", "MEXX", "BZQ", "BIS", "EPV", "ZSL", "TIPL", "RXL", "RXD", "ULE", "TPOR", "EURL",
                "UBT", "UPW", "DOZR", "UST", "UMDD", "LABU", "SDP", "YCL", "SIJ", "TWM", "DXD", "UPV", "TYD", "EZJ", "SKF", "UBR",
                "SDD", "SCC", "BNKD", "TMF", "EEV", "FAZ", "SRTY", "MZZ", "TZA", "SDOW", "NKEQ", "PFEL", "FXP", "SZK", "SRS", "SDS",
                "SILX", "EDZ", "FLYD", "SMDD", "NUGT", "SPXU", "SPXS", "DRV", "YANG", "REW", "SSG", "JNUG", "QID", "HIBS", "DRIP", "TECS",
                "GDXU", "SQQQ", "ERY", "WEBS", "BERZ", "FNGD", "OILD", "MSOX", "LTL", "FBGX", "FIEE", "UCC", "FIHD", "HDLB", "SCDL", "IWML", 
                "IWDL", "USML", "MTUL", "QULL", "ESUS", "FEDL", "TIPD", "XDAP", "XBAP", "QTAP", "XTAP", "WANT"
            ]
        }
    
    def __init__(self, tradingClient: AlpacaTradingClient, dataClient: AlpacaDataClient):
        self.tradingClient: AlpacaTradingClient = tradingClient
        self.dataClient: AlpacaDataClient = dataClient
        
    @classmethod
    def create(cls, tradingClient: AlpacaTradingClient, dataClient: AlpacaDataClient):
        return cls(
            tradingClient=tradingClient,
            dataClient=dataClient
        )
        
    def getAllCandidates(self) -> list[str]:
        tradableStocksSymbols = [asset.symbol for asset in self.tradingClient.allTradableStocks]
        res = []
        for symbol in tqdm(tradableStocksSymbols, desc="filter for viable stocks"):
            try:
                if self.dataClient.getMarketCap(symbol) > 10_000_000:
                    res.append(symbol)
            except:
                continue
                
        return res 
        
    
    def getETFCandidates(self) -> dict:
        res = {"leveraged": {},
               "unleveraged": {}}
        
        tradableStocksSymbols = [asset.symbol for asset in self.tradingClient.allTradableStocks]
        
        res["leveraged"][ETF_TYPES.OPTIONS] = [asset for asset in set(ETFs.leveraged[ETF_TYPES.OPTIONS]) if \
                                                asset in tradableStocksSymbols and \
                                                self.dataClient.getMarketCap(asset) > 10_000_000
                                            ]
        res["leveraged"][ETF_TYPES.NON_OPTIONS] = [asset for asset in set(ETFs.leveraged[ETF_TYPES.NON_OPTIONS]) if \
                                                asset in tradableStocksSymbols and \
                                                self.dataClient.getMarketCap(asset) > 10_000_000
                                            ]
        res["unleveraged"][ETF_TYPES.OPTIONS] = [asset for asset in set(ETFs.unleveraged[ETF_TYPES.OPTIONS]) if \
                                                asset in tradableStocksSymbols and \
                                                self.dataClient.getMarketCap(asset) > 10_000_000
                                            ]
        res["unleveraged"][ETF_TYPES.NON_OPTIONS] = [asset for asset in set(ETFs.unleveraged[ETF_TYPES.NON_OPTIONS]) if \
                                                asset in tradableStocksSymbols and \
                                                self.dataClient.getMarketCap(asset) > 10_000_000
                                            ]
        
        return res 
