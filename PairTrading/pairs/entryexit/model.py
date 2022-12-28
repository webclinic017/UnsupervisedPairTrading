import numpy as np
import pandas as pd
from PairTrading.pairs.entryexit import OptimalStopping
from PairTrading.pairs.entryexit import ou_mle as ou
from PairTrading.util.patterns.base import Base
from datetime import datetime


class OUModel(Base):
    '''
    How to use Model:
    1. Model.Update() in OnData (including during Warmup)
    2. if Model.Ready2Train() -> Model.Train()
        2.1. Retrain periodically
    3. Buy Portfolio if Model.IsEnter()
    4. If bought, sell if Model.IsExit()
    '''

    def __init__(self, close_A, close_B:np.array):
        self.optimal_stopping:OptimalStopping = None
        self.alloc_B:float = -1
        self.close_A:np.array = close_A
        self.close_B:np.array = close_B

        self.portfolio:Portfolio = None  # represents portfolio value of holding 
        # $1 of stock A and -$alloc_B of stock B
        self.Train()
        
    @classmethod
    def create(cls, stockAClose, stockBClose:np.array):
        return cls(
            close_A=stockAClose,
            close_B=stockBClose
        )


    def Train(self, r=.05, c=.05):
        '''
        Computes our OU and B-Allocation coefficients
        '''

        ts_A:np.array = np.array(self.close_A)
        ts_B:np.array = np.array(self.close_B)

        days:int = 252
        dt:float = 1.0 / days

        theta, mu, sigma, self.alloc_B = self.__argmax_B_alloc(ts_A, ts_B, dt)

        try:
            self.optimal_stopping = OptimalStopping(theta, mu, sigma, r, c)
        except:
            # sometimes we get weird OU Coefficients that lead to unsolveable Optimal Stopping
            self.optimal_stopping = None

        self.portfolio = Portfolio(ts_A[-1], ts_B[-1], ts_A[0], ts_B[0], self.alloc_B)


    def IsEnter(self) -> bool:
        '''
        Return True if it is optimal to enter the Pairs Trade, False otherwise
        '''
        return self.portfolio.Value <= self.optimal_stopping.OptimalEntry()

    def IsExit(self) -> bool:
        '''
        Return True if it is optimal to exit the Pairs Trade, False otherwise
        '''
        return self.portfolio.Value >= self.optimal_stopping.OptimalExit()

    def __compute_portfolio_values(self, ts_A, ts_B, alloc_B) -> float:
        '''
        Compute the portfolio values over time when holding $1 of stock A 
        and -$alloc_B of stock B

        input: ts_A - time-series of price data of stock A,
               ts_B - time-series of price data of stock B
        outputs: Portfolio values of holding $1 of stock A and -$alloc_B of stock B
        '''

        ts_A = ts_A.copy()  # defensive programming
        ts_B = ts_B.copy()

        ts_A = ts_A / ts_A[0]
        ts_B = ts_B / ts_B[0]
        return ts_A - alloc_B * ts_B

    def __argmax_B_alloc(self, ts_A, ts_B, dt) -> tuple[float]:
        '''
        Finds the $ allocation ratio to stock B to maximize the log likelihood
        from the fit of portfolio values to an OU process

        input: ts_A - time-series of price data of stock A,
               ts_B - time-series of price data of stock B
               dt - time increment (1 / days(start date - end date))
        returns: θ*, µ*, σ*, B*
        '''

        theta = mu = sigma = alloc_B = 0
        max_log_likelihood = 0

        def compute_coefficients(x):
            portfolio_values = self.__compute_portfolio_values(ts_A, ts_B, x)
            return ou.estimate_coefficients_MLE(portfolio_values, dt)

        vectorized = np.vectorize(compute_coefficients)
        linspace = np.linspace(.01, 1, 100)
        res = vectorized(linspace)
        index = res[3].argmax()

        return res[0][index], res[1][index], res[2][index], linspace[index]



class Portfolio:
    '''
    Represents a portfolio of holding $1 of stock A and -$alloc_B of stock B
    '''

    def __init__(
        self, 
        init_price_A, 
        init_price_B, 
        curr_price_A,
        curr_price_b,
        alloc_B:float
        ):
        self.init_price_A = init_price_A
        self.init_price_B = init_price_B
        self.curr_price_A = curr_price_A
        self.curr_price_B = curr_price_B
        self.alloc_B = alloc_B

    def Update(self, new_price_A, new_price_B):
        self.curr_price_A = new_price_A
        self.curr_price_B = new_price_B

    @property
    def Value(self):
        return self.curr_price_A / self.init_price_A - self.alloc_B * self.curr_price_B / self.init_price_B