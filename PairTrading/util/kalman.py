from PairTrading.util.patterns.base import Base 
from PairTrading.util.patterns.singleton import Singleton
from pykalman import KalmanFilter
from pandas import Series
import numpy as np
import statsmodels.api as sm

class KalmanEngine(Base, metaclass=Singleton):
    
    entryZscore:float = 1 
    exitZscore:float = 0
    
    def __init__(self):
        self.__zscore:float = None
    
    @classmethod
    def create(cls):
        return cls()
    
    @property
    def zscore(self) -> float:
        return self.zscore
    
    def __kalmanFilterAverage(self, x:Series) -> Series:
        # Construct a Kalman filter
        kf = KalmanFilter(transition_matrices = [1],
        observation_matrices = [1],
        initial_state_mean = 0,
        initial_state_covariance = 1,
        observation_covariance=1,
        transition_covariance=.01)
        # Use the observed values of the price to get a rolling mean
        state_means, _ = kf.filter(x.values)
        state_means = Series(state_means.flatten(), index=x.index)
        return state_means
    
    
    def __kalmanFilterRegression(self, x,y:Series) -> tuple:
        delta:float = 1e-3
        trans_cov = delta / (1 - delta) * np.eye(2) # How much random walk wiggles
        obs_mat = np.expand_dims(np.vstack([[x], [np.ones(len(x))]]).T, axis=1)
        kf = KalmanFilter(n_dim_obs=1, n_dim_state=2, # y is 1-dimensional, (alpha, beta) is 2-dimensional
        initial_state_mean=[0,0],
        initial_state_covariance=np.ones((2, 2)),
        transition_matrices=np.eye(2),
        observation_matrices=obs_mat,
        observation_covariance=2,
        transition_covariance=trans_cov)
        # Use the observations y to get running estimates and errors for the state parameters
        state_means, state_covs = kf.filter(y.values)
        return state_means
    
    def __halfLife(self, spread:Series) -> int:
        spread_lag = spread.shift(1)
        spread_lag.iloc[0] = spread_lag.iloc[1]
        spread_ret = spread - spread_lag
        spread_ret.iloc[0] = spread_ret.iloc[1]
        spread_lag2 = sm.add_constant(spread_lag)
        model = sm.OLS(spread_ret,spread_lag2)
        res = model.fit()
        halflife = int(round(-np.log(2) / res.params[1],0))
        if halflife <= 0:
            halflife = 1
        return halflife
    
    def fit(self, x, y:Series) -> None:
        
        stateMeans:Series = self.__kalmanFilterRegression(
            x=self.__kalmanFilterAverage(x), 
            y=self.__kalmanFilterAverage(y)
        )
        
        hr:Series = - stateMeans[:,0]
        spread:Series = y + (x * hr)
        
        halfLife:int = self.__halfLife(spread)
        
        meanSpread:float = spread.rolling(window=halfLife).mean()
        stdSpread:float = spread.rolling(window=halfLife).std()
        
        self.__zscore = ((spread - meanSpread) / stdSpread)[-1]
        
    def canEnter(self) -> bool:
        if not self.zscore:
            raise ValueError("current zscore hasn't been calculated")
        
        return self.zscore > KalmanEngine.entryZscore
    
    def canExit(self) -> bool:
        if not self.zscore:
            raise ValueError("current zscore hasn't been calculated")
        
        return self.zscore < KalmanEngine.exitZscore
        
    
        