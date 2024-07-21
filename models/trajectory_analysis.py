from typing import List, Dict, Tuple, Callable

import numpy as np
import pandas as pd
import json
from statsmodels.tsa import ar_model
import clusterdb as cdb
from utils import remove_outliers, nadaraya_watson_estimator, plot_zipf_regression


def fit_mean_growth_rate(years: Tuple[int, int] = None):
    growth_measure = 'annualized_growth_rate'
    q = 0.00001
    growth = cdb.get_cluster_growth_rate(years=years)
    remove_outliers(data=growth, col_name=growth_measure, q=q)
    nd_estimator_growth = nadaraya_watson_estimator(data=growth, x_name='population', y_name=growth_measure, nbins=100)
    x, y = nd_estimator_growth.index.values, nd_estimator_growth[f'mean_{growth_measure}'].values

    def mean_growth_rate(size: float):
        return np.interp(size, x, y)

    return mean_growth_rate


def load_trajectory(file_path: str) -> pd.DataFrame:
    with open(file_path, 'r') as f:
        traj = json.load(f)

    traj_data = []
    for y in traj.keys():
        if int(y) % 10 == 0:
            pop = traj[y]
            ids = np.arange(len(pop))
            traj_data.append(pd.DataFrame({'year': 1850 + int(y) * np.ones(len(pop), dtype=np.int64), 'cluster_uid': ids, 'population': pop}))

    traj_data = pd.concat(traj_data, ignore_index=True)
    return traj_data


def get_annualized_growth_rate(traj):
    def _growth_rate(x):
        if len(x) < 2:
            return np.nan
        g = -1 * x.diff(-1) / x
        return g

    traj = traj.sort_values(by=['cluster_uid', 'year'])
    traj['growth_rate'] = traj.groupby('cluster_uid')['population'].transform(_growth_rate)
    ydiff = -1 * traj['year'].diff(-1)
    traj['annualized_growth_rate'] = np.power(1 + traj['growth_rate'], 1/ydiff) - 1
    return traj


def compute_autocorrelation(traj: pd.DataFrame):
    traj = get_annualized_growth_rate(traj)
    def _autocorrelation(x):
        x_ = x.dropna().values
        if len(x_) < 4:
            return np.nan
        corr = x_[:-1].dot(x_[1:]) / (np.linalg.norm(x_[:-1]) * np.linalg.norm(x_[1:]))
        return corr

    autocorr = traj.groupby('cluster_uid').agg({'annualized_growth_rate': _autocorrelation, 'population': 'first'}).reset_index()
    autocorr = autocorr.rename(columns={'annualized_growth_rate': 'autocorrelation'})
    return autocorr


def compute_autocorrelation_mean_growth(traj: pd.DataFrame):
    traj = get_annualized_growth_rate(traj)
    mean_growth = fit_mean_growth_rate()

    def _autocorrelation_with_mean_growth(x):
        if len(x) < 3:
            return np.nan
        g = np.array([mean_growth(size) for size in x['population'].values[:-1]])
        x_ = x['annualized_growth_rate'].dropna().values - g
        corr = x_[:-1].dot(x_[1:]) # / (np.linalg.norm(x_[:-1]) * np.linalg.norm(x_[1:]))
        return corr


    autocorr_mean_growth = traj.groupby('cluster_uid').apply(_autocorrelation_with_mean_growth).to_frame(name='autocorrelation')
    pop = traj.groupby('cluster_uid')['population'].first()
    autocorr_mean_growth = pd.concat([autocorr_mean_growth, pop], axis=1)
    return autocorr_mean_growth