from typing import Any, Callable, Tuple, Dict
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import clusterdb as cdb
from abc import ABC, abstractmethod
import scipy
import json

from utils import remove_outliers, nadaraya_watson_estimator, plot_zipf_regression


class Model(ABC):
    def __init__(self, name: str, pop: np.ndarray, plot_fit: bool = False):
        self.name = name
        self.pop = pop
        self.traj = None
        self.fitter = CurveFitter(plot_fit=plot_fit)

    @abstractmethod
    def step(self) -> None:
        pass

    def init(self) -> None:
        pass

    def run(self, n_steps: int) -> Dict[int, np.ndarray]:
        print(f'Running {self.name} model')
        traj = {0: self.pop.copy()}
        for i in range(n_steps):
            self.step()
            traj.update({i + 1: self.pop.copy()})

        self.traj = traj
        return traj

    @abstractmethod
    def fit(self) -> Any:
        pass

    def plot(self, frequency: int, fig: go.Figure = None, color: str = 'black') -> go.Figure:
        fig = go.Figure() if fig is None else fig
        for key, val in self.traj.items():
            if key % frequency == 0:
                pop = 1 + pd.Series(val).to_frame(name='population')
                color_ = pd.DataFrame(np.array([color] * len(pop)).reshape(-1, 1), columns=['color'], index=pop.index)
                fig = plot_zipf_regression(population=pop, fig=fig,color=color_, name=f'{self.name}--{key}', plot_theory=False, plot_annotation=False, plot_regression=False, threshold_regression=5 * 10 ** 3)

        return fig

    def compare(self, frequency: int, fig: go.Figure = None) -> go.Figure:
        fig = self.plot(frequency=frequency, fig=fig)
        data = cdb.get_cluster_population()
        years = data['year'].unique()
        for year in years:
            if (year - 1850) % frequency == 0:
                pop = 1 + data[data['year'] == year]['population'].values
                pop = pd.Series(pop).to_frame(name='population')
                color = pd.DataFrame(np.array(['red'] * len(pop)).reshape(-1, 1), columns=['color'], index=pop.index)
                fig = plot_zipf_regression(population=pop, fig=fig, color=color, name=f'real--{year}', plot_theory=False, plot_annotation=False, plot_regression=False, threshold_regression=5 * 10 ** 3)

        return fig

    def save_traj(self, file_path: str) -> None:
        with open(file_path, "w") as outfile:
            json.dump({k: v.tolist() for k, v in self.traj.items()}, outfile)


class CurveFitter:
    def __init__(self, plot_fit: bool = False):
        self.plot_fit = plot_fit

    def mean_growth_rate_curve(self, years: Tuple[int, int] = None):
        growth_measure = 'annualized_growth_rate'
        q = 0.00001
        growth = cdb.get_cluster_growth_rate(years=years)
        remove_outliers(data=growth, col_name=growth_measure, q=q)
        nd_estimator_growth = nadaraya_watson_estimator(data=growth, x_name='population', y_name=growth_measure, nbins=100)
        x, y = nd_estimator_growth.index.values, nd_estimator_growth[f'mean_{growth_measure}'].values

        def mean_growth_rate(size: float):
            return np.interp(size, x, y)

        if self.plot_fit:
            fig = go.Figure()
            x_ = np.logspace(start=2, stop=7, num=100, base=10)
            y_ = np.array([mean_growth_rate(size) for size in x_])
            fig.add_trace(go.Scatter(x=np.log10(x_), y=y_, mode='lines', name='mean growth rate'))
            fig.update_layout(title='Mean growth rate', xaxis_title='Population', yaxis_title='Growth rate', template='plotly_white')
            fig.show()

        return mean_growth_rate

    def std_growth_rate_curve(self):
        growth_rate = cdb.get_cluster_growth_rate()
        growth_measure = 'annualized_growth_rate'
        growth_rate = growth_rate.sort_values(by=['cluster_uid', 'year'])
        growth_rate_grouped = growth_rate.groupby('cluster_uid').agg({growth_measure: 'std', 'population': 'first'}).reset_index().dropna()
        nd = nadaraya_watson_estimator(data=growth_rate_grouped, x_name='population', y_name=growth_measure, nbins=100, h=0.5)
        x, y = nd.index.values, nd[f'mean_{growth_measure}'].values

        def std_growth_rate(size: float):
            return np.interp(size, x, y)

        if self.plot_fit:
            fig = go.Figure()
            x_ = np.logspace(start=2, stop=7, num=100, base=10)
            y_ = np.array([std_growth_rate(size) for size in x_])
            fig.add_trace(go.Scatter(x=np.log10(x_), y=y_, mode='lines', name='mean growth rate'))
            fig.update_layout(title='Std growth rate', xaxis_title='Population', yaxis_title='Std growth rate', template='plotly_white')
            fig.show()

        return std_growth_rate

    def num_cluster_curve(self):
        pop = cdb.get_cluster_population()
        number_of_clusters = pop.groupby('year').count()['cluster_uid']

        def num_clusters(year: int):
            return np.interp(year, number_of_clusters.index, number_of_clusters.values)

        return num_clusters

    def population_curve(self):
        pop = cdb.get_cluster_population()
        total_population = pop.groupby('year')['population'].sum()

        def population(year: int):
            return np.interp(year, total_population.index, total_population.values)

        return population


class RandomWalkModel(Model):
    def __init__(self, name: str, pop: np.ndarray, lower_bound: int, plot_fit: bool = False):
        super().__init__(name=name, pop=pop, plot_fit=plot_fit)
        self.get_num_clusters, self.get_mean_growth_rate, self.get_std_growth_rate = self.fit()
        self.lower_bound = lower_bound
        self._current_year = 1850

    @abstractmethod
    def _get_growth_rate(self):
        pass

    def step(self):
        growth_rate = self._get_growth_rate()
        self.pop *= growth_rate
        new_clusters = np.random.lognormal(mean=8, sigma=1, size=int(self.get_num_clusters(year=self._current_year+1) - self.get_num_clusters(year=self._current_year)))
        self.pop = np.append(self.pop, new_clusters)
        self.pop = np.clip(self.pop, self.lower_bound, np.inf)
        self._current_year += 1

    def fit(self):
        return self.fitter.num_cluster_curve(), self.fitter.mean_growth_rate_curve(), self.fitter.std_growth_rate_curve()


class GabaixModel(RandomWalkModel):
    def __init__(self, pop: np.ndarray, lower_bound: int = 10 ** 2, plot_fit: bool = False):
        super().__init__(name='gabaix', pop=pop, lower_bound=lower_bound, plot_fit=plot_fit)

    def _get_growth_rate(self):
        growth_rate = 1 + np.array([np.random.normal(loc=self.get_mean_growth_rate(p), scale=self.get_std_growth_rate(p)) for p in self.pop])
        growth_rate = np.clip(growth_rate, 0, np.inf)
        return growth_rate


class BarthelemyModel(RandomWalkModel):
    def __init__(self, pop: np.ndarray, lower_bound: int = 10**2, plot_fit: bool = False):
        super().__init__(name='barthelemy', pop=pop, plot_fit=plot_fit, lower_bound=lower_bound)
        self.get_shock_exponent = self._fit_shock_exponent()

    def _get_growth_rate(self):
        growth_rates = 1 + np.array([scipy.stats.levy_stable.rvs(alpha=self.get_shock_exponent(p), beta=0, loc=self.get_mean_growth_rate(p), scale=self.get_std_growth_rate(p)) for p in self.pop])
        growth_rates = np.clip(growth_rates, 0, np.inf)
        return growth_rates

    def _fit_shock_exponent(self):
        def shock_exponent(size: float):
            if size < 5 * 10 ** 3:
                return 1.25
            else:
                return 1.5

        return shock_exponent


class PreferentialAttachmentModel(Model):
    def __init__(self, name: str, pop: np.ndarray, lump_sampler: Callable[[], int], plot_fit: bool = False):
        super().__init__(name=name, pop=pop, plot_fit=plot_fit)
        self.lump_sampler = lump_sampler
        self.get_population, self.get_num_clusters, self.get_mean_growth_rate, self.get_std_growth_rate = self.fit()
        self._current_year = 1850

    def _sample_lumps(self, total_pop: float) -> np.ndarray:
        lumps = []
        sum_pop_lumps = 0
        while sum_pop_lumps < total_pop:
            lump_pop = self.lump_sampler()
            lumps.append(lump_pop)
            sum_pop_lumps += lump_pop

        return np.array(lumps)

    def _assign_lump_to_cluster(self, lumps: np.ndarray) -> pd.DataFrame:
        if len(lumps) == 0:
            return pd.DataFrame(columns=['lump', 'cluster'])

        growth_rate = np.array([self.get_mean_growth_rate(p) for p in self.pop])
        lump_assignment_proportional = self.pop * growth_rate
        lump_assignment_proportional = np.clip(lump_assignment_proportional, 0, np.inf)
        lump_assignment_probability = lump_assignment_proportional / lump_assignment_proportional.sum()

        probability_new_cluster = min(1, (self.get_num_clusters(year=self._current_year+1) - self.get_num_clusters(year=self._current_year)) / len(lumps))
        lump_assignment_probability = (1 - probability_new_cluster) * lump_assignment_probability
        lump_assignment_probability = np.append(lump_assignment_probability, probability_new_cluster)

        lump_assignment = np.random.choice(np.arange(len(self.pop) + 1), size=len(lumps), p=lump_assignment_probability)
        lump_assignment = pd.DataFrame({'lump': lumps, 'cluster': lump_assignment})
        return lump_assignment

    @abstractmethod
    def step(self) -> None:
        pass

    def fit(self):
        return self.fitter.population_curve(), self.fitter.num_cluster_curve(), self.fitter.mean_growth_rate_curve(), self.fitter.std_growth_rate_curve()


class SimonModel(PreferentialAttachmentModel):
    def __init__(self, pop: np.ndarray, lump_sampler: Callable[[], int], name: str = 'simon', plot_fit: bool = False):
        super().__init__(name=name, pop=pop, lump_sampler=lump_sampler, plot_fit=plot_fit)

    def step(self):
        pop_change = self.get_population(year=self._current_year + 1) - self.get_population(year=self._current_year)
        lumps = self._sample_lumps(pop_change)

        lump_assignment = self._assign_lump_to_cluster(lumps)

        lump_assignment_existing_clusters = lump_assignment[lump_assignment['cluster'] < len(self.pop)]
        additional_pop_existing_clusters = lump_assignment_existing_clusters.groupby('cluster')['lump'].sum().sort_index()
        new_pop_existing_clusters = self.pop.copy()
        new_pop_existing_clusters[additional_pop_existing_clusters.index] += additional_pop_existing_clusters.values
        pop_new_clusters = lump_assignment[lump_assignment['cluster'] == len(self.pop)]['lump'].values

        self.pop = np.append(new_pop_existing_clusters, pop_new_clusters)
        self._current_year += 1


class DurantonModel(PreferentialAttachmentModel):
    def __init__(self, pop: np.ndarray, lump_sampler: Callable[[], int], plot_fit: bool = False, name: str = 'duranton', relocation_p: float = 0.001):
        super().__init__(pop=pop, lump_sampler=lump_sampler, name=name, plot_fit=plot_fit)

        self._lump_register = []
        for i, p in enumerate(self.pop):
            lumps = self._sample_lumps(p)
            for l in lumps:
                self._lump_register.append((l, i))

        self._lump_register = pd.DataFrame(self._lump_register, columns=['lump', 'cluster'])
        self.relocation_prob = relocation_p

    def _get_growth_rate(self):
        growth_rate = 1 + np.array([np.random.normal(loc=self.get_mean_growth_rate(p), scale=self.get_std_growth_rate(p)) for p in self.pop])
        growth_rate = np.clip(growth_rate, 0, np.inf)
        return growth_rate

    def step(self):
        pop_change = self.get_population(year=self._current_year + 1) - self.get_population(year=self._current_year)
        new_lumps = self._sample_lumps(pop_change)

        lump_count_cluster = self._lump_register.groupby('cluster')['lump'].count()
        cluster_with_more_than_one_lump = lump_count_cluster[lump_count_cluster > 1].index
        relocation_candidates = self._lump_register[self._lump_register['cluster'].isin(cluster_with_more_than_one_lump)]

        n_relocating_lumps = np.random.binomial(n=len(relocation_candidates), p=self.relocation_prob)
        relocation_proportional = (1 / relocation_candidates['lump'].values)
        relocation_probs = relocation_proportional / relocation_proportional.sum()
        relocating_lumps_index = np.random.choice(relocation_candidates.index, size=n_relocating_lumps, replace=False, p=relocation_probs)
        relocating_lumps = relocation_candidates.loc[relocating_lumps_index, 'lump'].values
        self._lump_register = self._lump_register.drop(index=relocating_lumps_index)

        lumps = np.append(new_lumps, relocating_lumps)
        lump_assignment = self._assign_lump_to_cluster(lumps)

        lump_assignment_existing_clusters = lump_assignment[lump_assignment['cluster'] < len(self.pop)]
        self._lump_register = pd.concat([self._lump_register, lump_assignment_existing_clusters], ignore_index=True)

        lump_assignment_new_clusters = lump_assignment[lump_assignment['cluster'] == len(self.pop)].copy()
        new_cluster_ids = np.arange(len(self.pop), len(self.pop) + len(lump_assignment_new_clusters))
        lump_assignment_new_clusters['cluster'] = new_cluster_ids
        self._lump_register = pd.concat([self._lump_register, lump_assignment_new_clusters], ignore_index=True)

        self.pop = self._lump_register.groupby('cluster')['lump'].sum().sort_index().values
        self.pop = np.clip(self.pop, 10**2, np.inf)
        self._current_year += 1
