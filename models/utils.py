import numpy as np
import pandas as pd
import statsmodels.api as sm
from typing import Tuple
import plotly.graph_objects as go


def nadaraya_watson_estimator(data: pd.DataFrame, x_name: str, y_name: str, nbins: int, h: float = 1) -> pd.DataFrame:
    # Use the same notation as in the PNAS paper "Laws of population growth" by Rozenfeld et al., but lowercase
    lower_bound, upper_bound = 0.9 * data[x_name].min(), 1.1 * data[x_name].max()

    s0 = np.logspace(start=np.log10(lower_bound), stop=np.log10(upper_bound), num=nbins, base=10)
    sit0 = data[x_name].values
    ris0 = data[y_name].values

    s0_m = np.outer(s0, np.ones_like(sit0))
    sit0_m = np.outer(np.ones_like(s0), sit0)
    kh = np.exp(-1 * (np.log(s0_m) - np.log(sit0_m)) ** 2 / (2 * h ** 2))

    sum_kn_across_clusters = kh.sum(axis=1)

    estimate_mean = (kh @ ris0) / sum_kn_across_clusters

    ris0_m = np.outer(ris0, np.ones_like(s0))
    estimate_mean_m = np.outer(np.ones_like(ris0), estimate_mean)
    estimate_variance = np.diag((kh @ np.power(ris0_m - estimate_mean_m, 2)) / sum_kn_across_clusters)

    estimate = pd.DataFrame({'bin': s0, f'mean_{y_name}': estimate_mean, f'std_{y_name}': np.sqrt(estimate_variance)})
    estimate.set_index('bin', inplace=True)
    return estimate


def remove_outliers(data: pd.DataFrame, col_name: str, q: float = 0.001) -> pd.DataFrame:
    qh = data[col_name].quantile(1 - q)
    ql = data[col_name].quantile(q)
    data = data[(data[col_name] < qh) & (data[col_name] > ql)]
    return data


def plot_zipf_regression(population: pd.DataFrame, color: pd.DataFrame = None, text: pd.DataFrame = None, fig: go.Figure = None, row: int = None, col: int = None, name='Population', title: str = 'Zipf regression', plot_annotation: bool = True, show_scatter_label: bool = True,
                         plot_theory: bool = True, plot_regression: bool = True, threshold_regression: int = 5*10**3) -> go.Figure:
    assert 'population' in population.columns, 'population must be a column of population'
    assert len(population) > 0, 'population must be a non-empty dataframe'

    x = population[population['population'] > threshold_regression]['population'].values
    reg, start_point = run_zipf_regression(x=x)
    intercept, slope, r2, adj_r2 = reg.params[0], reg.params[1], reg.rsquared, reg.rsquared_adj

    color_ = color if color is not None else pd.DataFrame(np.array(['black'] * len(population)).reshape(-1, 1), columns=['color'], index=population.index)
    text_ = text if text is not None else pd.DataFrame(np.array([''] * len(population)).reshape(-1, 1), columns=['text'], index=population.index)
    data = pd.concat([population, color_, text_], axis=1)
    data.sort_values(by='population', ascending=False, inplace=True)
    data['rank'] = np.arange(len(data)) + 1

    data['log10_pop'] = np.log10(data['population'])
    data['log10_rank'] = np.log10(data['rank'])
    log10 = np.log(10)
    trace_zipf_scatter = go.Scatter(x=data['log10_pop'], y=data['log10_rank'], mode='markers', name=name, text=data['text'], marker=dict(color=data['color']), showlegend=show_scatter_label)
    trace_zipf_line = go.Scatter(x=data['log10_pop'], y=slope * (data['log10_pop'] - start_point / log10) + intercept / log10, mode='lines', name=f'{name}--S: {np.round(slope, decimals=3)}', line=dict(color=data['color'].iloc[0]), showlegend=True)

    fig.add_trace(trace_zipf_scatter, row=row, col=col)

    if plot_regression:
        fig.add_trace(trace_zipf_line, row=row, col=col)

    if plot_theory:
        trace_theory = go.Scatter(x=data['log10_pop'], y=-1 * (data['log10_pop'] - start_point / log10) + np.log10(len(data)), mode='lines', name=f'{name}--Theory 1', showlegend=True, line=dict(color='black', dash='dash'))
        fig.add_trace(trace_theory, row=row, col=col)

    fig.update_layout(template='plotly_white', title_text=title, font=dict(size=25, color='black'))

    if plot_annotation:
        fig.add_annotation(text=f"Zipf exponent: {np.round(slope, decimals=3)}, R2: {np.round(r2, decimals=3)}, AdjR2: {np.round(adj_r2, decimals=3)}, Nobs: {len(population)}", xref="paper",
                           yref="paper", x=0.9, y=0.9, showarrow=False)

    fig.update_xaxes(title_text='Population')
    fig.update_yaxes(title_text='Rank', rangemode="nonnegative")
    return fig


def run_zipf_regression(x: np.ndarray) -> Tuple[sm.regression.linear_model.RegressionResults, int]:
    x_ = sorted(x, reverse=True)
    log_sorted_x = np.log(x_) - np.log(x_[-1])
    log_rank_x = np.log(np.arange(len(log_sorted_x)) + 1)
    reg = _fit_regression(x=log_sorted_x, y=log_rank_x)
    return reg, np.log(x_[-1])


def _fit_regression(x, y):
    reg = sm.OLS(y, sm.add_constant(x)).fit()
    return reg