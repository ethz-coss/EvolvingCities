import clusterdb as cdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import numpy as np
import statsmodels.api as sm

from utils import nadaraya_watson_estimator, plot_zipf_regression


def plot_mean_growth_rate_by_size_nd(threshold):
    gr = cdb.get_cluster_growth_rate()
    gr = gr[gr['population'] > threshold]
    nd = nadaraya_watson_estimator(data=gr, x_name='population', y_name='annualized_growth_rate', nbins=100)
    x, y = nd.index.values, nd['mean_annualized_growth_rate'].values

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y, mode='lines', line=dict(width=4)))
    fig.update_layout(template='plotly_white', title='Growth rate by size', xaxis_title='Population', yaxis_title='Annualized growth rate', font=dict(size=20, color='black'))
    fig.update_xaxes(type='log')
    fig.show()


def plot_growth_rate_by_size_box_plot(threshold, q):
    gr = cdb.get_cluster_growth_rate()
    gr = gr[gr['population'] > threshold]
    gr['log_pop'] = np.log10(gr['population'])
    gr['log_bins'] = pd.qcut(gr['log_pop'], q=q)
    gr['log_bins'] = gr['log_bins'].apply(lambda x: np.round(x.mid, decimals=2))

    log_bins = sorted(gr['log_bins'].unique())
    fig = go.Figure()
    for b in log_bins:
        data = gr[gr['log_bins'] == b]
        qh = data['annualized_growth_rate'].quantile(0.9)
        ql = data['annualized_growth_rate'].quantile(0.1)
        data = data[(data['annualized_growth_rate'] < qh) & (data['annualized_growth_rate'] > ql)]
        fig.add_trace(go.Box(y=data['annualized_growth_rate'], name=b, showlegend=False))

    fig.update_layout(template='plotly_white', title='Growth rate by size', xaxis_title='Log10(Population)', yaxis_title='Annualized growth rate', font=dict(size=20, color='black'))
    fig.show()


def plot_std_growth_rate_by_size():
    gr = cdb.get_cluster_growth_rate()
    gr = gr[gr['population'] > 10**3]
    std = gr.groupby('cluster_uid').agg({'annualized_growth_rate': 'std', 'population': 'first'}).rename(columns={'annualized_growth_rate': 'std_annualized_growth_rate'}).dropna()
    nd = nadaraya_watson_estimator(data=std, x_name='population', y_name='std_annualized_growth_rate', nbins=100)
    x, y = nd.index.values, nd['mean_std_annualized_growth_rate'].values

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y, mode='lines', line=dict(width=4)))
    fig.update_layout(template='plotly_white', title='Std growth rate by size', xaxis_title='Population', yaxis_title='Std annualized growth rate', font=dict(size=20, color='black'))
    fig.update_xaxes(type='log')
    fig.show()

    X = sm.add_constant(np.log(x))
    model = sm.OLS(np.log(y), X)
    results = model.fit()
    print(results.summary())

def plot_zipf():
    cids = cdb.get_cluster_population(year=1850)['cluster_uid'].values
    pop = cdb.get_cluster_population(cluster_ids=cids)

    pop_1850 = pop[pop['year'] == 1850].copy()
    c_1850 = pd.DataFrame(np.array([px.colors.qualitative.Plotly[0]] * len(pop_1850)).reshape(-1, 1), columns=['color'], index=pop_1850.index)
    pop_1940_1 = pop[pop['year'] == 1940].copy()
    c_1940_1 = pd.DataFrame(np.array([px.colors.qualitative.Plotly[1]] * len(pop_1940_1)).reshape(-1, 1), columns=['color'], index=pop_1940_1.index)
    pop_1940_2 = cdb.get_cluster_population(year=1940)
    c_1940_2 = pd.DataFrame(np.array([px.colors.qualitative.Plotly[2]] * len(pop_1940_2)).reshape(-1, 1), columns=['color'], index=pop_1940_2.index)

    fig = go.Figure()
    fig = plot_zipf_regression(population=pop_1850[['population']], name='1850', fig=fig, color=c_1850,  plot_theory=False, plot_annotation=False, threshold_regression=10**4, plot_regression=False)
    fig = plot_zipf_regression(population=pop_1940_1[['population']], name='1940 Fixed', color=c_1940_1, fig=fig, plot_theory=False, plot_annotation=False, threshold_regression=10**4, plot_regression=False)
    fig = plot_zipf_regression(population=pop_1940_2[['population']], name='1940 Variable', color=c_1940_2, fig=fig, plot_theory=False, plot_annotation=False, threshold_regression=10 ** 4, plot_regression=False)
    fig.update_layout(title='Zipf plots USA cities', font=dict(size=20, color='black'), xaxis_title='Log10(Population)', yaxis_title='Log10(Rank)')
    fig.show()

def plot_heatmap_growth_rate(gr: pd.DataFrame, fig: go.Figure = None, name: str = '', nbins: int = 20, row: int = None, col: int = None, showscale: bool = False):
    if fig is None:
        fig = go.Figure()
        fig.update_layout(template='plotly_white', title_text='Annualized growth rate', xaxis_title='Population', yaxis_title='Growth rate')
    fig = go.Figure() if fig is None else fig
    n_runs = gr['run'].nunique()
    xbins = np.linspace(2, 7, nbins)
    ybins = np.linspace(-0.5, 0.5, nbins)
    h, xedges, yedges = np.histogram2d(np.log10(gr['population']), gr['annualized_growth_rate'], bins=(xbins, ybins))
    fig.add_trace(go.Heatmap(z=np.log(1 + h.T/n_runs), x=xedges, y=yedges, colorscale='Viridis', name=name, showscale=showscale), row=row, col=col)
    return fig


def plot_heatmap_std_growth_rate(gr: pd.DataFrame, fig: go.Figure = None, name: str = '', nbins: int = 20, row: int = None, col: int = None, showscale: bool = False):
    if fig is None:
        fig = go.Figure()
        fig.update_layout(template='plotly_white', title_text='Standard deviation of growth rate', xaxis_title='Population', yaxis_title='Standard deviation')
    std_growth_rates = gr.groupby('cluster_uid').agg({'annualized_growth_rate': 'std', 'population': 'first'}).rename(columns={'annualized_growth_rate': 'std_annualized_growth_rate'})
    std_growth_rates = std_growth_rates.dropna()
    xbins = np.linspace(2, 7, nbins)
    ybins = np.linspace(0, 0.2, nbins)
    h, xedges, yedges = np.histogram2d(np.log10(std_growth_rates['population']), std_growth_rates['std_annualized_growth_rate'], bins=(xbins, ybins))
    fig.add_trace(go.Heatmap(z=np.log(1 + h.T), x=xedges, y=yedges, colorscale='Viridis', name=name, showscale=showscale), row=row, col=col)
    return fig


def plot_heatmap_zipf(traj: pd.DataFrame, fig: go.Figure = None, name: str = '', nbins: int = 20, row: int = None, col: int = None, showscale: bool = False):
    if fig is None:
        fig = go.Figure()
        fig.update_layout(template='plotly_white', title_text='Zipf plot', xaxis_title='Population', yaxis_title='Rank')

    def _get_rank(x):
        rank = x.rank(ascending=False).astype(np.int64)
        return rank

    max_year = traj['year'].max()
    traj = traj[traj['year'] == max_year][['run', 'cluster_uid', 'population']].copy()
    traj['rank'] = traj.groupby(['run'])['population'].transform(_get_rank)
    xbins = np.linspace(2, 7, nbins)
    ybins = np.linspace(0, 3, nbins)
    h, xedges, yedges = np.histogram2d(np.log10(traj['population']), np.log10(traj['rank']), bins=(xbins, ybins))
    fig.add_trace(go.Heatmap(z=np.log(1 + h.T), x=xedges, y=yedges, colorscale='Viridis', name=name, showscale=showscale), row=row, col=col)
    return fig


def plot_comparison(traj: pd.DataFrame, name: str):
    real = cdb.get_cluster_population()
    real['run'] = 1

    gr_traj = get_annualized_growth_rate(traj).dropna()
    gr_real = get_annualized_growth_rate(real).dropna()

    fig = make_subplots(rows=2, cols=2, subplot_titles=('Real Mean', f'{name} Mean', 'Real Std', f'{name} Std'), vertical_spacing=0.05, horizontal_spacing=0.05, shared_yaxes=True, shared_xaxes=True)
    fig = plot_heatmap_growth_rate(gr_real, fig=fig, row=1, col=1)
    fig = plot_heatmap_growth_rate(gr_traj, fig=fig, row=1, col=2)
    fig = plot_heatmap_std_growth_rate(gr_traj, fig=fig, row=2, col=1)
    fig = plot_heatmap_std_growth_rate(gr_real, fig=fig, row=2, col=2)
    fig.update_layout(template='plotly_white', title_text='Annualized growth rate')

    fig.show()

def get_annualized_growth_rate(traj):
    def _growth_rate(x):
        if len(x) < 2:
            return np.nan
        g = -1 * x.diff(-1) / x
        return g

    traj = traj.sort_values(by=['run', 'cluster_uid', 'year'])
    traj['growth_rate'] = traj.groupby(['run', 'cluster_uid'])['population'].transform(_growth_rate)
    ydiff = -1 * traj['year'].diff(-1)
    traj['annualized_growth_rate'] = np.power(1 + traj['growth_rate'], 1/ydiff) - 1
    return traj