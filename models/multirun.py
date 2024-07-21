import copy
from typing import Dict
import h5py
import numpy as np
import pandas as pd

from models import Model


def save_multirun(model: Model, n_runs: int, n_steps: int) -> None:
    with h5py.File("simulations.hdf5", "w") as f:
        group = f.create_group(model.name)
        for i in range(n_runs):
            model_ = copy.deepcopy(model)
            traj = model_.run(n_steps)
            traj_df = _trajectory_dict_to_df(traj)
            group.create_dataset(f'run_{i}', data=traj_df.values, dtype='int64')


def _trajectory_dict_to_df(traj: Dict[int, np.ndarray]) -> pd.DataFrame:
    traj_data = []
    for y, p in traj.items():
        pop = p
        ids = np.arange(len(pop))
        traj_data.append(pd.DataFrame({'year': 1850 + y * np.ones(len(pop), dtype=np.int64), 'cluster_uid': ids, 'population': pop}))

    traj_data = pd.concat(traj_data, ignore_index=True)
    return traj_data


def load_multirun(model_name: str, frequency) -> pd.DataFrame:
    with h5py.File("simulations.hdf5", "r") as f:
        group = f[model_name]
        traj = []
        for run in group:
            run_data = pd.DataFrame(group[run][:], columns=['year', 'cluster_uid', 'population'])
            run_data['run'] = int(run.split('_')[1])
            run_data = run_data[run_data['year'] % frequency == 0]
            traj.append(run_data)

        traj = pd.concat(traj, ignore_index=True)
        return traj
