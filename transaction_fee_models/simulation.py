from typing import List, Dict, Any, Tuple
import numpy as np
np.random.seed(42)
import pandas as pd
import tqdm

from simulation_parameters import SimulationParameters
from tx_fees_models import Block, Blockchain, MemPool, TransactionFeeMechanism, \
    EIP1559, StableFee, Transaction, create_demand


def _generate_random_bool(p_true=0.5):
    return np.random.choice([True, False], p=(p_true, 1.-p_true), size=1)[0]


def run_simulation(params: SimulationParameters) -> Tuple[pd.DataFrame, pd.DataFrame]:

    blockchain:Dict[str,Blockchain] = {
        "EIP": Blockchain(),
        "StableFee": Blockchain()
    }

    mempool:Dict[str, MemPool] = {
        "EIP": MemPool(),
        "StableFee": MemPool()
    }

    tf_models:dict[str, TransactionFeeMechanism] = {
        "EIP": EIP1559(),
        "StableFee": StableFee()
    }

    df_stats:Dict[str, pd.DataFrame] = {
        "EIP": pd.DataFrame(
            0, 
            columns=["demand_size", "num_tx", "price", "tot_paid"], 
            index=range(params.num_blocks)
        ),
        "StableFee": pd.DataFrame(
            0, 
            columns=["demand_size", "num_tx", "price", "tot_paid"], 
            index=range(params.num_blocks)
        )  
    } 

    pbar = tqdm.tqdm(total=params.num_blocks)

    for b in range(params.num_blocks):
        
        # transactions are created with random values
        demand_size:int = np.random.choice(params.demand_sizes, p=params.demand_probabilities, size=1)[0]

        demand:List[Transaction] = create_demand(
            demand_size, 
            fee_cap_range=params.fee_cap_range, 
            max_tip_pct=params.max_tip_pct,
            variable_gas=params.variable_gas_limits
        )
        
        stop_below_gas_limit:bool = _generate_random_bool(params.probability_stop_below_gas_limit)

        scale_block_size = np.random.uniform(
            0., params.scale_block_size
        )

        for chain, pool, tfm, stats in zip(
            blockchain.values(), mempool.values(), tf_models.values(), df_stats.values()
        ):
            scaled_demand:List[Transaction] = tfm.scale_demand(demand)
            
            # transactions are added to the mempool
            pool.add_txs(scaled_demand)
            
            # transactions are selected from the mempool based on the gas premium
            selected_transactions, to_be_purged_transactions = tfm.select_transactions(
                pool, 
                stop_below_gas_limit=stop_below_gas_limit, 
                scale_block_size=scale_block_size,
                purge_after=params.purge_after
            )

            # selected transactions are added to the blockchain
            chain.add_block(Block(selected_transactions))

            # the price is updated based on the selected transactions
            tfm.update_price(chain)

            # base fee is updated for the next round
            stats.loc[b, "demand_size"] = demand_size
            stats.loc[b, "num_tx"] = len(selected_transactions)
            stats.loc[b, "price"] = tfm.get_current_price()
            stats.loc[b, "tot_paid"] = tfm.total_paid_fees(selected_transactions)

            # clear the mempool
            pool.remove_txs(selected_transactions)
            pool.remove_txs(to_be_purged_transactions)

        pbar.update(1)

    pbar.close()

    df_stats_merged = pd.concat(
        [df_stats[mechanism].add_suffix("_" + mechanism) for mechanism in df_stats.keys()],
        axis=1
    )

    df_chain_stats:Dict[str,pd.DataFrame] = {
        "EIP": blockchain["EIP"].compute_stats(),
        "StableFee": blockchain["StableFee"].compute_stats()
    }

    df_chain_stats_merged = pd.concat(
        [df_chain_stats[mechanism].add_suffix("_" + mechanism) for mechanism in df_stats.keys()],
        axis=1
    )

    return df_stats_merged, df_chain_stats_merged
