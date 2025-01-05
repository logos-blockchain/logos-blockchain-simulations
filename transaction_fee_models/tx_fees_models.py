import numpy as np
import pandas as pd
from typing import List, Tuple, Dict
from dataclasses import dataclass, field
import uuid
import copy


PROTOCOL_CONSTANTS = {
    "TARGET_GAS_USED": 12500000.0,  # 12.5 million gas
    "MAX_GAS_ALLOWED": 25000000.0,  # 25 million gas
    "INITIAL_BASEFEE": 1.0,  # 10^9 wei = 1 Gwei
    "MIN_PRICE": 0.5,  # 10^8 wei = 0.1 Gwei
    "MAX_PRICE": 10.0  # 10^11 wei = 100 Gwei
}


class Transaction:

    def __init__(self, gas_used:float, fee_cap:float, tip:float):
        self.gas_used = gas_used
        self.fee_cap = fee_cap
        self.tip = tip  # this is ignored in the case of Stable Fee
        self.tx_hash = uuid.uuid4().int


@dataclass
class MemPool:
    pool: Dict[int, Transaction] = field(default_factory=dict)
    
    def add_tx(self, tx: Transaction):
        self.pool[tx.tx_hash] = tx
        
    def add_txs(self, txs: List[Transaction]):
        for tx in txs:
            self.add_tx(tx)

    def remove_tx(self, tx: Transaction):
        self.pool.pop(tx.tx_hash)
        
    def remove_txs(self, txs: List[Transaction]):
        for tx in txs:
            self.remove_tx(tx)
    
    def __len__(self):
        return len(self.pool)


@dataclass
class Block():
    txs:Dict[int, Transaction]
    
    def add_tx(self, tx: Transaction):
        self.txs[tx.tx_hash] = tx
    
    def add_txs(self, txs: List[Transaction]):
        for tx in txs:
            self.add_tx(tx)
            
    def print_dataframe(self) -> pd.DataFrame:
        _df = pd.DataFrame(
            [
                [tx.gas_used, tx.fee_cap, tx.tip]
                for tx in self.txs
            ], 
            columns=['gas_used', 'fee_cap', 'tip'],
            index=range(len(self.txs))
        )
        _df.index.name = "tx"
        return _df


class Blockchain:

    def __init__(self, blocks: List[Block]=None):
        self.blocks:List[Block] = []
        if blocks:
            self.add_blocks(blocks)

    def add_block(self, block: Block):
        self.blocks.append(block)

    def add_blocks(self, blocks: List[Block]):
        for block in blocks:
            self.add_block(block)

    def get_last_block(self):
        return self.blocks[-1]

    def compute_stats(self) -> pd.DataFrame:
        # compute total gas used, total gas premium, total fee cap, average gas premium, average fee cap
        _df = pd.DataFrame(
            0.,
            columns=['tot_gas_used', 'tot_fee_cap', 'tot_tips', 'avg_fee_cap', 'avg_tips', 'gas_target'],
            index=range(len(self.blocks))
        )

        for b, block in enumerate(self.blocks):
            num_tx = float(len(block.txs))
            tot_gas_used, tot_fee_cap, tot_tips = np.sum(
                [
                    [tx.gas_used, tx.fee_cap, tx.tip]
                    for tx in block.txs
                ], 
                axis=0
            )
            _df.iloc[b,:] = np.array(
                [
                    tot_gas_used, tot_fee_cap, tot_tips, 
                    tot_fee_cap/num_tx, tot_tips/num_tx, PROTOCOL_CONSTANTS["TARGET_GAS_USED"]
                ]
            )

        return _df


def create_demand(
        num_txs: int, 
        fee_cap_range:Tuple[float]=(-0.1, 0.1), 
        max_tip_pct:float=0.1,
        variable_gas:bool=True
    ) -> List[Transaction]:
    
    # these are levels that need to be scaled by the price/base fee of the specific transaction fee model
    fee_caps = 1. + np.random.normal(fee_cap_range[0], fee_cap_range[1], num_txs)
    #fee_caps = (1. + np.random.uniform(fee_cap_range[0], fee_cap_range[1], num_txs))
    tip = np.random.uniform(0., max_tip_pct, num_txs)  # 0.1 is the max gas premium factor

    if variable_gas:
        return _create_demand_variable_gas(fee_caps, tip)
    else:
        return _create_demand_const_gas(fee_caps, tip)


def _create_demand_const_gas(fee_caps:np.ndarray, tip:np.ndarray) -> List[Transaction]:
    
    demand: List[Transaction] = []
    gas_used = np.mean([  # 73_850 gas limit
        21_000 * 0.3,   # eth transfer
        45_000 * 0.3,   # erc20 transfer
        50_000 * 0.1,   # token approval
        200_000 * 0.2,  # token swap 
        150_000 * 0.03,  # NFT (ERC721) minting
        75_000* 0.03,   # NFT transfer
        120_000 * 0.03,  # NFT (ERC1155) minting
        500_000 * 0.01,  # smart contract deployment
    ])

    for fc, tp in zip(fee_caps, tip):
        tx = Transaction(
            gas_used = gas_used,
            fee_cap = fc,
            tip= tp
        )
        demand.append(tx)

    return demand


def _create_demand_variable_gas(fee_caps:np.ndarray, tip:np.ndarray) -> List[Transaction]:
    
    demand: List[Transaction] = []

    gas_used = np.random.choice(
        [
            21_000,   # eth transfer
            45_000,   # erc20 transfer
            50_000,   # token approval
            200_000,  # token swap 
            150_000,  # NFT (ERC721) minting
            75_000,   # NFT transfer
            120_000,  # NFT (ERC1155) minting
            500_000,  # smart contract deployment
        ], 
        p=(0.3, 0.3, 0.1, 0.2, 0.03, 0.03, 0.03, 0.01),
        size=len(fee_caps)
    )

    for gu, fc, tp in zip(gas_used, fee_caps, tip):
        tx = Transaction(
            gas_used = gu,
            fee_cap = fc,
            tip = tp
        )
        demand.append(tx)

    return demand


class TransactionFeeMechanism:

    def __init__(self):
        self.price:List[float] = []
        self.price.append(PROTOCOL_CONSTANTS["INITIAL_BASEFEE"])

    def update_price(self, blockchain:Blockchain):
        raise NotImplementedError

    def get_current_price(self) -> float:
        return self.price[-1]

    def scale_demand(self, demand:List[Transaction]) -> List[Transaction]:
        cur_price:float = self.get_current_price()
        scaled_demand = copy.deepcopy(demand)
        for tx in scaled_demand:
            tx.fee_cap *= cur_price
            tx.tip *= cur_price
        return scaled_demand

    def _select_from_sorted_txs(
        self, 
        sorted_txs:List[Transaction], 
        stop_below_gas_limit:bool=False, 
        scale_block_size:float=1.0, purge_after:int=4
    ) -> Tuple[List[Transaction], List[Transaction]]:

        # select transactions so that the sum of gas used is less than the block gas limit
        selected_txs_idx = 0
        to_be_purged_txs_idx = 0
        gas_used = 0
        
        # introduce some randomness in the selection in case there are too many transactions
        # this is to simulate the fact that miners may not always select the most profitable transactions
        # this increases or decreases the number of transactions selected based on the stop_below_gas_limit flag,
        # which is also randomly selected
        fac = 1.0 + (1. - 2.*stop_below_gas_limit) * scale_block_size
        
        for tx in sorted_txs:
            if gas_used + tx.gas_used < fac * PROTOCOL_CONSTANTS["TARGET_GAS_USED"]:
                selected_txs_idx += 1
            if gas_used + tx.gas_used < purge_after * PROTOCOL_CONSTANTS["MAX_GAS_ALLOWED"]:  # enough space for X full blocks
                to_be_purged_txs_idx += 1
            else:
                break
            gas_used += tx.gas_used

        return sorted_txs[:selected_txs_idx], sorted_txs[to_be_purged_txs_idx:]

    def select_transactions(
        self, mempool: MemPool, stop_below_gas_limit:bool=False, scale_block_size:float=1.0, purge_after:int=4
    ) -> Tuple[List[Transaction], List[Transaction]]:
        raise NotImplementedError

    def total_paid_fees(self, txs: List[Transaction]) -> float:
        raise NotImplementedError


class EIP1559(TransactionFeeMechanism):
    
    def __init__(self):
        self.base_factor:float = 1./8.
        self.base_fee:List[float] = []
        self.base_fee.append(PROTOCOL_CONSTANTS["INITIAL_BASEFEE"])
        super().__init__()

    def update_price(self, blockchain:Blockchain) -> float:
        base_fee:float = self.base_fee[-1]
        last_txs:List[Transaction] = blockchain.get_last_block().txs
        gas_used:float = sum([tx.gas_used for tx in last_txs])
        delta:float = (gas_used - PROTOCOL_CONSTANTS["TARGET_GAS_USED"])/PROTOCOL_CONSTANTS["TARGET_GAS_USED"]
        self.base_fee.append(
            base_fee * np.exp(delta * self.base_factor)  # (1. + delta * self.base_factor)
        )
        sum_price:float = sum([min(tx.fee_cap, base_fee+tx.tip) for tx in last_txs])
        self.price.append( 
            np.clip(
                sum_price/float(len(last_txs)), 
                a_min=PROTOCOL_CONSTANTS["MIN_PRICE"],
                a_max=PROTOCOL_CONSTANTS["MAX_PRICE"]
            )
        )

    def select_transactions(
        self, mempool: MemPool, stop_below_gas_limit:bool=False, scale_block_size:float=1.0, purge_after:int=4
    ) -> Tuple[List[Transaction], List[Transaction]]:

        base_fee:float = self.base_fee[-1]

        # Sort transactions by fee cap
        sorted_txs:List[Transaction] = sorted(
            mempool.pool.values(), 
            key=lambda tx: min(tx.fee_cap, base_fee+tx.tip) * tx.gas_used, 
            reverse=True
        )

        return self._select_from_sorted_txs(
            sorted_txs, 
            stop_below_gas_limit=stop_below_gas_limit, 
            scale_block_size=scale_block_size, 
            purge_after=purge_after
        )

    def total_paid_fees(self, txs: List[Transaction]) -> float:
        base_fee:float = self.base_fee[-1]
        return sum([min(tx.fee_cap, base_fee+tx.tip) * tx.gas_used for tx in txs])


class StableFee(TransactionFeeMechanism):

    def __init__(self):
        super().__init__()

    def update_price(self, blockchain:Blockchain):
        last_txs:List[Transaction] = blockchain.get_last_block().txs
        new_price:float = np.min([tx.fee_cap for tx in last_txs])
        self.price.append(
            np.clip(
                new_price, 
                a_min=PROTOCOL_CONSTANTS["MIN_PRICE"],
                a_max=PROTOCOL_CONSTANTS["MAX_PRICE"]
            )
        )

    def select_transactions(
        self, mempool: MemPool, stop_below_gas_limit:bool=False, scale_block_size:float=1.0, purge_after:int=4
    ) -> Tuple[List[Transaction], List[Transaction]]:

        # Sort transactions by fee cap
        sorted_txs = sorted(
            mempool.pool.values(), 
            key=lambda tx: tx.fee_cap * tx.gas_used, 
            reverse=True
        )

        return self._select_from_sorted_txs(
            sorted_txs, 
            stop_below_gas_limit=stop_below_gas_limit, 
            scale_block_size=scale_block_size,
            purge_after=purge_after
        )

    def total_paid_fees(self, txs: List[Transaction]) -> float:
        price:float = self.get_current_price()
        return sum([price * tx.gas_used for tx in txs])