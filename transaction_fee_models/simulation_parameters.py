from dataclasses import dataclass
from typing import List, Dict, Any, Tuple


@dataclass
class SimulationParameters:
    def __init__(
        self,
        num_blocks: int = 100,
        demand_sizes: List[int] = [50, 1000, 2000],  # very low, normal, very high demand
        demand_probabilities: List[float] = [0.05, 0.9, 0.05],
        fee_cap_range:Tuple[float]=(-0.1, 0.1), 
        max_tip_pct:float=0.1,
        scale_block_size: float = 0.2,
        probability_stop_below_gas_limit: float = 0.5,
        purge_after: int = 4,
        variable_gas_limits: bool = True
    ):
        assert len(demand_sizes) == len(demand_probabilities), "demand_sizes and demand_probabilities must have the same length"
        assert abs(sum(demand_probabilities) - 1.0) < 1.e-12, "demand_probabilities must sum to 1.0"
        assert all([0.0 <= p <= 1.0 for p in demand_probabilities]), "demand_probabilities must be between 0 and 1"
        assert 0.0 <= probability_stop_below_gas_limit <= 1.0, "probability_stop_below_gas_limit must be between 0 and 1"
        assert 0 < scale_block_size, "scale_block_size must be strictly positive"
        assert scale_block_size <= 1.0, "scale_block_size must be less than or equal to 1.0"
        assert len(fee_cap_range) == 2, "fee_cap_range must be a tuple of length 2"
        assert fee_cap_range[0] <= fee_cap_range[1], "fee_cap_range must be in increasing order"
        assert max_tip_pct >= 0.0, "max_tip_pct must be positive"
        assert purge_after >= 1, "purge_after must be at least 1"

        self.num_blocks = num_blocks
        self.demand_sizes = demand_sizes
        self.demand_probabilities = demand_probabilities
        self.fee_cap_range = fee_cap_range
        self.max_tip_pct = max_tip_pct
        self.scale_block_size = scale_block_size
        self.probability_stop_below_gas_limit = probability_stop_below_gas_limit
        self.purge_after = purge_after
        self.variable_gas_limits = variable_gas_limits