use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use netrunner::{
    network::regions::RegionsData,
    node::{NodeId, NodeIdExt},
};
use rand::{seq::SliceRandom, RngCore};

#[derive(Clone)]
pub struct Topology(HashMap<NodeId, HashSet<NodeId>>);

impl Topology {
    /// Builds a topology with the given nodes and peering degree
    /// by ensuring that all nodes are connected (no partition)
    /// and all nodes have the same number of connections (only if possible).
    pub fn new<R: RngCore>(nodes: &[NodeId], peering_degree: usize, mut rng: R) -> Self {
        tracing::info!("Building topology: peering_degree:{}", peering_degree);
        loop {
            let mut topology = nodes
                .iter()
                .map(|&node| (node, HashSet::new()))
                .collect::<HashMap<_, _>>();

            for node in nodes.iter() {
                // Collect peer candidates
                let mut others = nodes
                    .iter()
                    .filter(|&other| {
                        // Check if the other node is not already connected to the current node
                        // and the other node has not reached the peering degree.
                        other != node
                            && !topology.get(node).unwrap().contains(other)
                            && topology.get(other).unwrap().len() < peering_degree
                    })
                    .copied()
                    .collect::<Vec<_>>();

                // How many more connections the current node needs
                let num_needs = peering_degree - topology.get(node).unwrap().len();
                // Sample peers as many as possible and connect them to the current node
                let k = std::cmp::min(num_needs, others.len());
                others.as_mut_slice().shuffle(&mut rng);
                others.into_iter().take(k).for_each(|peer| {
                    topology.get_mut(node).unwrap().insert(peer);
                    topology.get_mut(&peer).unwrap().insert(*node);
                });
            }

            // Check constraints:
            // - All nodes are connected (no partition)
            // - All nodes have the same number of connections (if possible)
            let topology = Self(topology);
            let can_have_equal_conns = (nodes.len() * peering_degree) % 2 == 0;
            if topology.check_all_connected()
                && (!can_have_equal_conns || topology.check_equal_conns(peering_degree))
            {
                return topology;
            }
            tracing::info!("Topology doesn't meet constraints. Retrying...");
        }
    }

    /// Checks if all nodes are connected (no partition) in the topology.
    fn check_all_connected(&self) -> bool {
        let visited = self.dfs(*self.0.keys().next().unwrap());
        visited.len() == self.0.len()
    }

    /// Depth-first search to visit nodes in the topology.
    fn dfs(&self, start_node: NodeId) -> HashSet<NodeId> {
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut stack: Vec<NodeId> = Vec::new();
        stack.push(start_node);
        while let Some(node) = stack.pop() {
            visited.insert(node);
            for peer in self.0.get(&node).unwrap().iter() {
                if !visited.contains(peer) {
                    stack.push(*peer);
                }
            }
        }
        visited
    }

    /// Checks if all nodes have the same number of connections.
    fn check_equal_conns(&self, peering_degree: usize) -> bool {
        self.0
            .iter()
            .all(|(_, peers)| peers.len() == peering_degree)
    }

    /// Calculate the diameter (longest path length) of the topology.
    pub fn diameter(&self) -> usize {
        // Calculate a diameter from each node and take the maximum
        self.0
            .keys()
            .map(|&node| self.diameter_from(node))
            .fold(0, usize::max)
    }

    /// Calculate a diameter (longest path length) of the topology from the start_node.
    fn diameter_from(&self, start_node: NodeId) -> usize {
        // start_node is visited at the beginning
        let mut visited: HashSet<NodeId> = HashSet::from([start_node]);

        // Count the number of hops to visit all nodes
        let mut hop_count = 0;
        let mut next_hop: HashSet<NodeId> = self.0.get(&start_node).unwrap().clone();
        while !next_hop.is_empty() {
            // First, visit all nodes in the next hop and increase the hop count
            next_hop.iter().for_each(|&node| {
                assert!(visited.insert(node));
            });
            hop_count += 1;
            // Then, build the new next hop by collecting all peers of the current next hop
            // except peers already visited
            next_hop = next_hop
                .iter()
                .flat_map(|node| self.0.get(node).unwrap())
                .filter(|&peer| !visited.contains(peer))
                .copied()
                .collect();
        }
        hop_count
    }

    pub fn conn_latency_distribution(
        &self,
        regions_data: &RegionsData,
    ) -> HashMap<Duration, usize> {
        // Initialize a distribution
        let distribution = regions_data
            .region_network_behaviour
            .values()
            .map(|behaviour| (behaviour.delay(), 0))
            .collect();
        // Populate the distribution
        self.0.iter().fold(distribution, |mut acc, (node, peers)| {
            let region_a = regions_data.node_region(*node);
            peers.iter().for_each(|peer| {
                let region_b = regions_data.node_region(*peer);
                let behaviour = regions_data.network_behaviour_between_regions(region_a, region_b);
                acc.entry(behaviour.delay()).and_modify(|count| *count += 1);
            });
            acc
        })
    }

    pub fn get(&self, node: &NodeId) -> Option<&HashSet<NodeId>> {
        self.0.get(node)
    }

    /// Converts all [`NodeId`]s in the topology to their indices.
    pub fn to_node_indices(&self) -> HashMap<usize, Vec<usize>> {
        self.0
            .iter()
            .map(|(node, peers)| {
                (
                    node.index(),
                    peers.iter().map(|peer| peer.index()).collect(),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use netrunner::node::NodeIdExt;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::*;

    #[test]
    fn test_build_topology_with_equal_conns() {
        // If num_nodes * peering_degree is even,
        // it is possible that all nodes can have the same number of connections
        let nodes = (0..7).map(NodeId::from_index).collect::<Vec<_>>();
        let peering_degree = 4;

        let mut rng = rand::rngs::OsRng;
        let topology = Topology::new(&nodes, peering_degree, &mut rng);
        assert_eq!(topology.0.len(), nodes.len());
        for (node, peers) in topology.0.iter() {
            assert!(peers.len() == peering_degree);
            for peer in peers.iter() {
                assert!(topology.get(peer).unwrap().contains(node));
            }
        }
    }

    #[test]
    fn test_build_topology_with_inequal_conns() {
        // If num_nodes * peering_degree is odd,
        // it is impossible that all nodes can have the same number of connections
        let nodes = (0..7).map(NodeId::from_index).collect::<Vec<_>>();
        let peering_degree = 3;

        let mut rng = rand::rngs::OsRng;
        let topology = Topology::new(&nodes, peering_degree, &mut rng);
        assert_eq!(topology.0.len(), nodes.len());
        for (node, peers) in topology.0.iter() {
            assert!(peers.len() <= peering_degree);
            for peer in peers.iter() {
                assert!(topology.get(peer).unwrap().contains(node));
            }
        }
    }

    #[test]
    fn test_diameter() {
        let nodes = (0..100).map(NodeId::from_index).collect::<Vec<_>>();
        let peering_degree = 4;
        let mut rng = ChaCha8Rng::seed_from_u64(0);
        let topology = Topology::new(&nodes, peering_degree, &mut rng);
        let diameter = topology.diameter();
        println!("diameter: {}", diameter);
        assert!(diameter > 0);
        assert!(diameter <= nodes.len());
    }
}
